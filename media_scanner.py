import os
import time
import hashlib
import asyncio
import logging
import datetime
import sqlite3
import queue
from typing import Dict
from concurrent.futures import ThreadPoolExecutor

from .media_database import MediaDatabase
from .constants import *

logger = logging.getLogger('MediaCompressor.Scanner')

class MediaScanner:
    """
    Media Scanner class that asynchronously scans directories and compares
    files to the database, tracking new and changed files.
    """
    def __init__(self, config: Dict, db: MediaDatabase):
        self.config = config
        self.db = db
        self.scan_queue = queue.Queue()
        self.files_scanned = 0
        self.new_files_found = 0
        self.changed_files_found = 0
        self.scan_start_time = None
        self.current_directory = None
        # Add scanning state
        self.is_scanning = False
        self.scan_progress = 0
        self.total_dirs = 0
        self.processed_dirs = 0
    
    def _get_file_checksum(self, file_path: str) -> str:
        """Calculate a fast file checksum by hashing parts of the file."""
        try:
            file_size = os.path.getsize(file_path)
            
            # For small files, hash the entire file
            if file_size < 8 * 1024 * 1024:  # Less than 8MB
                with open(file_path, 'rb') as f:
                    return hashlib.md5(f.read()).hexdigest()
            
            # For larger files, hash the first and last 4MB for speed
            md5 = hashlib.md5()
            with open(file_path, 'rb') as f:
                # Read first 4MB
                data = f.read(4 * 1024 * 1024)
                md5.update(data)
                
                # Seek to last 4MB
                f.seek(-4 * 1024 * 1024, os.SEEK_END)
                data = f.read(4 * 1024 * 1024)
                md5.update(data)
            
            return md5.hexdigest()
        except (IOError, OSError) as e:
            logger.error(f"Error calculating checksum for {file_path}: {str(e)}")
            return ""
    
    def should_process_file(self, file_path: str) -> bool:
        """Quick check if a file should be processed (valid extension and size)."""
        # Check extension
        if not any(file_path.lower().endswith(ext) for ext in self.config["extensions"]):
            return False
        
        # Check min size
        try:
            file_size = os.path.getsize(file_path)
            if file_size < self.config["min_size_mb"] * 1024 * 1024:
                return False
            
            return True
        except (FileNotFoundError, PermissionError) as e:
            logger.debug(f"Error checking file {file_path}: {str(e)}")
            return False
    
    async def scan_directory_async(self, directory: str):
        """
        Asynchronously scan a directory and its subdirectories for media files.
        """
        self.current_directory = directory
        start_time = time.time()
        files_to_update = []
        file_count = 0
        total_size = 0
        
        logger.info(f"Starting scan of directory: {directory}")
        
        try:
            # Get total file count first for better progress tracking
            total_files_estimate = 0
            for root, dirs, files in os.walk(directory, topdown=True):
                for file_name in files:
                    if any(file_name.lower().endswith(ext) for ext in self.config["extensions"]):
                        total_files_estimate += 1
                        
                        # For large directories, sample only a portion to estimate
                        if total_files_estimate > 1000:
                            break
                
                # Break after sampling enough directories
                if total_files_estimate > 1000:
                    # Extrapolate based on remaining directories
                    remaining_dirs_estimate = sum(1 for _ in os.walk(directory))
                    avg_files_per_dir = total_files_estimate / max(1, len(list(os.walk(directory, topdown=True))))
                    total_files_estimate = int(avg_files_per_dir * remaining_dirs_estimate)
                    break
            
            # Now do the actual scan with better progress tracking
            for root, dirs, files in os.walk(directory):
                for file_name in files:
                    file_path = os.path.join(root, file_name)
                    
                    # Quick check for valid files
                    if not self.should_process_file(file_path):
                        continue
                    
                    # Track stats
                    file_count += 1
                    try:
                        file_size = os.path.getsize(file_path)
                        total_size += file_size
                        self.files_scanned += 1
                        
                        # Update progress based on better total estimate
                        if total_files_estimate > 0:
                            self.scan_progress = min(99, (self.files_scanned / total_files_estimate) * 100)
                        
                    except OSError as e:
                        logger.debug(f"Error getting size for {file_path}: {str(e)}")
                        continue
                    
                    # Check if file is in the database
                    db_info = self.db.get_file_status(file_path)
                    
                    if not db_info["in_database"]:
                        # New file, calculate checksum and add to database
                        checksum = self._get_file_checksum(file_path)
                        
                        file_info = {
                            "file_path": file_path,
                            "size": file_size,
                            "checksum": checksum,
                            "status": STATUS_PENDING
                        }
                        
                        self.db.add_new_file(file_info)
                        self.new_files_found += 1
                        
                        if self.new_files_found % 100 == 0:
                            logger.info(f"Found {self.new_files_found} new files so far")
                    
                    else:
                        # Existing file, check if changed
                        stored_checksum = db_info["checksum"]
                        
                        # Only calculate new checksum if the file size changed
                        # This is a significant performance optimization
                        if file_size != db_info["original_size"]:
                            checksum = self._get_file_checksum(file_path)
                            
                            # File changed, update status for reprocessing
                            if checksum != stored_checksum:
                                update_info = {
                                    "file_path": file_path,
                                    "status": STATUS_NEEDS_REPROCESSING,
                                    "last_checked_date": datetime.datetime.now().isoformat(),
                                    "checksum": checksum,
                                    "original_size": file_size
                                }
                                
                                files_to_update.append(update_info)
                                self.changed_files_found += 1
                                
                                # Perform batch updates periodically
                                if len(files_to_update) >= self.config["scan_batch_size"]:
                                    self.db.bulk_update_statuses(files_to_update)
                                    files_to_update = []
                        
                        # File hasn't changed, just update last_checked_date
                        else:
                            update_info = {
                                "file_path": file_path,
                                "last_checked_date": datetime.datetime.now().isoformat()
                            }
                            
                            # Only update if status allows for recompression
                            if db_info["status"] in [STATUS_ERROR, STATUS_COMPLETED]:
                                files_to_update.append(update_info)
                    
                    # Yield control periodically to allow other tasks to run
                    if file_count % 100 == 0:
                        await asyncio.sleep(0)
                
                # Update progress
                self.processed_dirs += 1
                if self.total_dirs > 0:
                    self.scan_progress = (self.processed_dirs / self.total_dirs) * 100
            
            # Final batch update
            if files_to_update:
                self.db.bulk_update_statuses(files_to_update)
            
            # Record directory scan stats
            duration = time.time() - start_time
            self.db.record_directory_scan(directory, file_count, total_size, duration)
            
            logger.info(f"Completed scan of {directory}: found {file_count} files, {self.new_files_found} new, {self.changed_files_found} changed")
            
        except Exception as e:
            logger.error(f"Error scanning directory {directory}: {str(e)}")
            self.db.log_system_event("scan_error", f"Error scanning directory {directory}: {str(e)}", "error")
        
        finally:
            self.current_directory = None
    
    async def scan_all_directories_async(self):
        """
        Asynchronously scan all configured media paths with concurrency control.
        """
        self.scan_start_time = time.time()
        self.files_scanned = 0
        self.new_files_found = 0
        self.changed_files_found = 0
        self.is_scanning = True
        self.scan_progress = 0
        self.processed_dirs = 0
        
        # Queue all directories for scanning
        self.total_dirs = 0
        for path in self.config["media_paths"]:
            if os.path.exists(path) and os.path.isdir(path):
                self.scan_queue.put(path)
                self.total_dirs += 1
                
                # Also count subdirectories to estimate progress better
                try:
                    # Sample subdirectory count for large libraries
                    if os.path.getsize(path) > 1_000_000_000:  # >1GB
                        subdirs = sum(1 for _ in os.walk(path, topdown=True, onerror=lambda e: None))
                        self.total_dirs += subdirs
                except Exception:
                    pass
            else:
                logger.warning(f"Media path does not exist or is not a directory: {path}")
        
        if self.total_dirs == 0:
            logger.warning("No valid directories to scan")
            self.is_scanning = False
            return {
                "files_scanned": 0,
                "new_files": 0,
                "changed_files": 0,
                "duration": 0,
                "status": "completed",
                "message": "No valid directories to scan"
            }
        
        # Process the queue with limited concurrency
        tasks = []
        semaphore = asyncio.Semaphore(self.config["max_concurrent_scans"])
        
        async def worker():
            while not self.scan_queue.empty():
                try:
                    directory = self.scan_queue.get_nowait()
                    
                    async with semaphore:
                        await self.scan_directory_async(directory)
                    
                    self.scan_queue.task_done()
                except queue.Empty:
                    break
                except Exception as e:
                    logger.error(f"Error in scan worker: {str(e)}")
                    self.db.log_system_event("scan_worker_error", f"Error in scan worker: {str(e)}", "error")
        
        # Start worker tasks
        for _ in range(min(self.config["max_concurrent_scans"], self.scan_queue.qsize())):
            task = asyncio.create_task(worker())
            tasks.append(task)
        
        # Wait for all scanning tasks to complete
        await asyncio.gather(*tasks)
        
        # Log summary
        duration = time.time() - self.scan_start_time
        logger.info(f"Complete media scan finished in {duration:.2f} seconds")
        logger.info(f"Files scanned: {self.files_scanned}")
        logger.info(f"New files: {self.new_files_found}")
        logger.info(f"Changed files: {self.changed_files_found}")
        
        # Mark new and changed files as ready for compression
        try:
            conn = sqlite3.connect(self.config["database_path"])
            cursor = conn.cursor()
            
            cursor.execute('''
            UPDATE processed_files 
            SET status = ?, queued_date = ?
            WHERE status IN (?, ?)
            ''', (
                STATUS_PENDING, 
                datetime.datetime.now().isoformat(),
                STATUS_NEW, 
                STATUS_NEEDS_REPROCESSING
            ))
            
            affected_rows = cursor.rowcount
            conn.commit()
            conn.close()
            
            logger.info(f"Marked {affected_rows} files as pending for compression")
        except sqlite3.Error as e:
            logger.error(f"Database error marking files for compression: {str(e)}")
            self.db.log_system_event("db_update_error", f"Error marking files for compression: {str(e)}", "error")
        
        # Mark scanning as completed
        self.is_scanning = False
        self.scan_progress = 100
        
        # Log event
        self.db.log_system_event(
            "scan_completed", 
            f"Scan completed: {self.files_scanned} files processed, {self.new_files_found} new, {self.changed_files_found} changed",
            "info"
        )
        
        return {
            "files_scanned": self.files_scanned,
            "new_files": self.new_files_found,
            "changed_files": self.changed_files_found,
            "duration": duration,
            "status": "completed"
        }
    
    def run_scan(self):
        """Run the media scanner synchronously (wrapper for async function)."""
        return asyncio.run(self.scan_all_directories_async())
    
    def get_scan_status(self):
        """Get the current status of the scanning process."""
        if not self.is_scanning:
            return {"status": "idle"}
        
        if self.scan_start_time is None:
            return {"status": "starting"}
        
        duration = time.time() - self.scan_start_time
        
        # Calculate ETA
        eta = None
        if self.scan_progress > 0:
            total_estimated_time = duration / (self.scan_progress / 100)
            remaining_time = total_estimated_time - duration
            eta = remaining_time
        
        return {
            "status": "scanning",
            "current_directory": self.current_directory,
            "files_scanned": self.files_scanned,
            "new_files": self.new_files_found,
            "changed_files": self.changed_files_found,
            "duration": duration,
            "progress": self.scan_progress,
            "eta_seconds": eta
        }
