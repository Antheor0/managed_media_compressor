import os
import time
import threading
import logging
import datetime
import concurrent
import queue
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Tuple, Optional, Any, TypedDict
from contextlib import contextmanager

from content_analyzer import ContentAnalyzer
from compression_engine import CompressionEngine
from notification_service import NotificationService
from resource_monitor import ResourceMonitor
from file_processor import FileProcessor
from constants import *

logger = logging.getLogger('MediaCompressor.Compressor')

class JobInfo(TypedDict):
    file_path: str
    file_name: str
    start_time: float
    progress: float
    file_size: int
    status: str
    estimated_time: int
    eta: Optional[float]
    current_stage: str

class MediaCompressor:
    """
    Media Compressor class that takes files from the database and
    compresses them using HandBrakeCLI.
    """
    def __init__(self, config: Dict, db):
        """Initialize the media compressor with configuration."""
        self.config = config
        self.db = db
        
        # Ensure temporary directory exists
        os.makedirs(self.config["temp_dir"], exist_ok=True)
        
        # Initialize components
        self.content_analyzer = ContentAnalyzer(config, quality_validator=None)
        self.quality_validator = None  # Will be set after imports to avoid circular reference
        self.compression_engine = CompressionEngine(config, quality_validator=None)
        self.notification_service = NotificationService(config, db_logger=self.db.log_system_event)
        self.resource_monitor = ResourceMonitor(config, db_logger=self.db.log_system_event)
        self.file_processor = FileProcessor(config)
        
        # Track current state
        self.active_jobs: Dict[int, JobInfo] = {}  # Dictionary to track active jobs: {thread_id: job_info}
        self.jobs_lock = threading.RLock()  # Lock for thread-safe access to active_jobs
        self.compression_start_time = None
        self.stats = {
            "session_start": time.time(),
            "files_processed": 0,
            "total_original_size": 0,
            "total_compressed_size": 0,
            "errors": 0
        }
        
        # Dynamic Job Management
        self.paused = False
        self.running = True
        self.job_queue = queue.PriorityQueue()
        self.job_history = {}  # Track job history for priority
    
    def set_quality_validator(self, quality_validator):
        """Set the quality validator after initialization to avoid circular imports."""
        self.quality_validator = quality_validator
        self.content_analyzer.quality_validator = quality_validator
        self.compression_engine.quality_validator = quality_validator
    
    @contextmanager
    def _db_connection(self):
        """Context manager for database connections to ensure proper cleanup."""
        conn = None
        try:
            conn = sqlite3.connect(self.config["database_path"])
            yield conn
        finally:
            if conn:
                conn.close()
    
    def pause_compression(self):
        """Pause all active compression jobs."""
        with self.jobs_lock:
            self.paused = True
            # Update file statuses
            for job_info in self.active_jobs.values():
                file_path = job_info["file_path"]
                self.db.update_file_status(file_path, STATUS_PAUSED)
            
            logger.info("Compression paused")
            self.db.log_system_event("compression_paused", "Compression jobs paused by user", "info")
    
    def resume_compression(self):
        """Resume compression jobs."""
        with self.jobs_lock:
            self.paused = False
            # Reset paused file statuses
            with self._db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE processed_files SET status = ? WHERE status = ?",
                    (STATUS_PENDING, STATUS_PAUSED)
                )
                conn.commit()
            
            logger.info("Compression resumed")
            self.db.log_system_event("compression_resumed", "Compression jobs resumed", "info")
    
    def stop_compression(self):
        """Stop all compression jobs."""
        with self.jobs_lock:
            self.running = False
            # Give active jobs a chance to complete
            logger.info("Stopping compression jobs (may take a moment to complete active jobs)")
            self.db.log_system_event("compression_stopped", "Compression jobs stopped by user", "info")
    
    def prioritize_file(self, file_path: str, priority: int = 10):
        """Set a high priority for a specific file."""
        self.db.update_file_status(file_path, STATUS_PENDING, priority=priority)
        logger.info(f"Prioritized {file_path} with priority {priority}")
        self.db.log_system_event("file_prioritized", f"File {file_path} prioritized with level {priority}", "info")
    
    def _register_job(self, file_path: str):
        """Register a new compression job in the active jobs list."""
        with self.jobs_lock:
            thread_id = threading.get_ident()
            
            # Get file info for progress tracking
            file_size = 0
            try:
                file_size = os.path.getsize(file_path)
            except OSError as e:
                logger.warning(f"Could not get file size for {file_path}: {e}")
                
            # Get estimated time
            estimated_time = 0
            try:
                with self._db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT estimated_time FROM processed_files WHERE file_path = ?", (file_path,))
                    result = cursor.fetchone()
                    if result and result[0]:
                        estimated_time = result[0]
            except sqlite3.Error as e:
                logger.warning(f"Database error getting estimated time: {e}")
            
            self.active_jobs[thread_id] = {
                "file_path": file_path,
                "file_name": os.path.basename(file_path),
                "start_time": time.time(),
                "progress": 0,
                "file_size": file_size,
                "status": "starting",
                "estimated_time": estimated_time,
                "eta": None,
                "current_stage": "initializing"
            }
    
    def _update_job_status(self, status: str, progress: Optional[float] = None, stage: Optional[str] = None, eta: Optional[float] = None):
        """Update the status of the current compression job with ETA calculation."""
        with self.jobs_lock:
            thread_id = threading.get_ident()
            if thread_id in self.active_jobs:
                job = self.active_jobs[thread_id]
                
                job["status"] = status
                if progress is not None:
                    job["progress"] = progress
                
                if stage is not None:
                    job["current_stage"] = stage
                
                if eta is not None:
                    job["eta"] = eta
                # Calculate ETA if we have progress and no direct ETA is provided
                elif progress is not None and progress > 0 and eta is None:
                    elapsed_time = time.time() - job["start_time"]
                    if elapsed_time > 0:
                        total_estimated_time = elapsed_time / (progress / 100)
                        remaining_time = total_estimated_time - elapsed_time
                        job["eta"] = remaining_time
    
    def _unregister_job(self):
        """Remove a compression job from the active jobs list."""
        with self.jobs_lock:
            thread_id = threading.get_ident()
            if thread_id in self.active_jobs:
                # Store job history for future prediction
                job = self.active_jobs[thread_id]
                file_path = job["file_path"]
                completion_time = time.time() - job["start_time"]
                
                # Update time prediction in database
                self.db.update_compression_time(file_path, int(completion_time))
                
                # Remove from active jobs
                del self.active_jobs[thread_id]
    
    def compress_file(self, file_path: str) -> Dict[str, Any]:
        """Compress a video file using HandBrakeCLI with NVENC."""
        start_time = time.time()
        
        try:
            original_size = os.path.getsize(file_path)
        except Exception as e:
            logger.error(f"Cannot access file {file_path}: {str(e)}")
            return {"status": "error", "error": f"Cannot access file: {str(e)}"}
        
        # Register this job in the active jobs list
        self._register_job(file_path)
        
        # Mark file as in progress in database
        self.db.update_file_status(
            file_path, 
            STATUS_IN_PROGRESS,
            processing_started=datetime.datetime.now().isoformat()
        )
        
        try:
            # Verify file integrity
            if self.config["recovery"]["verify_files"] and not self.file_processor.verify_file_integrity(
                file_path, update_status_callback=self._update_job_status
            ):
                error_msg = f"Original file integrity check failed for {file_path}"
                logger.error(error_msg)
                
                self.db.update_file_status(
                    file_path,
                    STATUS_ERROR,
                    error_message=error_msg
                )
                
                self._unregister_job()
                return {"status": "error", "error": error_msg, "original_size": original_size}
            
            # Get content-specific compression settings
            compression_settings = self.content_analyzer.get_compression_settings(file_path)
            
            # Prepare for compression
            temp_output, settings = self.compression_engine.prepare_compression(file_path, compression_settings)
            
            # Define status callback, is_paused and is_running functions for the compression engine
            status_callback = lambda status, progress=None, stage=None, eta=None: self._update_job_status(status, progress, stage, eta)
            is_paused = lambda: self.paused
            is_running = lambda: self.running
            
            # Run HandBrake
            if not self.compression_engine.run_handbrake(
                file_path, temp_output, settings, 
                status_callback=status_callback,
                paused_check=is_paused,
                running_check=is_running
            ):
                # If compression was stopped due to pause/stop
                if self.paused:
                    logger.info(f"Compression of {file_path} paused")
                    self.db.update_file_status(file_path, STATUS_PAUSED)
                    self._unregister_job()
                    return {"status": "paused", "original_size": original_size}
                elif not self.running:
                    logger.info(f"Compression of {file_path} stopped")
                    self.db.update_file_status(file_path, STATUS_PENDING)
                    self._unregister_job()
                    return {"status": "stopped", "original_size": original_size}
                
                error_msg = "HandBrake compression failed"
                logger.error(f"Error compressing {file_path}: {error_msg}")
                
                self.db.update_file_status(
                    file_path,
                    STATUS_ERROR,
                    error_message=error_msg
                )
                
                self._unregister_job()
                return {"status": "error", "error": error_msg, "original_size": original_size}
            
            # Define verify integrity function for finalization
            verify_integrity = lambda file_path: self.file_processor.verify_file_integrity(
                file_path, update_status_callback=self._update_job_status
            )
            
            # Finalize compression
            result = self.compression_engine.finalize_compression(
                file_path, temp_output, original_size,
                verify_integrity=verify_integrity,
                status_callback=status_callback
            )
            
            # Calculate time taken
            duration = time.time() - start_time
            
            if result["status"] == "success":
                # Get checksum for successful compression
                checksum = self.file_processor.get_file_checksum(file_path)
                result["checksum"] = checksum
                
                # Record in database
                self.db.update_file_status(
                    file_path,
                    STATUS_COMPLETED,
                    original_size=original_size,
                    compressed_size=result["compressed_size"],
                    compression_date=datetime.datetime.now().isoformat(),
                    checksum=checksum,
                    content_type=settings["content_type"],
                    quality_score=result["quality_score"],
                    compression_count=1,  # Increment compression count
                    actual_time=int(duration)
                )
                
                # Update stats
                self.stats["files_processed"] += 1
                self.stats["total_original_size"] += original_size
                self.stats["total_compressed_size"] += result["compressed_size"]
                
                # Log success
                logger.info(f"Successfully compressed {file_path}. "
                            f"Original: {original_size/1024/1024:.2f}MB, "
                            f"Compressed: {result['compressed_size']/1024/1024:.2f}MB, "
                            f"Reduction: {result['reduction']:.2%}, "
                            f"Quality: {result['quality_score']:.2f}, "
                            f"Time: {duration:.2f}s")
            
            elif result["status"] == "skipped":
                self.db.update_file_status(
                    file_path,
                    STATUS_SKIPPED,
                    skip_reason=result.get("reason", "Unknown reason"),
                    content_type=settings["content_type"],
                    quality_score=result.get("quality_score", 0)
                )
            
            # Unregister this job from active jobs
            self._unregister_job()
            
            # Add duration to the result
            result["duration"] = duration
            return result
        
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Unexpected error compressing {file_path}: {error_msg}")
            self.stats["errors"] += 1
            
            # Send error notification
            self.notification_service.send_notification(
                f"Unexpected error compressing {file_path}: {error_msg}",
                level="error"
            )
            
            # Clean up temp file if it exists
            if 'temp_output' in locals() and os.path.exists(temp_output):
                try:
                    os.remove(temp_output)
                except OSError:
                    pass
            
            # Update database with error status
            self.db.update_file_status(
                file_path,
                STATUS_ERROR,
                error_message=error_msg[:1000]  # Limit error message length
            )
            
            # Unregister this job from active jobs
            self._unregister_job()
            
            return {
                "status": "error",
                "error": error_msg,
                "original_size": original_size
            }
    
    def get_estimated_completion_time(self) -> Dict[str, Any]:
        """Get estimated completion time for all pending files."""
        stats = self.db.get_statistics()
        
        if not stats["processing_times"]["average_seconds"]:
            return {
                "eta_seconds": 0,
                "eta_formatted": "Unknown",
                "total_files": 0,
                "average_time_per_file": 0
            }
        
        total_eta = stats["estimated_remaining_time"]
        avg_time = stats["processing_times"]["average_seconds"]
        
        # Calculate how many concurrent jobs we can run
        concurrent_jobs = self.config["max_concurrent_jobs"]
        
        # Adjust total ETA based on concurrency
        if concurrent_jobs > 1:
            total_eta = total_eta / concurrent_jobs
        
        # Format the ETA
        eta_formatted = self._format_time_remaining(total_eta)
        
        pending_files = stats["status_counts"].get(STATUS_PENDING, 0)
        
        return {
            "eta_seconds": total_eta,
            "eta_formatted": eta_formatted,
            "total_files": pending_files,
            "average_time_per_file": avg_time
        }
    
    def _format_time_remaining(self, seconds: float) -> str:
        """Format ETA into human-readable string."""
        if seconds <= 0:
            return "< 1 minute"
        elif seconds < 60:
            return f"{int(seconds)} seconds"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            return f"{minutes} minute{'s' if minutes > 1 else ''}"
        elif seconds < 86400:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours} hour{'s' if hours > 1 else ''}, {minutes} minute{'s' if minutes > 1 else ''}"
        else:
            days = int(seconds // 86400)
            hours = int((seconds % 86400) // 3600)
            return f"{days} day{'s' if days > 1 else ''}, {hours} hour{'s' if hours > 1 else ''}"
    
    def process_compression_queue(self, limit: Optional[int] = None, force_now: bool = False) -> Dict[str, Any]:
        """Process files in the compression queue."""
        if not self.compression_engine.check_dependencies():
            logger.error("Dependency check failed, cannot start compression")
            return {"status": "error", "message": "Dependency check failed"}
        
        if not force_now and not self.resource_monitor.is_within_schedule():
            logger.info("Current time is outside the scheduled window or system load is too high. Exiting.")
            return {"status": "skipped", "reason": "Outside schedule window"}
        
        start_time = time.time()
        self.compression_start_time = start_time
        self.stats["session_start"] = start_time
        self.paused = False
        self.running = True
        
        # Check system resources
        if not self.resource_monitor.check_system_resources():
            logger.warning("Insufficient system resources, postponing compression")
            return {"status": "skipped", "reason": "Insufficient system resources"}
        
        # Get files for compression
        max_files = limit or self.config["compression_queue_size"]
        files_to_compress = self.db.get_files_for_compression(max_files)
        
        if not files_to_compress:
            logger.info("No files found for compression. Exiting.")
            return {"status": "completed", "files_processed": 0, "message": "No files to process"}
        
        logger.info(f"Starting compression of {len(files_to_compress)} files")
        
        # Process files with ThreadPoolExecutor
        total_original_size = 0
        total_compressed_size = 0
        files_processed = 0
        errors = 0
        
        with ThreadPoolExecutor(max_workers=self.config["max_concurrent_jobs"]) as executor:
            # Create a map of future to file data
            future_to_file = {}
            
            # Submit compression jobs 
            for file_data in files_to_compress:
                file_path = file_data["file_path"]
                future = executor.submit(self.compress_file, file_path)
                future_to_file[future] = file_data
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_file):
                file_data = future_to_file[future]
                file_path = file_data["file_path"]
                
                try:
                    result = future.result()
                    
                    if result["status"] == "success":
                        total_original_size += result["original_size"]
                        total_compressed_size += result["compressed_size"]
                        files_processed += 1
                    elif result["status"] == "error":
                        errors += 1
                    
                    # Check if we should stop processing
                    if not self.running:
                        logger.info("Compression stopped")
                        break
                        
                    # Check if we should pause
                    while self.paused and self.running:
                        logger.debug("Compression paused, waiting...")
                        time.sleep(5)
                        
                except Exception as e:
                    logger.error(f"Exception processing {file_path}: {str(e)}")
                    errors += 1
                    
                    # Update database with error status
                    self.db.update_file_status(
                        file_path,
                        STATUS_ERROR,
                        error_message=str(e)[:1000]
                    )
        
        # Record session statistics if any files were processed
        if files_processed > 0 or errors > 0:
            return self._record_compression_statistics(
                start_time, files_processed, errors, total_original_size, total_compressed_size)
        else:
            logger.info("No files were successfully compressed.")
            return {"status": "completed", "files_processed": 0, "errors": errors}
    
    def _record_compression_statistics(self, start_time: float, files_processed: int, 
                                     errors: int, total_original_size: int, 
                                     total_compressed_size: int) -> Dict[str, Any]:
        """Record compression statistics to database and generate summary."""
        # Calculate savings
        if total_original_size > 0:
            savings_percentage = (1 - (total_compressed_size / total_original_size)) * 100
        else:
            savings_percentage = 0
        
        # Update overall stats
        self.stats["files_processed"] = files_processed
        self.stats["total_original_size"] = total_original_size
        self.stats["total_compressed_size"] = total_compressed_size
        self.stats["errors"] = errors
        
        # Record in database
        try:
            with self._db_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                INSERT INTO compression_stats 
                (start_time, end_time, files_processed, total_original_size, total_compressed_size, savings_percentage, errors)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    datetime.datetime.fromtimestamp(start_time).isoformat(),
                    datetime.datetime.now().isoformat(),
                    files_processed,
                    total_original_size,
                    total_compressed_size,
                    savings_percentage,
                    errors
                ))
                
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Database error recording stats: {str(e)}")
        
        # Send completion notification
        self.notification_service.send_completion_notification({
            "files_processed": files_processed,
            "errors": errors,
            "total_original_size": total_original_size,
            "total_compressed_size": total_compressed_size,
            "savings_percentage": savings_percentage
        })
        
        # Log completion details
        logger.info(f"Compression session completed.")
        logger.info(f"Files processed: {files_processed}")
        logger.info(f"Errors: {errors}")
        logger.info(f"Total original size: {total_original_size/1024/1024/1024:.2f}GB")
        logger.info(f"Total compressed size: {total_compressed_size/1024/1024/1024:.2f}GB")
        logger.info(f"Space saved: {(total_original_size-total_compressed_size)/1024/1024/1024:.2f}GB ({savings_percentage:.2f}%)")
        logger.info(f"Total duration: {(time.time() - start_time)/60:.2f} minutes")
        
        return {
            "status": "completed",
            "files_processed": files_processed,
            "errors": errors,
            "total_original_size": total_original_size,
            "total_compressed_size": total_compressed_size,
            "savings_percentage": savings_percentage,
            "duration": time.time() - start_time
        }
    
    def get_compression_status(self) -> Dict[str, Any]:
        """Get the current status of the compression process."""
        if self.compression_start_time is None:
            return {"status": "idle", "active_jobs": []}
        
        duration = time.time() - self.compression_start_time
        
        # Get a copy of active jobs to avoid modification during iteration
        with self.jobs_lock:
            active_jobs_list = []
            for thread_id, job_info in self.active_jobs.items():
                # Calculate elapsed time for each job
                job_elapsed = time.time() - job_info["start_time"]
                
                # Get just the filename for display
                filename = job_info.get("file_name", "Unknown")
                
                # Format job info for display
                active_jobs_list.append({
                    "filename": filename,
                    "full_path": job_info["file_path"],
                    "status": job_info["status"],
                    "stage": job_info["current_stage"],
                    "progress": job_info["progress"],
                    "size_mb": job_info["file_size"] / (1024 * 1024) if job_info["file_size"] else 0,
                    "elapsed_seconds": job_elapsed,
                    "elapsed_formatted": self._format_time(job_elapsed),
                    "eta_seconds": job_info["eta"],
                    "eta_formatted": self._format_time(job_info["eta"]) if job_info["eta"] else "Unknown"
                })
        
        # Get overall ETA information
        eta_info = self.get_estimated_completion_time()
        
        # Check if system is paused
        status = "compressing"
        if self.paused:
            status = "paused"
        elif not self.running:
            status = "stopping"
        
        return {
            "status": status,
            "paused": self.paused,
            "active_jobs": active_jobs_list,
            "files_processed": self.stats["files_processed"],
            "errors": self.stats["errors"],
            "total_original_size": self.stats["total_original_size"],
            "total_compressed_size": self.stats["total_compressed_size"],
            "duration": duration,
            "duration_formatted": self._format_time(duration),
            "eta": eta_info
        }
    
    def _format_time(self, seconds: Optional[float]) -> str:
        """Format time in seconds to a human readable string."""
        if seconds is None:
            return "Unknown"
            
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            sec = int(seconds % 60)
            return f"{minutes}m {sec}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"