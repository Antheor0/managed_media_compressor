import sqlite3
import os
import shutil
import datetime
import logging
from pathlib import Path
from typing import Dict, List
from datetime import time

from constants import *

logger = logging.getLogger('MediaCompressor.Database')

class MediaDatabase:
    """
    Database manager for tracking file processing status.
    Separated from the main compressor class for better organization.
    """
    def __init__(self, db_path: str, backup_path: str = None):
        self.db_path = db_path
        self.backup_path = backup_path or db_path + ".backup"
        self.last_backup_time = None
        self._init_database()
    
    def _init_database(self):
        """Initialize the SQLite database with enhanced schema for tracking files."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Enhanced processed_files table with more status info
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_files (
                id INTEGER PRIMARY KEY,
                file_path TEXT UNIQUE,
                file_name TEXT,              -- Just the file name for quicker display
                directory_path TEXT,         -- Directory for grouping and filtering
                original_size INTEGER,
                compressed_size INTEGER,
                first_seen_date TIMESTAMP,   -- When the file was first discovered
                last_checked_date TIMESTAMP, -- Last time file was checked
                compression_date TIMESTAMP,  -- When compression was completed
                queued_date TIMESTAMP,       -- When file was queued for compression
                processing_started TIMESTAMP,-- When compression started
                checksum TEXT,
                content_type TEXT,
                quality_score REAL,
                status TEXT,                 -- More detailed status tracking
                error_message TEXT,          -- Store full error messages
                skip_reason TEXT,            -- Reason for skipping
                compression_count INTEGER DEFAULT 0, -- Number of times compressed
                priority INTEGER DEFAULT 0,   -- Priority for compression queue
                estimated_time INTEGER DEFAULT 0, -- Estimated processing time in seconds
                actual_time INTEGER DEFAULT 0     -- Actual processing time in seconds
            )
            ''')
            
            # Session stats table (unchanged)
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS compression_stats (
                id INTEGER PRIMARY KEY,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                files_processed INTEGER,
                total_original_size INTEGER,
                total_compressed_size INTEGER,
                savings_percentage REAL,
                errors INTEGER
            )
            ''')
            
            # New table for tracking directories
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS scanned_directories (
                id INTEGER PRIMARY KEY,
                directory_path TEXT UNIQUE,
                last_scan_date TIMESTAMP,
                file_count INTEGER,
                total_size INTEGER,
                scan_duration REAL,
                status TEXT
            )
            ''')

            # New table for system events and errors
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_events (
                id INTEGER PRIMARY KEY,
                timestamp TIMESTAMP,
                event_type TEXT,
                details TEXT,
                severity TEXT
            )
            ''')
            
            # Improved indexing for faster lookups
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_path ON processed_files (file_path)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON processed_files (status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_directory ON processed_files (directory_path)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_first_seen ON processed_files (first_seen_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_priority ON processed_files (priority)')
            
            conn.commit()
            conn.close()
            
            logger.info(f"Database initialized at {self.db_path}")
            
            # Create initial backup
            self.backup_database()
            
            # Ensure schema is up to date with current code
            self._ensure_schema_updated()
            
        except sqlite3.Error as e:
            logger.error(f"Database initialization error: {str(e)}")
            self.repair_database()

    def _ensure_schema_updated(self):
        """Check if database schema is current and update if needed."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get current columns in processed_files table
            cursor.execute("PRAGMA table_info(processed_files)")
            columns = [row[1] for row in cursor.fetchall()]
            
            # Check for missing columns and add them
            expected_columns = {
                "estimated_time": "INTEGER DEFAULT 0",
                "actual_time": "INTEGER DEFAULT 0",
                "priority": "INTEGER DEFAULT 0"
            }
            
            for col_name, col_type in expected_columns.items():
                if col_name not in columns:
                    logger.info(f"Adding missing column {col_name} to processed_files table")
                    cursor.execute(f"ALTER TABLE processed_files ADD COLUMN {col_name} {col_type}")
            
            conn.commit()
            conn.close()
            return True
        except sqlite3.Error as e:
            logger.error(f"Error updating database schema: {str(e)}")
            return False
    
    def backup_database(self):
        """Create a backup of the database."""
        try:
            if os.path.exists(self.db_path):
                shutil.copy2(self.db_path, self.backup_path)
                self.last_backup_time = datetime.datetime.now()
                logger.info(f"Database backup created at {self.backup_path}")
                return True
        except Exception as e:
            logger.error(f"Failed to backup database: {str(e)}")
        return False
    
    def repair_database(self):
        """Attempt to repair the database from a backup or by rebuilding."""
        try:
            # First try to restore from backup
            if os.path.exists(self.backup_path):
                logger.warning(f"Attempting to restore database from backup {self.backup_path}")
                
                # Rename corrupt database
                if os.path.exists(self.db_path):
                    corrupt_path = f"{self.db_path}.corrupt.{int(time.time())}"
                    shutil.move(self.db_path, corrupt_path)
                    logger.warning(f"Moved corrupt database to {corrupt_path}")
                
                # Restore backup
                shutil.copy2(self.backup_path, self.db_path)
                logger.info(f"Database restored from backup")
                
                # Verify the restored database
                try:
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    cursor.execute("SELECT count(*) FROM processed_files")
                    count = cursor.fetchone()[0]
                    conn.close()
                    logger.info(f"Restored database verified, contains {count} files")
                    return True
                except sqlite3.Error:
                    logger.error("Restored database also corrupt, will rebuild")
            
            # If backup doesn't exist or is also corrupt, rebuild the database
            logger.warning("Rebuilding database from scratch")
            
            # Rename corrupt database if it exists
            if os.path.exists(self.db_path):
                corrupt_path = f"{self.db_path}.corrupt.{int(time.time())}"
                shutil.move(self.db_path, corrupt_path)
            
            # Initialize a fresh database
            self._init_database()
            
            # Log the event
            self.log_system_event("database_rebuilt", "Database was rebuilt due to corruption", "error")
            
            logger.info("Database has been rebuilt")
            return True
            
        except Exception as e:
            logger.error(f"Database repair failed: {str(e)}")
            return False
    
    def check_database_integrity(self):
        """Check the integrity of the SQLite database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            conn.close()
            
            if result[0] != "ok":
                logger.error(f"Database integrity check failed: {result[0]}")
                return False
            
            return True
        except sqlite3.Error as e:
            logger.error(f"Database integrity check error: {str(e)}")
            return False
    
    def log_system_event(self, event_type: str, details: str, severity: str = "info"):
        """Log a system event to the database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT INTO system_events (timestamp, event_type, details, severity)
            VALUES (?, ?, ?, ?)
            ''', (
                datetime.datetime.now().isoformat(),
                event_type,
                details,
                severity
            ))
            
            conn.commit()
            conn.close()
        except sqlite3.Error as e:
            logger.error(f"Failed to log system event: {str(e)}")
    
    def get_file_status(self, file_path: str) -> Dict:
        """Get the status of a file from the database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT id, status, checksum, original_size, compressed_size, priority FROM processed_files WHERE file_path = ?", 
                (file_path,)
            )
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    "id": result[0],
                    "status": result[1],
                    "checksum": result[2],
                    "original_size": result[3],
                    "compressed_size": result[4],
                    "priority": result[5],
                    "in_database": True
                }
            else:
                return {"in_database": False}
        except sqlite3.Error as e:
            logger.error(f"Database error in get_file_status: {str(e)}")
            return {"in_database": False, "error": str(e)}
    
    def add_new_file(self, file_info: Dict):
        """Add a new file to the database with initial status."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            now = datetime.datetime.now().isoformat()
            
            cursor.execute('''
            INSERT INTO processed_files 
            (file_path, file_name, directory_path, original_size, first_seen_date, 
             last_checked_date, checksum, status, priority)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                file_info["file_path"],
                os.path.basename(file_info["file_path"]),
                os.path.dirname(file_info["file_path"]),
                file_info["size"],
                now,
                now,
                file_info.get("checksum", ""),
                file_info.get("status", STATUS_NEW),
                file_info.get("priority", 0)
            ))
            
            conn.commit()
            conn.close()
        except sqlite3.Error as e:
            logger.error(f"Database error in add_new_file: {str(e)}")
            # If it's a unique constraint error, try to update instead
            if "UNIQUE constraint failed" in str(e):
                self.update_file_status(
                    file_info["file_path"],
                    file_info.get("status", STATUS_NEW),
                    last_checked_date=now,
                    checksum=file_info.get("checksum", "")
                )
    
    def update_file_status(self, file_path: str, status: str, **kwargs):
        """Update the status and other fields of a file in the database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Build the SQL query dynamically based on provided kwargs
            fields = ["status = ?"]
            values = [status]
            
            for key, value in kwargs.items():
                fields.append(f"{key} = ?")
                values.append(value)
            
            # Add the file_path to values
            values.append(file_path)
            
            sql = f"UPDATE processed_files SET {', '.join(fields)} WHERE file_path = ?"
            cursor.execute(sql, values)
            
            conn.commit()
            conn.close()
        except sqlite3.Error as e:
            logger.error(f"Database error in update_file_status: {str(e)}")
            if "no such table" in str(e).lower() or "database is locked" in str(e).lower():
                self.repair_database()
                # Try operation again after repair
                try:
                    self.update_file_status(file_path, status, **kwargs)
                except:
                    pass
    
    def get_files_for_compression(self, limit: int = 100) -> List[Dict]:
        """Get a batch of files that are ready for compression, ordered by priority."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # This enables column access by name
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT file_path, original_size, checksum, priority, estimated_time
            FROM processed_files
            WHERE status = ?
            ORDER BY priority DESC, original_size DESC
            LIMIT ?
            ''', (STATUS_PENDING, limit))
            
            files = [dict(row) for row in cursor.fetchall()]
            conn.close()
            
            return files
        except sqlite3.Error as e:
            logger.error(f"Database error in get_files_for_compression: {str(e)}")
            self.repair_database()
            return []
    
    def record_directory_scan(self, directory: str, file_count: int, total_size: int, duration: float):
        """Record information about a directory scan."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT OR REPLACE INTO scanned_directories
            (directory_path, last_scan_date, file_count, total_size, scan_duration, status)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                directory,
                datetime.datetime.now().isoformat(),
                file_count,
                total_size,
                duration,
                "completed"
            ))
            
            conn.commit()
            conn.close()
        except sqlite3.Error as e:
            logger.error(f"Database error in record_directory_scan: {str(e)}")
    
    def update_compression_time(self, file_path: str, actual_time: int):
        """Update the actual compression time for a file and adjust estimated times."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Update actual time for this file
            cursor.execute('''
            UPDATE processed_files
            SET actual_time = ?
            WHERE file_path = ?
            ''', (actual_time, file_path))
            
            # Get file size for calculating time per MB
            cursor.execute('SELECT original_size FROM processed_files WHERE file_path = ?', (file_path,))
            result = cursor.fetchone()
            
            if result and result[0] > 0:
                original_size_mb = result[0] / (1024 * 1024)
                time_per_mb = actual_time / max(1, original_size_mb)
                
                # Update estimated times for pending files based on this rate
                cursor.execute('''
                UPDATE processed_files
                SET estimated_time = ROUND(original_size * ? / (1024 * 1024))
                WHERE status = ? AND estimated_time = 0
                ''', (time_per_mb, STATUS_PENDING))
            
            conn.commit()
            conn.close()
        except sqlite3.Error as e:
            logger.error(f"Database error in update_compression_time: {str(e)}")
    
    def get_statistics(self) -> Dict:
        """Get various statistics from the database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get file counts by status
            cursor.execute('''
            SELECT status, COUNT(*) as count 
            FROM processed_files 
            GROUP BY status
            ''')
            status_counts = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Calculate total files
            cursor.execute('SELECT COUNT(*) FROM processed_files')
            total_files = cursor.fetchone()[0] or 0
            
            # Get total sizes
            cursor.execute('''
            SELECT 
                SUM(original_size) as total_original,
                SUM(compressed_size) as total_compressed
            FROM processed_files
            WHERE status = ?
            ''', (STATUS_COMPLETED,))
            
            size_row = cursor.fetchone()
            total_original = size_row[0] or 0
            total_compressed = size_row[1] or 0
            
            # Get recent compression stats
            cursor.execute('''
            SELECT 
                SUM(files_processed) as total_files,
                SUM(total_original_size) as total_original,
                SUM(total_compressed_size) as total_compressed
            FROM compression_stats
            ORDER BY end_time DESC
            LIMIT 10
            ''')
            
            stats_row = cursor.fetchone()
            recent_files = stats_row[0] or 0
            recent_original = stats_row[1] or 0
            recent_compressed = stats_row[2] or 0
            
            # Get average processing times
            cursor.execute('''
            SELECT 
                AVG(actual_time) as avg_time,
                MIN(actual_time) as min_time,
                MAX(actual_time) as max_time
            FROM processed_files
            WHERE status = ? AND actual_time > 0
            ''', (STATUS_COMPLETED,))
            
            time_row = cursor.fetchone()
            avg_time = time_row[0] or 0
            min_time = time_row[1] or 0
            max_time = time_row[2] or 0
            
            # Get estimated total remaining time
            cursor.execute('''
            SELECT SUM(estimated_time) as total_estimated_time
            FROM processed_files
            WHERE status = ?
            ''', (STATUS_PENDING,))
            
            eta_row = cursor.fetchone()
            total_eta = eta_row[0] or 0
            
            conn.close()
            
            # Calculate savings
            if total_original > 0:
                savings_percentage = ((total_original - total_compressed) / total_original) * 100
            else:
                savings_percentage = 0
            
            return {
                "status_counts": status_counts,
                "total_files": total_files,  # Explicitly include total files count
                "total_original_size": total_original,
                "total_compressed_size": total_compressed,
                "space_saved": total_original - total_compressed,
                "savings_percentage": savings_percentage,
                "recent_compression": {
                    "files": recent_files,
                    "original_size": recent_original,
                    "compressed_size": recent_compressed
                },
                "processing_times": {
                    "average_seconds": avg_time,
                    "min_seconds": min_time,
                    "max_seconds": max_time
                },
                "estimated_remaining_time": total_eta
            }
        except sqlite3.Error as e:
            logger.error(f"Database error in get_statistics: {str(e)}")
            self.repair_database()
            return {
                "status_counts": {},
                "total_files": 0,
                "total_original_size": 0,
                "total_compressed_size": 0,
                "space_saved": 0,
                "savings_percentage": 0,
                "recent_compression": {
                    "files": 0,
                    "original_size": 0,
                    "compressed_size": 0
                },
                "processing_times": {
                    "average_seconds": 0,
                    "min_seconds": 0,
                    "max_seconds": 0
                },
                "estimated_remaining_time": 0,
                "error": str(e)
            }
    
    def bulk_update_statuses(self, file_list: List[Dict]):
        """Update multiple file statuses in a single transaction for efficiency."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            try:
                cursor.execute("BEGIN TRANSACTION")
                
                for file_info in file_list:
                    file_path = file_info["file_path"]
                    status = file_info["status"]
                    
                    # Extract additional fields if present
                    additional_fields = {}
                    for key, value in file_info.items():
                        if key not in ["file_path", "status"]:
                            additional_fields[key] = value
                    
                    # Build the SQL dynamically
                    fields = ["status = ?"]
                    values = [status]
                    
                    for key, value in additional_fields.items():
                        fields.append(f"{key} = ?")
                        values.append(value)
                    
                    # Add file_path
                    values.append(file_path)
                    
                    cursor.execute(
                        f"UPDATE processed_files SET {', '.join(fields)} WHERE file_path = ?",
                        values
                    )
                
                conn.commit()
                logger.debug(f"Bulk updated {len(file_list)} files")
                
            except Exception as e:
                conn.rollback()
                logger.error(f"Error in bulk update: {str(e)}")
                
            finally:
                conn.close()
        except sqlite3.Error as e:
            logger.error(f"Database error in bulk_update_statuses: {str(e)}")
            self.repair_database()
    
    def get_recent_events(self, limit: int = 100) -> List[Dict]:
        """Get recent system events from the database."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT id, timestamp, event_type, details, severity
            FROM system_events
            ORDER BY timestamp DESC
            LIMIT ?
            ''', (limit,))
            
            events = [dict(row) for row in cursor.fetchall()]
            conn.close()
            
            return events
        except sqlite3.Error as e:
            logger.error(f"Database error in get_recent_events: {str(e)}")
            return []