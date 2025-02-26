import threading
import time
import datetime
import logging
import os
import json
from .config_validator import ConfigValidator
from .media_database import MediaDatabase
from .media_scanner import MediaScanner
from .media_compressor import MediaCompressor
from .web_server import MediaCompressionWebServer
from .constants import DEFAULT_CONFIG

logger = logging.getLogger('MediaCompressor.Manager')

class MediaCompressionManager:
    """
    Main class that coordinates the scanner and compressor components,
    providing a unified interface for the entire system.
    """
    def __init__(self, config_path: str = None):
        """Initialize the media compression manager with configuration."""
        # Load configuration
        self.config = DEFAULT_CONFIG.copy()
        self.config_path = config_path
        
        # Load custom config if provided
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    custom_config = json.load(f)
                    # Recursively update nested dictionaries
                    self._deep_update(self.config, custom_config)
                
                # Validate configuration
                is_valid, errors = ConfigValidator.validate_config(self.config)
                if not is_valid:
                    logger.error("Configuration validation failed: " + "; ".join(errors))
            except Exception as e:
                logger.error(f"Error loading configuration: {str(e)}")
        
        logger.info(f"MediaCompressionManager initialized")
        
        # Initialize components
        self.db = MediaDatabase(self.config["database_path"], self.config.get("backup_path"))
        self.scanner = MediaScanner(self.config, self.db)
        self.compressor = MediaCompressor(self.config, self.db)
        self.web_server = MediaCompressionWebServer(self.config, self.db, self.scanner, self.compressor)
        
        # Start web server if enabled
        if self.config["web_interface"]["enabled"]:
            self.web_server.start()
        
        # Schedule periodic database backups
        self._schedule_db_backup()
    
    def _deep_update(self, d, u):
        """Recursively update nested dictionaries."""
        for k, v in u.items():
            if isinstance(v, dict) and k in d and isinstance(d[k], dict):
                self._deep_update(d[k], v)
            else:
                d[k] = v
    
    def _schedule_db_backup(self):
        """Schedule periodic database backups."""
        if not hasattr(self, 'backup_thread'):
            self.backup_thread = threading.Thread(target=self._backup_thread_func, daemon=True)
            self.backup_thread.start()
    
    def _backup_thread_func(self):
        """Thread function for periodic database backups."""
        backup_interval = self.config["recovery"]["db_backup_interval"] * 3600  # Convert hours to seconds
        
        while True:
            time.sleep(backup_interval)
            logger.info("Running scheduled database backup")
            self.db.backup_database()
    
    def reload_config(self):
        """Reload configuration."""
        if self.config_path:
            return self.web_server.reload_configuration(self.config_path)
        return {"success": False, "message": "No configuration file specified"}
    
    def run_scan(self):
        """Run a media scan operation."""
        return self.scanner.run_scan()
    
    def run_compression(self, limit: int = None, force_now: bool = False):
        """Run a compression operation."""
        return self.compressor.process_compression_queue(limit, force_now)
    
    def run_daemon(self):
        """Run in daemon mode, continuously scanning and compressing."""
        logger.info("Starting in daemon mode")
        
        # Flag for scan in progress
        scan_in_progress = False
        compression_in_progress = False
        
        try:
            while True:
                current_time = datetime.datetime.now()
                current_hour = current_time.hour
                
                # Determine if we're in the scheduled window
                in_schedule = (self.config["schedule"]["start_hour"] <= current_hour < 
                              self.config["schedule"]["end_hour"])
                
                # Run scan if not already in progress (can run outside schedule)
                if not scan_in_progress and not self.scanner.is_scanning:
                    scan_in_progress = True
                    
                    try:
                        logger.info("Starting media scan")
                        scan_thread = threading.Thread(target=self.run_scan)
                        scan_thread.daemon = True
                        scan_thread.start()
                        
                        # Wait a bit to avoid hitting the disk too hard with both operations
                        time.sleep(60)
                    except Exception as e:
                        logger.error(f"Error starting scan: {str(e)}")
                        self.db.log_system_event(
                            "scan_error",
                            f"Error starting scan: {str(e)}",
                            "error"
                        )
                    
                    scan_in_progress = False
                
                # Run compression if in schedule and not already in progress
                # Also respect pause state
                if in_schedule and not compression_in_progress and not self.compressor.paused:
                    compression_in_progress = True
                    
                    try:
                        logger.info("Starting compression process")
                        compression_thread = threading.Thread(
                            target=self.run_compression,
                            kwargs={"force_now": True}
                        )
                        compression_thread.daemon = True
                        compression_thread.start()
                        
                        # Wait for compression to finish or timeout
                        compression_thread.join(timeout=3600)  # 1 hour timeout
                    except Exception as e:
                        logger.error(f"Error in compression process: {str(e)}")
                        self.db.log_system_event(
                            "compression_error",
                            f"Error in compression process: {str(e)}",
                            "error"
                        )
                    
                    compression_in_progress = False
                
                # Sleep appropriately based on schedule
                if in_schedule:
                    # During schedule window, check more frequently
                    time.sleep(300)  # 5 minutes
                else:
                    # Outside schedule window, check less frequently
                    now = datetime.datetime.now()
                    start_hour = self.config["schedule"]["start_hour"]
                    
                    # Calculate time until next schedule window
                    if now.hour >= start_hour:
                        # Next window is tomorrow
                        next_window = datetime.datetime(
                            now.year, now.month, now.day, start_hour, 0, 0
                        ) + datetime.timedelta(days=1)
                    else:
                        # Next window is today
                        next_window = datetime.datetime(
                            now.year, now.month, now.day, start_hour, 0, 0
                        )
                    
                    seconds_until_window = (next_window - now).total_seconds()
                    
                    # Sleep until next schedule or at most 1 hour
                    sleep_time = min(seconds_until_window, 3600)
                    logger.info(f"Outside schedule window, sleeping for {sleep_time/60:.1f} minutes")
                    time.sleep(sleep_time)
        
        except KeyboardInterrupt:
            logger.info("Daemon interrupted, exiting")
            self.db.log_system_event("daemon_stopped", "Daemon interrupted by user", "info")
        except Exception as e:
            logger.error(f"Unexpected error in daemon mode: {str(e)}")
            self.db.log_system_event(
                "daemon_error",
                f"Unexpected error in daemon mode: {str(e)}",
                "error"
            )
