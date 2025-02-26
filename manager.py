import threading
import time
import datetime
import logging
import os
import json
import signal
import sys
from typing import Dict, Any, Optional, List
from contextlib import contextmanager
from config_validator import ConfigValidator
from media_database import MediaDatabase
from media_scanner import MediaScanner
from media_compressor import MediaCompressor
from web_server import MediaCompressionWebServer
from constants import DEFAULT_CONFIG

logger = logging.getLogger('MediaCompressor.Manager')

class MediaCompressionManager:
    """
    Main class that coordinates the scanner and compressor components,
    providing a unified interface for the entire system.
    """
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the media compression manager with configuration."""
        # Load configuration
        self.config = DEFAULT_CONFIG.copy()
        self.config_path = config_path
        self.shutdown_requested = False
        
        # Set up signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
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
        
        # Thread synchronization for daemon mode
        self.daemon_lock = threading.RLock()
        self.scan_in_progress = False
        self.compression_in_progress = False
        
        # Start web server if enabled
        if self.config["web_interface"]["enabled"]:
            self.web_server.start()
        
        # Schedule periodic database backups
        self._schedule_db_backup()
    
    def _setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown."""
        try:
            signal.signal(signal.SIGINT, self._handle_shutdown)
            signal.signal(signal.SIGTERM, self._handle_shutdown)
            # SIGKILL cannot be caught
        except (AttributeError, ValueError):
            # May fail on Windows or in certain environments
            logger.warning("Could not set up signal handlers for graceful shutdown")
    
    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating immediate shutdown")
        self.shutdown_requested = True
        
        # Stop all active operations
        self.compressor.stop_compression()
        self.scanner.stop_scan()
        
        # Clean up resources
        self._cleanup_resources()
        
        # Set a flag to exit daemon mode if running
        if hasattr(self, 'daemon_lock'):
            with self.daemon_lock:
                self.scan_in_progress = False
                self.compression_in_progress = False
        
        # Exit immediately on SIGINT (Ctrl+C) if not in daemon mode
        if signum == signal.SIGINT and not hasattr(self, 'in_daemon_mode') or not self.in_daemon_mode:
            logger.info("Exiting immediately")
            sys.exit(1)
    
    def _cleanup_resources(self):
        """Clean up resources before shutdown."""
        logger.info("Cleaning up resources before shutdown")
        
        # Stop web server if running
        if hasattr(self, 'web_server') and self.config["web_interface"]["enabled"]:
            try:
                # TODO: Implement web_server.stop() method if needed
                pass
            except Exception as e:
                logger.error(f"Error stopping web server: {e}")
        
        # Ensure database connections are closed
        if hasattr(self, 'db'):
            try:
                # Run a final backup
                self.db.backup_database()
            except Exception as e:
                logger.error(f"Error during final database backup: {e}")
    
    def _deep_update(self, d: Dict[str, Any], u: Dict[str, Any]) -> None:
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
        
        while not self.shutdown_requested:
            time.sleep(backup_interval)
            logger.info("Running scheduled database backup")
            self.db.backup_database()
    
    def reload_config(self) -> Dict[str, Any]:
        """Reload configuration."""
        if self.config_path:
            return self.web_server.reload_configuration(self.config_path)
        return {"success": False, "message": "No configuration file specified"}
    
    def run_scan(self) -> Dict[str, Any]:
        """Run a media scan operation."""
        with self.daemon_lock:
            self.scan_in_progress = True
        
        try:
            result = self.scanner.run_scan()
            return result
        finally:
            with self.daemon_lock:
                self.scan_in_progress = False
    
    def run_compression(self, limit: Optional[int] = None, force_now: bool = False) -> Dict[str, Any]:
        """Run a compression operation."""
        with self.daemon_lock:
            self.compression_in_progress = True
        
        try:
            result = self.compressor.process_compression_queue(limit, force_now)
            return result
        finally:
            with self.daemon_lock:
                self.compression_in_progress = False
    
    @contextmanager
    def daemon_context(self):
        """Context manager for daemon mode operations."""
        try:
            yield
        except Exception as e:
            logger.error(f"Error in daemon operation: {str(e)}")
            self.db.log_system_event(
                "daemon_error",
                f"Error in daemon operation: {str(e)}",
                "error"
            )
    
    def _calculate_next_window_sleep_time(self) -> float:
        """Calculate sleep time until next schedule window."""
        now = datetime.datetime.now()
        start_hour = self.config["schedule"]["start_hour"]
        
        # Calculate time until next window
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
        
        sleep_seconds = (next_window - now).total_seconds()
        # Cap at 1 hour to periodically check for config changes
        return min(sleep_seconds, 3600)
    
    def run_daemon(self):
        """Run in daemon mode, continuously scanning and compressing."""
        logger.info("Starting in daemon mode")
        self.in_daemon_mode = True
        
        try:
            while not self.shutdown_requested:
                current_time = datetime.datetime.now()
                current_hour = current_time.hour
                
                # Determine if we're in the scheduled window
                in_schedule = (self.config["schedule"]["start_hour"] <= current_hour < 
                              self.config["schedule"]["end_hour"])
                
                # Run scan if not already in progress (can run outside schedule)
                with self.daemon_lock:
                    if not self.scan_in_progress and not self.scanner.is_scanning:
                        self.scan_in_progress = True
                
                if self.scan_in_progress:
                    with self.daemon_context():
                        logger.info("Starting media scan")
                        scan_thread = threading.Thread(target=self.run_scan)
                        scan_thread.daemon = True
                        scan_thread.start()
                        
                        # Wait a bit to avoid hitting the disk too hard with both operations
                        time.sleep(60)
                
                # Run compression if in schedule and not already in progress
                # Also respect pause state
                with self.daemon_lock:
                    if (in_schedule and not self.compression_in_progress 
                        and not self.compressor.paused):
                        self.compression_in_progress = True
                
                if self.compression_in_progress:
                    with self.daemon_context():
                        logger.info("Starting compression process")
                        compression_thread = threading.Thread(
                            target=self.run_compression,
                            kwargs={"force_now": True}
                        )
                        compression_thread.daemon = True
                        compression_thread.start()
                        
                        # Wait for compression to finish or timeout
                        compression_thread.join(timeout=3600)  # 1 hour timeout
                
                # Sleep appropriately based on schedule
                if in_schedule:
                    # During schedule window, check more frequently
                    logger.debug("In schedule window, sleeping for 5 minutes")
                    time.sleep(300)  # 5 minutes
                else:
                    # Outside schedule window, sleep until next window or max 1 hour
                    sleep_time = self._calculate_next_window_sleep_time()
                    logger.info(f"Outside schedule window, sleeping for {sleep_time/60:.1f} minutes")
                    
                    # Use smaller sleep intervals to check for shutdown
                    end_time = time.time() + sleep_time
                    while time.time() < end_time and not self.shutdown_requested:
                        time.sleep(min(300, end_time - time.time()))  # Sleep up to 5 minutes at a time
        
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
        finally:
            self.in_daemon_mode = False
            self._cleanup_resources()
            logger.info("Daemon mode exited")