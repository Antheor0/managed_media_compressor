import psutil
import shutil
import datetime
import logging
import subprocess
from typing import Dict, Any, Optional

logger = logging.getLogger('MediaCompressor.ResourceMonitor')

class ResourceMonitor:
    """Monitor system resources for media compression operations."""
    
    def __init__(self, config: Dict[str, Any], db_logger=None):
        """Initialize the resource monitor with configuration."""
        self.config = config
        self.db_logger = db_logger  # For database event logging
    
    def check_system_resources(self) -> bool:
        """Check if system has sufficient resources for compression."""
        # Check disk space
        if not self.check_disk_space(self.config["temp_dir"]):
            return False
        
        # Check memory
        memory = psutil.virtual_memory()
        available_mb = memory.available / (1024 * 1024)
        
        if available_mb < self.config["min_memory_mb"]:
            logger.warning(f"Low memory: {available_mb:.2f}MB available, minimum {self.config['min_memory_mb']}MB required")
            if self.db_logger:
                self.db_logger("resource_warning", f"Low memory: {available_mb:.2f}MB available", "warning")
            return False
        
        # Check CPU load
        cpu_percent = psutil.cpu_percent(interval=1)
        if cpu_percent > 90:  # Allow high CPU usage but warn
            logger.warning(f"High CPU usage: {cpu_percent}%")
            if self.db_logger:
                self.db_logger("resource_warning", f"High CPU usage: {cpu_percent}%", "warning")
        
        return True
    
    def check_disk_space(self, path: str, required_mb: Optional[int] = None) -> bool:
        """
        Ensure sufficient disk space exists before compressing.
        
        Args:
            path: Path to check for disk space
            required_mb: Required free space in MB, uses config value if None
        """
        if required_mb is None:
            required_mb = self.config["min_free_space_mb"]
        
        try:
            disk_stats = shutil.disk_usage(path)
            free_space_mb = disk_stats.free / (1024 * 1024)
            
            if free_space_mb < required_mb:
                error_msg = f"Insufficient disk space on {path}: {free_space_mb:.2f}MB free, {required_mb}MB required"
                logger.error(error_msg)
                
                # Log to DB if logger available
                if self.db_logger:
                    self.db_logger("disk_space_error", error_msg, "error")
                
                return False
            
            return True
        except Exception as e:
            logger.error(f"Error checking disk space on {path}: {str(e)}")
            return False
    
    def check_system_load(self) -> bool:
        """Check if system load is low enough to run compression tasks."""
        # Get CPU usage
        cpu_usage = psutil.cpu_percent(interval=1)
        
        # Get memory usage
        memory_usage = psutil.virtual_memory().percent
        
        # Get GPU usage (simplified, in production you'd use pynvml or similar)
        gpu_usage = 0
        try:
            gpu_info = subprocess.run(
                ["nvidia-smi", "--query-gpu=utilization.gpu", "--format=csv,noheader,nounits"],
                capture_output=True, text=True, check=True
            )
            gpu_usage = float(gpu_info.stdout.strip())
        except Exception:
            # If can't get GPU usage, assume it's available
            pass
        
        logger.debug(f"System load: CPU {cpu_usage}%, Memory {memory_usage}%, GPU {gpu_usage}%")
        
        # Check if system is under heavy load
        if cpu_usage > 80 or memory_usage > 90 or gpu_usage > 80:
            logger.info(f"System under heavy load (CPU: {cpu_usage}%, Memory: {memory_usage}%, GPU: {gpu_usage}%), pausing")
            return False
        
        return True
    
    def is_within_schedule(self) -> bool:
        """Check if current time is within the scheduled window."""
        now = datetime.datetime.now()
        start_hour = self.config["schedule"]["start_hour"]
        end_hour = self.config["schedule"]["end_hour"]
        
        current_hour = now.hour
        
        # If dynamic scheduling is enabled, also check system load
        if self.config["schedule"]["dynamic_scheduling"]:
            if not self.check_system_load():
                return False
        
        return start_hour <= current_hour < end_hour