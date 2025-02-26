import os
import subprocess
import logging
from typing import List, Dict, Tuple, Optional, Set, Any, Callable

logger = logging.getLogger('MediaCompressor.ConfigValidator')

class ConfigValidator:
    """
    Validates configuration settings to ensure they're valid before running the system.
    Helps prevent runtime errors from incorrect configuration.
    """
    @staticmethod
    def validate_config(config: Dict) -> Tuple[bool, List[str]]:
        """
        Validate the configuration and return validation status and any error messages.
        
        Args:
            config: The configuration dictionary to validate
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []
        
        # Validate media paths
        for path in config.get("media_paths", []):
            if not os.path.exists(path):
                errors.append(f"Media path does not exist: {path}")
            elif not os.path.isdir(path):
                errors.append(f"Media path is not a directory: {path}")
        
        # Validate schedule times
        start_hour = config.get("schedule", {}).get("start_hour", 0)
        end_hour = config.get("schedule", {}).get("end_hour", 0)
        
        if not (0 <= start_hour < 24):
            errors.append(f"Invalid start hour: {start_hour}. Must be 0-23.")
        if not (0 <= end_hour < 24):
            errors.append(f"Invalid end hour: {end_hour}. Must be 0-23.")
            
        if start_hour == end_hour:
            errors.append(f"Start hour and end hour cannot be the same: {start_hour}")
        
        # Validate HandBrakeCLI path
        handbrake_path = config.get("compression", {}).get("handbrake_path", "")
        try:
            subprocess.run([handbrake_path, "--version"], 
                          capture_output=True, check=False)
        except FileNotFoundError:
            errors.append(f"HandBrakeCLI not found at: {handbrake_path}")
        
        # Validate quality settings
        quality_threshold = config.get("quality_validation", {}).get("threshold", 0)
        if not (0 <= quality_threshold <= 100):
            errors.append(f"Invalid quality threshold: {quality_threshold}. Must be 0-100.")
        
        # Validate temp directory
        temp_dir = config.get("temp_dir", "")
        if not os.path.exists(temp_dir):
            try:
                os.makedirs(temp_dir, exist_ok=True)
            except Exception as e:
                errors.append(f"Could not create temp directory {temp_dir}: {str(e)}")
        elif not os.access(temp_dir, os.W_OK):
            errors.append(f"Temp directory {temp_dir} is not writable")
        
        # Validate database path
        db_path = config.get("database_path", "")
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            try:
                os.makedirs(db_dir, exist_ok=True)
            except Exception as e:
                errors.append(f"Could not create database directory {db_dir}: {str(e)}")
        
        # Validate web interface settings
        if config.get("web_interface", {}).get("enabled", False):
            port = config.get("web_interface", {}).get("port", 0)
            if not (1024 <= port <= 65535):
                errors.append(f"Invalid web interface port: {port}. Must be 1024-65535.")
            
            if config.get("web_interface", {}).get("secure", False):
                if not config.get("web_interface", {}).get("username"):
                    errors.append("Missing username for secure web interface")
                if not config.get("web_interface", {}).get("password"):
                    errors.append("Missing password for secure web interface")
        
        # Validate notification settings
        if config.get("notifications", {}).get("email", {}).get("enabled", False):
            email_config = config.get("notifications", {}).get("email", {})
            required_fields = ["smtp_server", "smtp_port", "username", "password", "from_addr", "to_addr"]
            
            for field in required_fields:
                if not email_config.get(field):
                    errors.append(f"Missing required email configuration: {field}")
        
        # Validate min_size_mb is reasonable
        min_size = config.get("min_size_mb", 0)
        if min_size <= 0:
            errors.append(f"min_size_mb must be positive, got {min_size}")
        
        # Check if other validation results should be warnings instead of errors
        warnings = []
        for i in range(len(errors)-1, -1, -1):
            # Convert some errors to warnings based on severity
            if "Media path does not exist" in errors[i]:
                warnings.append(errors[i])
                errors.pop(i)
        
        # Log warnings but don't fail validation for them
        for warning in warnings:
            logger.warning(f"Configuration warning: {warning}")
        
        return len(errors) == 0, errors
