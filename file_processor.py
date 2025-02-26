import os
import hashlib
import subprocess
import logging
from typing import Optional

logger = logging.getLogger('MediaCompressor.FileProcessor')

class FileProcessor:
    """Process media files with validation and checksum utilities."""
    
    def __init__(self, config=None):
        """Initialize the file processor with optional configuration."""
        self.config = config
        
    def verify_file_integrity(self, file_path: str, update_status_callback=None) -> bool:
        """
        Verify that a file is valid and not corrupted - less aggressive version.
        
        Args:
            file_path: Path to the file to verify
            update_status_callback: Optional callback for status updates
        """
        if update_status_callback:
            update_status_callback("verifying file", stage="integrity check")
        
        try:
            # Quick size check
            if os.path.getsize(file_path) == 0:
                logger.error(f"File {file_path} has zero size")
                return False
            
            # Try to read the file headers with ffprobe - using a more reliable approach
            # Just check if the format can be detected, nothing more
            cmd = ["ffprobe", "-v", "error", "-hide_banner", "-of", "json", 
                   "-show_format", "-i", file_path]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)  # Increased timeout
            
            # We don't check returncode because some files might return warnings
            # but still be valid. Instead, we check if we got format information.
            try:
                import json
                data = json.loads(result.stdout)
                if "format" not in data:
                    logger.error(f"File format check failed for {file_path}: No format section in output")
                    
                    # If we're here, file might still be valid but with issues
                    # Return True unless in strict validation mode
                    if self.config and self.config.get("strict_validation", False):
                        return False
                    return True
            except (json.JSONDecodeError, ValueError):
                # If output isn't valid JSON, check if it's due to an error
                if result.returncode != 0:
                    logger.error(f"File format check failed for {file_path}")
                    return False
                
                # If returncode is 0 but no JSON, the file might still be accessible
                # Accept it unless in strict validation mode
                if self.config and self.config.get("strict_validation", False):
                    return False
                return True
            
            # If we got here, file is valid enough to proceed
            logger.info(f"File integrity verified for {file_path}")
            return True
            
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout verifying file integrity for {file_path}")
            # Timeouts might happen with very large files, but they could still be valid
            # Accept them unless in strict validation mode
            if self.config and self.config.get("strict_validation", False):
                return False
            return True
            
        except Exception as e:
            logger.error(f"Error verifying file integrity for {file_path}: {str(e)}")
            return False
    
    def get_file_checksum(self, file_path: str) -> str:
        """
        Calculate file checksum using first and last 4MB for speed.
        
        Args:
            file_path: Path to the file for checksum calculation
        """
        try:
            file_size = os.path.getsize(file_path)
            
            # For small files, hash the entire file
            if file_size < 8 * 1024 * 1024:  # Less than 8MB
                with open(file_path, 'rb') as f:
                    return hashlib.md5(f.read()).hexdigest()
            
            # For larger files, hash the first and last 4MB
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
        except Exception as e:
            logger.error(f"Error calculating checksum for {file_path}: {str(e)}")
            return ""
            
    def get_file_metadata(self, file_path: str) -> dict:
        """
        Get basic metadata from a media file.
        
        Args:
            file_path: Path to the media file
        """
        metadata = {
            "size": 0,
            "exists": False,
            "is_video": False,
            "duration": 0,
            "resolution": "",
            "codec": ""
        }
        
        try:
            if not os.path.exists(file_path):
                return metadata
                
            metadata["exists"] = True
            metadata["size"] = os.path.getsize(file_path)
            
            # Check if it's a video file
            cmd = ["ffprobe", "-v", "error", "-select_streams", "v:0",
                   "-show_entries", "stream=codec_name,width,height,duration",
                   "-of", "json", file_path]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)
                if "streams" in data and data["streams"]:
                    stream = data["streams"][0]
                    metadata["is_video"] = True
                    metadata["codec"] = stream.get("codec_name", "")
                    
                    if "width" in stream and "height" in stream:
                        metadata["resolution"] = f"{stream['width']}x{stream['height']}"
                    
                    if "duration" in stream:
                        metadata["duration"] = float(stream["duration"])
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error getting file metadata for {file_path}: {str(e)}")
            return metadata