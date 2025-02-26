import os
import re
import subprocess
import logging
import shutil
import time
from typing import Dict, List, Tuple, Optional, Any

logger = logging.getLogger('MediaCompressor.CompressionEngine')

class CompressionEngine:
    """Engine for compressing media files using HandBrakeCLI."""
    
    def __init__(self, config: Dict[str, Any], quality_validator=None):
        """Initialize the compression engine with configuration."""
        self.config = config
        self.quality_validator = quality_validator
        self.dependencies_checked = False
    
    def check_dependencies(self) -> bool:
        """Check if all required external tools are available."""
        if self.dependencies_checked:
            return True
            
        dependencies = [
            {"name": "HandBrakeCLI", "cmd": [self.config["compression"]["handbrake_path"], "--version"]},
            {"name": "ffmpeg", "cmd": ["ffmpeg", "-version"]},
            {"name": "ffprobe", "cmd": ["ffprobe", "-version"]}
        ]
        
        missing_deps = []
        
        for dep in dependencies:
            try:
                result = subprocess.run(dep["cmd"], capture_output=True, check=False)
                if result.returncode != 0:
                    missing_deps.append(dep["name"])
                    logger.error(f"Dependency {dep['name']} check failed with return code {result.returncode}")
                else:
                    logger.info(f"Dependency {dep['name']} is available")
            except (subprocess.SubprocessError, FileNotFoundError) as e:
                missing_deps.append(dep["name"])
                logger.error(f"Dependency {dep['name']} not found: {str(e)}")
        
        if missing_deps:
            error_msg = f"Missing dependencies: {', '.join(missing_deps)}"
            logger.error(error_msg)
            # We'll let the caller handle logging to database
            return False
        
        self.dependencies_checked = True
        return True
    
    def prepare_compression(self, file_path: str, compression_settings: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Prepare for compression by setting up output paths and settings."""
        # Generate temporary output path
        file_name = os.path.basename(file_path)
        base_name, ext = os.path.splitext(file_name)
        temp_output = os.path.join(self.config["temp_dir"], f"{base_name}_compressed{ext}")
        
        # Get content-specific compression settings
        content_type = compression_settings.get("content_type", "live_action")
        
        # Update NVENC options with content-aware settings
        nvenc_base = self.config["compression"]["nvenc_options"]
        nvenc_options = re.sub(
            r'--quality\s+\d+',
            f'--quality {compression_settings["quality"]}',
            nvenc_base
        )
        nvenc_options = re.sub(
            r'--encoder-preset\s+\w+',
            f'--encoder-preset {compression_settings["preset"]}',
            nvenc_options
        )
        
        return temp_output, {
            "nvenc_options": nvenc_options,
            "content_type": content_type
        }
    
    def run_handbrake(self, file_path: str, temp_output: str, settings: Dict[str, Any], 
                     status_callback=None, paused_check=None, running_check=None) -> bool:
        """
        Run HandBrakeCLI command with progress monitoring.
        
        Args:
            file_path: Path to input file
            temp_output: Path to output file
            settings: Compression settings dictionary
            status_callback: Optional callback function for progress updates
            paused_check: Optional function to check if compression is paused
            running_check: Optional function to check if compression should still be running
        """
        # Build HandBrakeCLI command
        handbrake_cmd = [
            self.config["compression"]["handbrake_path"],
            "-i", file_path,
            "-o", temp_output
        ]
        
        # Add compression options
        handbrake_cmd.extend(settings["nvenc_options"].split())
        handbrake_cmd.extend(self.config["compression"]["audio_options"].split())
        handbrake_cmd.extend(self.config["compression"]["subtitle_options"].split())
        
        # Add special handling for large high-bitrate files
        if os.path.getsize(file_path) > 10 * 1024 * 1024 * 1024:  # If larger than 10GB
            logger.info(f"Large file detected: {file_path}, adding optimized processing options")
            # These options improve handling of large Blu-ray files
            handbrake_cmd.extend(["--no-two-pass", "--no-fast-decode"])
        
        logger.info(f"Starting compression of {file_path} (content type: {settings['content_type']})")
        logger.debug(f"HandBrake command: {' '.join(handbrake_cmd)}")
        
        # Update job status if callback provided
        if status_callback:
            status_callback("compressing", stage="encoding")
        
        try:
            # Run HandBrakeCLI with progress monitoring
            process = subprocess.Popen(
                handbrake_cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,  # Line buffered
                universal_newlines=True
            )
            
            # Monitor progress in real-time
            for line in iter(process.stdout.readline, ''):
                # Check if process was paused or stopped
                if (paused_check and paused_check()) or (running_check and not running_check()):
                    process.terminate()
                    return False
                
                # Extract progress percentage from HandBrake output
                if "Encoding" in line and "%" in line:
                    try:
                        # Extract percentage
                        match = re.search(r'(\d+\.\d+) %', line)
                        if match and status_callback:
                            progress = float(match.group(1))
                            status_callback("compressing", progress)
                            
                            # Extract ETA if available
                            eta_match = re.search(r'ETA\s+(\d+)h(\d+)m(\d+)s', line)
                            if eta_match:
                                h, m, s = map(int, eta_match.groups())
                                eta_seconds = h * 3600 + m * 60 + s
                                # Pass eta directly through status_callback
                                status_callback("compressing", progress, eta=eta_seconds)
                    except Exception as e:
                        logger.debug(f"Error parsing HandBrake progress: {e}")
            
            # Wait for process to complete
            process.wait()
            
            # Check if process succeeded
            if process.returncode != 0:
                raise subprocess.CalledProcessError(process.returncode, handbrake_cmd)
            
            return True
        
        except subprocess.CalledProcessError as e:
            logger.error(f"HandBrake error: {str(e)}")
            return False
        
        except Exception as e:
            logger.error(f"Error during compression: {str(e)}")
            return False
    
    def finalize_compression(self, file_path: str, temp_output: str, original_size: int, 
                           verify_integrity=None, status_callback=None) -> Dict[str, Any]:
        """
        Finalize compression by validating quality and replacing the original file.
        
        Args:
            file_path: Path to the original file
            temp_output: Path to the temporary compressed file
            original_size: Size of the original file in bytes
            verify_integrity: Optional function to verify file integrity
            status_callback: Optional callback function for status updates
        """
        # Update job status if callback provided
        if status_callback:
            status_callback("validating quality", stage="quality check")
        
        # Verify the compressed file exists and is valid
        if not os.path.exists(temp_output) or os.path.getsize(temp_output) == 0:
            logger.error("Compression produced an empty or missing file")
            try:
                if os.path.exists(temp_output):
                    os.remove(temp_output)
            except OSError:
                pass
            return {
                "status": "error",
                "error": "Compression produced an empty or missing file",
                "original_size": original_size
            }
        
        # Get compressed file size
        compressed_size = os.path.getsize(temp_output)
        
        # Calculate size reduction
        size_reduction = 1 - (compressed_size / original_size)
        
        # Validate compression quality if enabled and quality validator is available
        quality_result = {"score": 100, "acceptable": True}
        if self.config["quality_validation"]["enabled"] and self.quality_validator:
            quality_result = self.quality_validator.validate_compression(file_path, temp_output)
            logger.info(f"Quality validation for {file_path}: score={quality_result['score']:.2f}, acceptable={quality_result['acceptable']}")
        
        # Check if compression achieved the minimum expected reduction and quality
        if size_reduction < self.config["size_reduction_threshold"] or not quality_result["acceptable"]:
            reason = []
            if size_reduction < self.config["size_reduction_threshold"]:
                reason.append(f"insufficient reduction (got {size_reduction:.2%}, expected {self.config['size_reduction_threshold']:.2%})")
            
            if not quality_result["acceptable"]:
                reason.append(f"quality below threshold (got {quality_result['score']:.2f}, required {self.config['quality_validation']['threshold']})")
            
            logger.warning(f"Compression of {file_path} did not meet criteria: {', '.join(reason)}. Keeping original file.")
            
            # Update job status if callback provided
            if status_callback:
                status_callback("cleaning up", stage="skipping file")
            
            # Clean up
            try:
                os.remove(temp_output)
            except OSError:
                pass
            
            return {
                "status": "skipped",
                "reason": ", ".join(reason),
                "original_size": original_size,
                "compressed_size": compressed_size,
                "reduction": size_reduction,
                "quality_score": quality_result.get("score", 0)
            }
        
        # Update job status if callback provided
        if status_callback:
            status_callback("replacing original", 100, "finalizing")
        
        # Verify integrity of compressed file before replacing
        if self.config["recovery"]["verify_files"] and verify_integrity and not verify_integrity(temp_output):
            logger.error(f"Compressed file integrity check failed for {temp_output}")
            try:
                os.remove(temp_output)
            except OSError:
                pass
            return {
                "status": "error",
                "error": "Compressed file integrity verification failed",
                "original_size": original_size
            }
        
        # Replace the original file with the compressed one
        try:
            shutil.move(temp_output, file_path)
        except Exception as e:
            logger.error(f"Error replacing original file: {str(e)}")
            return {
                "status": "error",
                "error": f"Failed to replace original file: {str(e)}",
                "original_size": original_size
            }
        
        return {
            "status": "success",
            "original_size": original_size,
            "compressed_size": compressed_size,
            "reduction": size_reduction,
            "quality_score": quality_result.get("score", 100)
        }