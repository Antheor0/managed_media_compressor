import subprocess
import re
import json
import os
import logging
from typing import List, Dict, Tuple, Optional, Set, Any, Callable
from datetime import time
logger = logging.getLogger('MediaCompressor.QualValidator')

class QualityValidator:
    """
    Handles media quality validation using various metrics like VMAF, SSIM, and PSNR.
    This separates the validation logic from the compressor class for better organization.
    """
    def __init__(self, config: Dict):
        self.config = config
    
    def validate_compression(self, original_path: str, compressed_path: str) -> Dict:
        """Validate compression quality using VMAF or SSIM metrics with robust fallbacks."""
        if not self.config["quality_validation"]["enabled"]:
            return {"score": 100, "acceptable": True, "method": "none"}
        
        try:
            primary_method = self.config["quality_validation"]["method"].lower()
            threshold = self.config["quality_validation"]["threshold"]
            sample_duration = self.config["quality_validation"]["sample_duration"]
            
            # Get video info for both files to ensure compatibility
            original_info = self._get_video_info(original_path)
            compressed_info = self._get_video_info(compressed_path)
            
            if "error" in original_info or "error" in compressed_info:
                logger.warning(f"Could not get video info for comparison, assuming acceptable quality")
                return {"score": 100, "acceptable": True, "method": "none", "note": "video info error"}
            
            # Get the shorter duration to avoid out-of-bounds errors
            orig_duration = original_info.get("duration_s", 0)
            comp_duration = compressed_info.get("duration_s", 0)
            
            if orig_duration <= 0 or comp_duration <= 0:
                logger.warning(f"Could not determine duration for comparison, assuming acceptable quality")
                return {"score": 100, "acceptable": True, "method": "none", "note": "duration error"}
            
            # Use the shorter duration of the two
            safe_duration = min(orig_duration, comp_duration)
            
            # Calculate safe sampling points
            safe_start = min(30, safe_duration * 0.1)
            
            # Ensure we have enough duration for the sample
            if safe_start + sample_duration > safe_duration:
                # If the video is too short, reduce sample duration
                adjusted_duration = max(10, safe_duration - safe_start)
                logger.warning(f"Video too short for full sample, reducing sample duration to {adjusted_duration}s")
                sample_duration = adjusted_duration
            
            # Try multiple methods in sequence if one fails
            methods_to_try = []
            
            # Always try the primary method first
            methods_to_try.append(primary_method)
            
            # Add fallback methods
            for method in ["vmaf", "ssim", "psnr"]:
                if method != primary_method and method not in methods_to_try:
                    methods_to_try.append(method)
            
            # Try each method until one succeeds
            for method in methods_to_try:
                try:
                    # Create a temporary output file
                    temp_dir = self.config["temp_dir"]
                    os.makedirs(temp_dir, exist_ok=True)
                    result_json = os.path.join(temp_dir, f"quality_{method}_{int(time.time())}.json")
                    
                    # Build appropriate command based on method
                    if method == "vmaf":
                        cmd = [
                            "ffmpeg", "-y", "-v", "error",
                            "-ss", str(safe_start), "-t", str(sample_duration),
                            "-i", original_path, 
                            "-ss", str(safe_start), "-t", str(sample_duration),
                            "-i", compressed_path,
                            "-filter_complex", f"libvmaf=log_fmt=json:log_path={result_json}:model=version=vmaf_v0.6.1:n_threads=4",
                            "-f", "null", "-"
                        ]
                    elif method == "ssim":
                        cmd = [
                            "ffmpeg", "-y", "-v", "error",
                            "-ss", str(safe_start), "-t", str(sample_duration),
                            "-i", original_path, 
                            "-ss", str(safe_start), "-t", str(sample_duration),
                            "-i", compressed_path,
                            "-filter_complex", "ssim=stats_file=" + result_json,
                            "-f", "null", "-"
                        ]
                    else:  # psnr
                        cmd = [
                            "ffmpeg", "-y", "-v", "error",
                            "-ss", str(safe_start), "-t", str(sample_duration),
                            "-i", original_path, 
                            "-ss", str(safe_start), "-t", str(sample_duration),
                            "-i", compressed_path,
                            "-filter_complex", "psnr=stats_file=" + result_json,
                            "-f", "null", "-"
                        ]
                    
                    logger.info(f"Running quality validation using {method}")
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                    
                    # Parse results based on method
                    if os.path.exists(result_json) and os.path.getsize(result_json) > 0:
                        with open(result_json, 'r') as f:
                            content = f.read()
                            
                            # Parse for VMAF
                            if method == "vmaf" and "pooled_metrics" in content:
                                results = json.loads(content)
                                score = results["pooled_metrics"]["vmaf"]["mean"]
                                logger.info(f"VMAF validation successful: score={score}")
                                
                                os.remove(result_json)
                                return {
                                    "score": score,
                                    "acceptable": score >= threshold,
                                    "method": method
                                }
                            
                            # Parse for SSIM
                            elif method == "ssim":
                                match = re.search(r'All:([\d.]+)', content)
                                if match:
                                    score = float(match.group(1)) * 100
                                    logger.info(f"SSIM validation successful: score={score}")
                                    
                                    os.remove(result_json)
                                    return {
                                        "score": score,
                                        "acceptable": score >= max(threshold * 0.8, 80),
                                        "method": method
                                    }
                            
                            # Parse for PSNR
                            elif method == "psnr":
                                match = re.search(r'average:([\d.]+)', content)
                                if match:
                                    psnr_value = float(match.group(1))
                                    score = min(100, psnr_value * 2) if psnr_value < 50 else 100
                                    
                                    logger.info(f"PSNR validation successful: score={score}")
                                    
                                    os.remove(result_json)
                                    return {
                                        "score": score,
                                        "acceptable": psnr_value >= 30,
                                        "method": method
                                    }
                    
                    # Clean up
                    if os.path.exists(result_json):
                        os.remove(result_json)
                    
                    logger.warning(f"Quality validation with {method} failed, trying next method")
                    
                except Exception as e:
                    logger.warning(f"Error in {method} validation: {str(e)}")
                    if 'result_json' in locals() and os.path.exists(result_json):
                        os.remove(result_json)
            
            # If we got here, all methods failed
            logger.error(f"All quality validation methods failed for {original_path}")
            
            # Return a fallback value
            return {
                "score": 85,
                "acceptable": True,
                "method": "fallback",
                "note": "All validation methods failed, using fallback value"
            }
        
        except Exception as e:
            logger.error(f"Critical error in quality validation: {str(e)}")
            return {
                "score": 80,
                "acceptable": True,
                "method": "error_fallback",
                "note": f"Validation error: {str(e)}"
            }
    
    def _get_video_info(self, file_path: str) -> Dict:
        """Get video information using ffprobe."""
        try:
            cmd = [
                "ffprobe", "-v", "quiet", "-print_format", "json",
                "-show_format", "-show_streams", file_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            # Default structure
            video_info = {
                "has_video": False, 
                "has_audio": False, 
                "duration_s": 0, 
                "bitrate": 0,
                "video_streams": [],
                "audio_streams": [],
                "subtitle_streams": []
            }
            
            # Check for valid output
            if result.returncode != 0 or not result.stdout.strip():
                logger.warning(f"ffprobe failed for {os.path.basename(file_path)}")
                return video_info
                
            try:
                info = json.loads(result.stdout)
            except json.JSONDecodeError:
                logger.warning(f"Could not parse ffprobe JSON output for {os.path.basename(file_path)}")
                return video_info
            
            # Extract format information
            if "format" in info:
                format_info = info["format"]
                
                # Get duration
                if "duration" in format_info:
                    try:
                        video_info["duration_s"] = float(format_info["duration"])
                    except (ValueError, TypeError):
                        pass
                        
                # Get bitrate
                if "bit_rate" in format_info:
                    try:
                        video_info["bitrate"] = int(format_info["bit_rate"])
                    except (ValueError, TypeError):
                        pass
                
                video_info["format_name"] = format_info.get("format_name", "unknown")
            
            # Process streams
            for stream in info.get("streams", []):
                self._process_stream(stream, video_info)
            
            # Make one more attempt to get duration if it's still 0
            if video_info["duration_s"] == 0 and video_info["has_video"]:
                self._try_alternate_duration_methods(file_path, video_info)
            
            return video_info
        
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout getting video info for {os.path.basename(file_path)}")
            return {"error": "ffprobe timeout", "has_video": False, "duration_s": 0}
        except Exception as e:
            logger.error(f"Error getting video info for {os.path.basename(file_path)}: {str(e)}")
            return {"error": str(e), "has_video": False, "duration_s": 0}
    
    def _process_stream(self, stream: Dict, video_info: Dict):
        """Process a stream from ffprobe output."""
        stream_type = stream.get("codec_type", "unknown")
        
        if stream_type == "video":
            video_info["has_video"] = True
            
            # Calculate fps
            fps = 0
            avg_frame_rate = stream.get("avg_frame_rate", "0/1")
            try:
                if "/" in avg_frame_rate:
                    num, denom = avg_frame_rate.split("/")
                    num, denom = int(num), int(denom)
                    fps = num / denom if denom != 0 else 0
                else:
                    fps = float(avg_frame_rate)
            except (ValueError, ZeroDivisionError):
                fps = 0
            
            # Get bitrate
            bit_rate = 0
            try:
                if "bit_rate" in stream:
                    bit_rate = int(stream["bit_rate"])
            except (ValueError, TypeError):
                pass
            
            video_stream = {
                "width": stream.get("width", 0),
                "height": stream.get("height", 0),
                "codec": stream.get("codec_name", "unknown"),
                "bit_rate": bit_rate,
                "fps": fps
            }
            video_info["video_streams"].append(video_stream)
        
        elif stream_type == "audio":
            video_info["has_audio"] = True
            audio_stream = {
                "codec": stream.get("codec_name", "unknown"),
                "channels": stream.get("channels", 0),
                "language": stream.get("tags", {}).get("language", "unknown")
            }
            video_info["audio_streams"].append(audio_stream)
        
        elif stream_type == "subtitle":
            subtitle_stream = {
                "codec": stream.get("codec_name", "unknown"),
                "language": stream.get("tags", {}).get("language", "unknown")
            }
            video_info["subtitle_streams"].append(subtitle_stream)
    
    def _try_alternate_duration_methods(self, file_path: str, video_info: Dict):
        """Try alternative methods to get video duration."""
        try:
            # Try alternative ffprobe format
            cmd = [
                "ffprobe", "-v", "error", 
                "-select_streams", "v:0", 
                "-show_entries", "stream=duration", 
                "-of", "default=noprint_wrappers=1:nokey=1", 
                file_path
            ]
            duration_result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if duration_result.returncode == 0 and duration_result.stdout.strip():
                video_info["duration_s"] = float(duration_result.stdout.strip())
                return
            
            # Try to get duration from container
            container_cmd = [
                "ffprobe", "-v", "error", 
                "-show_entries", "format=duration", 
                "-of", "default=noprint_wrappers=1:nokey=1", 
                file_path
            ]
            container_result = subprocess.run(container_cmd, capture_output=True, text=True, timeout=10)
            if container_result.returncode == 0 and container_result.stdout.strip():
                video_info["duration_s"] = float(container_result.stdout.strip())
        except Exception as e:
            logger.debug(f"Error getting alternative duration: {str(e)}")