import os
import time
import subprocess
import threading
import psutil
import logging
import glob
import re
import datetime
import hashlib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
import concurrent
import smtplib
import shutil
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Tuple, Optional, Set, Any, Callable
import queue
import sqlite3
from .media_database import MediaDatabase
from .quality_validator import QualityValidator
from .constants import *

logger = logging.getLogger('MediaCompressor.Compressor')

class MediaCompressor:
    """
    Media Compressor class that takes files from the database and
    compresses them using HandBrakeCLI.
    """
    def __init__(self, config: Dict, db: MediaDatabase):
        self.config = config
        self.db = db
        self.quality_validator = QualityValidator(config)
        
        # Ensure temporary directory exists
        os.makedirs(self.config["temp_dir"], exist_ok=True)
        
        # Track current state
        self.active_jobs = {}  # Dictionary to track active jobs: {thread_id: file_path}
        self.jobs_lock = threading.RLock()  # Lock for thread-safe access to active_jobs
        self.compression_start_time = None
        self.stats = {
            "session_start": time.time(),
            "files_processed": 0,
            "total_original_size": 0,
            "total_compressed_size": 0,
            "errors": 0
        }
        
        # 6. Dynamic Job Management
        self.paused = False
        self.running = True
        self.job_queue = queue.PriorityQueue()
        self.job_history = {}  # Track job history for priority
        self.dependencies_checked = False
    
    # 7. Dependency Checking
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
            self.db.log_system_event("dependency_check_failed", error_msg, "error")
            return False
        
        self.dependencies_checked = True
        return True
    
    # 2. Resource Management
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
            self.db.log_system_event("resource_warning", f"Low memory: {available_mb:.2f}MB available", "warning")
            return False
        
        # Check CPU load
        cpu_percent = psutil.cpu_percent(interval=1)
        if cpu_percent > 90:  # Allow high CPU usage but warn
            logger.warning(f"High CPU usage: {cpu_percent}%")
            self.db.log_system_event("resource_warning", f"High CPU usage: {cpu_percent}%", "warning")
        
        return True
    
    # 6. Dynamic Job Management
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
            conn = sqlite3.connect(self.config["database_path"])
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE processed_files SET status = ? WHERE status = ?",
                (STATUS_PENDING, STATUS_PAUSED)
            )
            conn.commit()
            conn.close()
            
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
            except:
                pass
                
            # Get estimated time
            estimated_time = 0
            try:
                conn = sqlite3.connect(self.config["database_path"])
                cursor = conn.cursor()
                cursor.execute("SELECT estimated_time FROM processed_files WHERE file_path = ?", (file_path,))
                result = cursor.fetchone()
                if result and result[0]:
                    estimated_time = result[0]
                conn.close()
            except:
                pass
            
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
            
    def _update_job_status(self, status: str, progress: float = None, stage: str = None):
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
                
                # Calculate ETA if we have progress
                if progress is not None and progress > 0:
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
    
    def get_video_info(self, file_path: str) -> Dict:
        """Get video information using ffprobe with robust error handling."""
        return self.quality_validator._get_video_info(file_path)
    
    def detect_content_type(self, file_path: str) -> str:
        """
        Detect if the content is animation, live action, or mixed using multiple methods.
        Falls back to simpler methods if primary analysis fails.
        """
        frames_dir = os.path.join(self.config["temp_dir"], f"frames_{int(time.time())}")
        frames = []
        
        # Update job status
        self._update_job_status("detecting content", stage="content analysis")
        
        try:
            os.makedirs(frames_dir, exist_ok=True)
            
            # Get video info and check if we can determine basic properties
            video_info = self.get_video_info(file_path)
            duration = video_info.get("duration_s", 0)
            
            if duration <= 0:
                logger.warning(f"Could not determine duration for {file_path}, assuming live action")
                return "live_action"
            
            # Check for common animation indicators in the filename
            filename = os.path.basename(file_path).lower()
            animation_keywords = ['animation', 'animated', 'anime', 'cartoon', 'pixar', 'disney']
            
            if any(keyword in filename for keyword in animation_keywords):
                logger.info(f"Detected likely animation based on filename: {filename}")
                return "animation"
            
            # Extract a few frames for analysis - try two methods
            frames_extracted = False
            
            # Method 1: Try FFmpeg's scene detection to get keyframes
            try:
                # Get 5-10 scene change frames if possible
                scene_cmd = [
                    "ffmpeg", "-i", file_path, 
                    "-vf", "select='gt(scene,0.3)',showinfo", 
                    "-vsync", "vfr", 
                    "-frame_pts", "1",
                    "-frames:v", "10", 
                    "-y",
                    os.path.join(frames_dir, "scene_%03d.jpg")
                ]
                
                subprocess.run(scene_cmd, capture_output=True, timeout=60)
                
                # Check if frames were created
                scene_frames = glob.glob(os.path.join(frames_dir, "scene_*.jpg"))
                if len(scene_frames) >= 3:
                    frames = scene_frames[:5]  # Use up to 5 frames
                    frames_extracted = True
                    logger.debug(f"Extracted {len(frames)} scene frames")
            except Exception as e:
                logger.debug(f"Scene detection failed: {str(e)}, trying regular interval sampling")
            
            # Method 2: If scene detection failed, use regular interval sampling
            if not frames_extracted:
                # Sample frames at regular intervals
                interval = duration / 6  # 5 frames + buffer
                for i in range(1, 6):
                    time_pos = interval * i
                    frame_path = os.path.join(frames_dir, f"frame_{i}.jpg")
                    
                    try:
                        extract_cmd = [
                            "ffmpeg", "-ss", str(time_pos), "-i", file_path,
                            "-vframes", "1", "-q:v", "2", frame_path, "-y"
                        ]
                        subprocess.run(extract_cmd, capture_output=True, timeout=10, check=True)
                        if os.path.exists(frame_path) and os.path.getsize(frame_path) > 0:
                            frames.append(frame_path)
                    except Exception as e:
                        logger.debug(f"Error extracting frame at {time_pos}s: {str(e)}")
            
            # If we couldn't extract enough frames, return default
            if len(frames) < 3:
                logger.warning(f"Could not extract enough frames for {file_path}, assuming live action")
                return "live_action"
            
            # Try multiple content analysis methods
            animation_score = 0
            analysis_success = False
            
            # Method 1: Use ImageMagick if available
            imagemagick_available = self._check_imagemagick_available()
            
            if imagemagick_available:
                try:
                    for frame in frames[:3]:  # Analyze first 3 frames
                        # Use ImageMagick to analyze colors and edges
                        try:
                            # Count unique colors
                            color_cmd = ["identify", "-format", "%k", frame]
                            color_result = subprocess.run(color_cmd, capture_output=True, text=True, timeout=5)
                            
                            if color_result.returncode == 0 and color_result.stdout.strip():
                                unique_colors = int(color_result.stdout.strip())
                                
                                # Get edge score (higher for animation)
                                edge_cmd = [
                                    "convert", frame, "-edge", "1", "-format", "%[mean]", "info:"
                                ]
                                edge_result = subprocess.run(edge_cmd, capture_output=True, text=True, timeout=5)
                                
                                if edge_result.returncode == 0 and edge_result.stdout.strip():
                                    edge_value = float(edge_result.stdout.strip())
                                    
                                    # Animation typically has fewer colors and more defined edges
                                    if unique_colors < 10000 and edge_value > 0.05:
                                        animation_score += 1
                                        
                                    analysis_success = True
                        except Exception as e:
                            logger.debug(f"Error analyzing frame with ImageMagick {frame}: {str(e)}")
                except Exception as e:
                    logger.debug(f"ImageMagick analysis failed: {str(e)}")
            
            # Method 2: If ImageMagick failed, use FFmpeg for simple edge detection
            if not analysis_success:
                try:
                    for frame in frames[:3]:
                        # Use FFmpeg for edge detection
                        edge_frame = os.path.join(frames_dir, f"edge_{os.path.basename(frame)}")
                        
                        # Create edge-detected version of the frame
                        edge_cmd = [
                            "ffmpeg", "-i", frame, 
                            "-filter_complex", "edgedetect=low=0.1:high=0.4", 
                            "-y", edge_frame
                        ]
                        
                        subprocess.run(edge_cmd, capture_output=True, timeout=10)
                        
                        if os.path.exists(edge_frame):
                            # Check percentage of edge pixels
                            histogram_cmd = [
                                "ffmpeg", "-i", edge_frame,
                                "-filter_complex", "histogram,metadata=print:file=-",
                                "-f", "null", "-"
                            ]
                            
                            hist_result = subprocess.run(histogram_cmd, capture_output=True, text=True, timeout=10)
                            
                            # Check if edge percentage is high (typical for animation)
                            edge_percentage = 0
                            if "lavfi.histogram.0.level" in hist_result.stderr:
                                # Extract edge percentage from histogram data
                                match = re.search(r'lavfi\.histogram\.0\.level=(\d+\.\d+)', hist_result.stderr)
                                if match:
                                    edge_percentage = float(match.group(1))
                                    if edge_percentage > 0.15:  # Animation threshold
                                        animation_score += 1
                            
                            analysis_success = True
                except Exception as e:
                    logger.debug(f"FFmpeg edge analysis failed: {str(e)}")
            
            # Method 3: Final fallback - simple color count analysis using FFmpeg
            if not analysis_success:
                try:
                    for frame in frames[:3]:
                        # Get frame dimensions
                        probe_cmd = [
                            "ffprobe", "-v", "error", 
                            "-select_streams", "v:0", 
                            "-show_entries", "stream=width,height", 
                            "-of", "csv=p=0", frame
                        ]
                        
                        dim_result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=5)
                        if dim_result.returncode == 0 and dim_result.stdout.strip():
                            dimensions = dim_result.stdout.strip().split(',')
                            if len(dimensions) == 2:
                                width, height = int(dimensions[0]), int(dimensions[1])
                                total_pixels = width * height
                                
                                # Simplified analysis - check for large areas of solid color
                                # Sample random points in the image
                                color_cmd = [
                                    "ffmpeg", "-i", frame,
                                    "-filter_complex", "signalstats=stat=tout:c=r+g+b",
                                    "-f", "null", "-"
                                ]
                                
                                stats_result = subprocess.run(color_cmd, capture_output=True, text=True, timeout=10)
                                
                                # Animations often have higher contrast/saturation
                                if "Parsed_signalstats" in stats_result.stderr:
                                    if "excessive max values" in stats_result.stderr or "low PSNR values" in stats_result.stderr:
                                        animation_score += 1
                                
                                analysis_success = True
                except Exception as e:
                    logger.debug(f"Color analysis failed: {str(e)}")
            
            # Final content type determination
            content_type = "live_action"  # Default
            
            # If we got a score, determine the type
            if analysis_success:
                if animation_score >= 2:
                    content_type = "animation"
                elif animation_score >= 1:
                    content_type = "mixed"
            
            # Additional filename-based heuristics as fallback
            if content_type == "live_action":
                # Check for common animation file patterns
                if re.search(r'(anime|cartoon|animation)', filename, re.IGNORECASE):
                    content_type = "animation"
                # Look for common anime/animation release patterns
                elif re.search(r'\[\s*\d{3,4}p\s*\].*\[(BD|BluRay|Web-DL)', filename, re.IGNORECASE):
                    if "FLAC" in filename or "VORBIS" in filename:  # Common in anime releases
                        content_type = "animation"
            
            logger.info(f"Detected content type for {os.path.basename(file_path)}: {content_type}")
            return content_type
            
        except Exception as e:
            logger.error(f"Error detecting content type for {file_path}: {str(e)}")
            return "live_action"  # Default to live action on error
        
        finally:
            # Clean up frames no matter what happened
            for frame in frames:
                try:
                    if os.path.exists(frame):
                        os.remove(frame)
                except:
                    pass
            
            try:
                if os.path.exists(frames_dir):
                    shutil.rmtree(frames_dir, ignore_errors=True)
            except:
                pass
    
    def _check_imagemagick_available(self) -> bool:
        """Check if ImageMagick is installed and available."""
        try:
            result = subprocess.run(["identify", "--version"], 
                                    capture_output=True, 
                                    text=True, 
                                    timeout=2)
            return result.returncode == 0 and "ImageMagick" in result.stdout
        except:
            return False
    
    def get_compression_settings(self, file_path: str) -> Dict:
        """Get optimal compression settings based on content type."""
        if not self.config["compression"]["content_aware"]:
            return {"quality": 22, "preset": "slow"}
        
        content_type = self.detect_content_type(file_path)
        logger.info(f"Using settings for content type: {content_type}")
        
        if content_type == "animation":
            return {
                "quality": self.config["compression"]["animation_quality"],
                "preset": "slower"
            }
        elif content_type == "mixed":
            # For mixed content, use a value between animation and live action
            mixed_quality = (self.config["compression"]["animation_quality"] + 
                           self.config["compression"]["live_action_quality"]) / 2
            return {"quality": mixed_quality, "preset": "slow"}
        else:  # live_action
            return {
                "quality": self.config["compression"]["live_action_quality"],
                "preset": "slow"
            }
    
    def check_disk_space(self, path: str, required_mb: int = None) -> bool:
        """Ensure sufficient disk space exists before compressing."""
        if required_mb is None:
            required_mb = self.config["min_free_space_mb"]
        
        try:
            disk_stats = shutil.disk_usage(path)
            free_space_mb = disk_stats.free / (1024 * 1024)
            
            if free_space_mb < required_mb:
                logger.error(f"Insufficient disk space on {path}: {free_space_mb:.2f}MB free, {required_mb}MB required")
                self.send_notification(
                    f"Insufficient disk space on {path}: {free_space_mb:.2f}MB free, {required_mb}MB required",
                    level="error"
                )
                self.db.log_system_event(
                    "disk_space_error",
                    f"Insufficient disk space on {path}: {free_space_mb:.2f}MB free, {required_mb}MB required",
                    "error"
                )
                return False
            
            return True
        except Exception as e:
            logger.error(f"Error checking disk space on {path}: {str(e)}")
            return False
    
    # 3. Error Recovery - File validation
    def verify_file_integrity(self, file_path: str) -> bool:
        """Verify that a file is valid and not corrupted - less aggressive version."""
        self._update_job_status("verifying file", stage="integrity check")
        
        try:
            # Quick size check
            if os.path.getsize(file_path) == 0:
                logger.error(f"File {file_path} has zero size")
                return False
            
            # Try to read the file headers with ffprobe
            cmd = ["ffprobe", "-v", "error", "-show_format", file_path]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)  # Increased timeout
            
            if result.returncode != 0:
                logger.error(f"File format check failed for {file_path}: {result.stderr}")
                return False
            
            # For video files, check for stream existence but don't try to decode or count frames
            # This is a much more lenient check that will work with high-bitrate files
            cmd = ["ffprobe", "-v", "error", "-select_streams", "v:0", 
                "-show_entries", "stream=codec_type", 
                "-of", "default=noprint_wrappers=1:nokey=1", file_path]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                logger.error(f"Video stream check failed for {file_path}: {result.stderr}")
                return False
            
            # Just check if we got "video" in the output which confirms a valid video stream exists
            if "video" not in result.stdout.strip().lower():
                logger.error(f"No valid video stream found in {file_path}")
                return False
            
            # If we got here, file is valid enough to proceed
            logger.info(f"File integrity verified for {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error verifying file integrity for {file_path}: {str(e)}")
            return False                    
    
    # 5. Code Refactoring - Breaking down the compress_file method
    def prepare_compression(self, file_path: str) -> Tuple[str, Dict]:
        """Prepare for compression by setting up output paths and settings."""
        # Generate temporary output path
        file_name = os.path.basename(file_path)
        base_name, ext = os.path.splitext(file_name)
        temp_output = os.path.join(self.config["temp_dir"], f"{base_name}_compressed{ext}")
        
        # Detect content type for optimal settings
        content_type = "live_action"
        compression_settings = {}
        
        if self.config["compression"]["content_aware"]:
            content_type = self.detect_content_type(file_path)
            compression_settings = self.get_compression_settings(file_path)
            
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
        else:
            nvenc_options = self.config["compression"]["nvenc_options"]
        
        return temp_output, {
            "nvenc_options": nvenc_options,
            "content_type": content_type
        }
    
    def run_handbrake(self, file_path: str, temp_output: str, settings: Dict) -> bool:
        """Run HandBrakeCLI command with progress monitoring."""
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
        
        # Update job status
        self._update_job_status("compressing", stage="encoding")
        
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
                if self.paused or not self.running:
                    process.terminate()
                    return False
                
                # Extract progress percentage from HandBrake output
                if "Encoding" in line and "%" in line:
                    try:
                        # Extract percentage
                        match = re.search(r'(\d+\.\d+) %', line)
                        if match:
                            progress = float(match.group(1))
                            self._update_job_status("compressing", progress)
                            
                            # Extract ETA if available
                            eta_match = re.search(r'ETA\s+(\d+)h(\d+)m(\d+)s', line)
                            if eta_match:
                                h, m, s = map(int, eta_match.groups())
                                eta_seconds = h * 3600 + m * 60 + s
                                # Update job ETA directly
                                with self.jobs_lock:
                                    thread_id = threading.get_ident()
                                    if thread_id in self.active_jobs:
                                        self.active_jobs[thread_id]["eta"] = eta_seconds
                    except Exception:
                        pass
            
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
    
    def finalize_compression(self, file_path: str, temp_output: str, original_size: int) -> Dict:
        """Finalize compression by validating quality and replacing the original file."""
        # Update job status
        self._update_job_status("validating quality", stage="quality check")
        
        # Verify the compressed file exists and is valid
        if not os.path.exists(temp_output) or os.path.getsize(temp_output) == 0:
            logger.error("Compression produced an empty or missing file")
            try:
                if os.path.exists(temp_output):
                    os.remove(temp_output)
            except:
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
        
        # Validate compression quality if enabled
        quality_result = {"score": 100, "acceptable": True}
        if self.config["quality_validation"]["enabled"]:
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
            
            # Update job status
            self._update_job_status("cleaning up", stage="skipping file")
            
            # Clean up
            try:
                os.remove(temp_output)
            except:
                pass
            
            return {
                "status": "skipped",
                "reason": ", ".join(reason),
                "original_size": original_size,
                "compressed_size": compressed_size,
                "reduction": size_reduction,
                "quality_score": quality_result.get("score", 0)
            }
        
        # Update job status
        self._update_job_status("replacing original", 100, "finalizing")
        
        # Verify integrity of compressed file before replacing
        if self.config["recovery"]["verify_files"] and not self.verify_file_integrity(temp_output):
            logger.error(f"Compressed file integrity check failed for {temp_output}")
            try:
                os.remove(temp_output)
            except:
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
        
        # Get new checksum
        checksum = self._get_file_checksum(file_path)
        
        return {
            "status": "success",
            "original_size": original_size,
            "compressed_size": compressed_size,
            "reduction": size_reduction,
            "quality_score": quality_result.get("score", 100),
            "checksum": checksum
        }
    
    def compress_file(self, file_path: str) -> Dict:
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
            if self.config["recovery"]["verify_files"] and not self.verify_file_integrity(file_path):
                error_msg = f"Original file integrity check failed for {file_path}"
                logger.error(error_msg)
                
                self.db.update_file_status(
                    file_path,
                    STATUS_ERROR,
                    error_message=error_msg
                )
                
                self._unregister_job()
                return {"status": "error", "error": error_msg, "original_size": original_size}
            
            # Prepare for compression
            temp_output, settings = self.prepare_compression(file_path)
            
            # Run HandBrake
            if not self.run_handbrake(file_path, temp_output, settings):
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
            
            # Finalize compression
            result = self.finalize_compression(file_path, temp_output, original_size)
            
            # Calculate time taken
            duration = time.time() - start_time
            
            if result["status"] == "success":
                # Record in database
                self.db.update_file_status(
                    file_path,
                    STATUS_COMPLETED,
                    original_size=original_size,
                    compressed_size=result["compressed_size"],
                    compression_date=datetime.datetime.now().isoformat(),
                    checksum=result["checksum"],
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
            self.send_notification(
                f"Unexpected error compressing {file_path}: {error_msg}",
                level="error"
            )
            
            # Clean up temp file if it exists
            if 'temp_output' in locals() and os.path.exists(temp_output):
                try:
                    os.remove(temp_output)
                except:
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
    
    def _get_file_checksum(self, file_path: str) -> str:
        """Calculate file checksum using first and last 4MB for speed."""
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
    
    def send_notification(self, message: str, level: str = "info"):
        """Send notifications through various channels."""
        # Email notifications
        if (self.config["notifications"]["email"]["enabled"] and 
            ((level == "error" and self.config["notifications"]["email"]["on_error"]) or 
             (level == "info" and self.config["notifications"]["email"]["on_completion"]))):
            self._send_email(
                subject=f"Media Compressor {level.capitalize()}", 
                body=message
            )
        
        # Webhook notifications
        if (self.config["notifications"]["webhook"]["enabled"] and 
            ((level == "error" and self.config["notifications"]["webhook"]["on_error"]) or 
             (level == "info" and self.config["notifications"]["webhook"]["on_completion"]))):
            self._send_webhook({
                "level": level,
                "message": message,
                "timestamp": datetime.datetime.now().isoformat()
            })
        
        # Log to system events
        self.db.log_system_event(
            f"notification_{level}",
            message,
            level
        )
    
    def _send_email(self, subject: str, body: str):
        """Send an email notification."""
        try:
            config = self.config["notifications"]["email"]
            
            msg = MIMEMultipart()
            msg['From'] = config["from_addr"]
            msg['To'] = config["to_addr"]
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(config["smtp_server"], config["smtp_port"])
            server.starttls()
            server.login(config["username"], config["password"])
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Email notification sent: {subject}")
        except Exception as e:
            logger.error(f"Error sending email notification: {str(e)}")
    
    def _send_webhook(self, data: Dict):
        """Send a webhook notification."""
        try:
            webhook_url = self.config["notifications"]["webhook"]["url"]
            
            # Add additional system info
            data["system_info"] = {
                "hostname": os.uname().nodename,
                "free_space_gb": shutil.disk_usage(self.config["temp_dir"]).free / (1024**3)
            }
            
            response = requests.post(
                webhook_url,
                json=data,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code < 200 or response.status_code >= 300:
                logger.warning(f"Webhook response error: {response.status_code} - {response.text}")
            else:
                logger.info(f"Webhook notification sent successfully")
        
        except Exception as e:
            logger.error(f"Error sending webhook notification: {str(e)}")
    
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
    
    def check_system_load(self) -> bool:
        """Check if system load is low enough to run compression tasks."""
        # Get CPU usage
        cpu_usage = psutil.cpu_percent(interval=1)
        
        # Get memory usage
        memory_usage = psutil.virtual_memory().percent
        
        # Get GPU usage (simplified, in production you'd use pynvml or similar)
        try:
            gpu_info = subprocess.run(
                ["nvidia-smi", "--query-gpu=utilization.gpu", "--format=csv,noheader,nounits"],
                capture_output=True, text=True, check=True
            )
            gpu_usage = float(gpu_info.stdout.strip())
        except Exception:
            gpu_usage = 0  # If can't get GPU usage, assume it's available
        
        logger.debug(f"System load: CPU {cpu_usage}%, Memory {memory_usage}%, GPU {gpu_usage}%")
        
        # Check if system is under heavy load
        if cpu_usage > 80 or memory_usage > 90 or gpu_usage > 80:
            logger.info(f"System under heavy load (CPU: {cpu_usage}%, Memory: {memory_usage}%, GPU: {gpu_usage}%), pausing")
            return False
        
        return True
    
    # 9. Better Progress Tracking
    def get_estimated_completion_time(self) -> Dict:
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
        if total_eta <= 0:
            eta_formatted = "< 1 minute"
        elif total_eta < 60:
            eta_formatted = f"{int(total_eta)} seconds"
        elif total_eta < 3600:
            minutes = int(total_eta // 60)
            eta_formatted = f"{minutes} minute{'s' if minutes > 1 else ''}"
        elif total_eta < 86400:
            hours = int(total_eta // 3600)
            minutes = int((total_eta % 3600) // 60)
            eta_formatted = f"{hours} hour{'s' if hours > 1 else ''}, {minutes} minute{'s' if minutes > 1 else ''}"
        else:
            days = int(total_eta // 86400)
            hours = int((total_eta % 86400) // 3600)
            eta_formatted = f"{days} day{'s' if days > 1 else ''}, {hours} hour{'s' if hours > 1 else ''}"
        
        pending_files = stats["status_counts"].get(STATUS_PENDING, 0)
        
        return {
            "eta_seconds": total_eta,
            "eta_formatted": eta_formatted,
            "total_files": pending_files,
            "average_time_per_file": avg_time
        }
    
    def process_compression_queue(self, limit: int = None, force_now: bool = False):
        """Process files in the compression queue."""
        if not self.check_dependencies():
            logger.error("Dependency check failed, cannot start compression")
            return {"status": "error", "message": "Dependency check failed"}
        
        if not force_now and not self.is_within_schedule():
            logger.info("Current time is outside the scheduled window or system load is too high. Exiting.")
            return {"status": "skipped", "reason": "Outside schedule window"}
        
        start_time = time.time()
        self.compression_start_time = start_time
        self.stats["session_start"] = start_time
        self.paused = False
        self.running = True
        
        # Check system resources
        if not self.check_system_resources():
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
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config["max_concurrent_jobs"]) as executor:
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
        
        # Record session statistics
        if files_processed > 0 or errors > 0:
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
                conn = sqlite3.connect(self.config["database_path"])
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
                conn.close()
            except sqlite3.Error as e:
                logger.error(f"Database error recording stats: {str(e)}")
            
            # Send completion notification
            if self.config["notifications"]["email"]["on_completion"] or self.config["notifications"]["webhook"]["on_completion"]:
                self.send_notification(
                    f"Compression session completed. "
                    f"Files processed: {files_processed}, "
                    f"Errors: {errors}, "
                    f"Space saved: {(total_original_size-total_compressed_size)/1024/1024/1024:.2f}GB ({savings_percentage:.2f}%)",
                    level="info"
                )
            
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
        else:
            logger.info("No files were successfully compressed.")
            return {"status": "completed", "files_processed": 0, "errors": errors}
    
    def get_compression_status(self):
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
    
    def _format_time(self, seconds):
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