import os
import time
import subprocess
import logging
import glob
import re
import shutil
from typing import List, Dict, Tuple, Optional, Any

logger = logging.getLogger('MediaCompressor.ContentAnalyzer')

class ContentAnalyzer:
    """Content analyzer class for detecting video content type and optimal compression settings."""
    
    def __init__(self, config: Dict[str, Any], quality_validator=None):
        """Initialize the content analyzer with configuration."""
        self.config = config
        self.quality_validator = quality_validator
    
    def get_video_info(self, file_path: str) -> Dict[str, Any]:
        """Get video information using ffprobe with robust error handling."""
        if self.quality_validator:
            return self.quality_validator._get_video_info(file_path)
        else:
            # Fallback if quality validator is not available
            try:
                cmd = ["ffprobe", "-v", "error", "-select_streams", "v:0",
                       "-show_entries", "stream=width,height,duration,bit_rate,codec_name",
                       "-of", "json", file_path]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                if result.returncode != 0:
                    logger.error(f"Failed to get video info: {result.stderr}")
                    return {}
                
                import json
                data = json.loads(result.stdout)
                stream = data.get("streams", [{}])[0]
                
                return {
                    "width": int(stream.get("width", 0)),
                    "height": int(stream.get("height", 0)),
                    "duration_s": float(stream.get("duration", 0)),
                    "bitrate": int(stream.get("bit_rate", 0)),
                    "codec": stream.get("codec_name", "")
                }
            except Exception as e:
                logger.error(f"Error getting video info: {str(e)}")
                return {}
    
    def detect_content_type(self, file_path: str) -> str:
        """
        Detect if the content is animation, live action, or mixed using multiple methods.
        Falls back to simpler methods if primary analysis fails.
        """
        frames_dir = os.path.join(self.config["temp_dir"], f"frames_{int(time.time())}")
        frames = []
        
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
            
            # Extract frames for analysis
            frames = self._extract_frames_for_analysis(file_path, frames_dir, duration)
            
            # If we couldn't extract enough frames, return default
            if len(frames) < 3:
                logger.warning(f"Could not extract enough frames for {file_path}, assuming live action")
                return "live_action"
            
            # Try content analysis methods
            content_type = self._analyze_frames(frames)
            
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
            self._cleanup_frame_files(frames, frames_dir)
    
    def _extract_frames_for_analysis(self, file_path: str, frames_dir: str, duration: float) -> List[str]:
        """Extract frames from video for content analysis."""
        frames = []
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
        
        return frames
    
    def _analyze_frames(self, frames: List[str]) -> str:
        """Analyze frames to determine content type."""
        animation_score = 0
        analysis_success = False
        
        # Method 1: Use ImageMagick if available
        if self._check_imagemagick_available():
            success, animation_score = self._analyze_with_imagemagick(frames, animation_score)
            analysis_success = success
        
        # Method 2: If ImageMagick failed, use FFmpeg for simple edge detection
        if not analysis_success:
            success, animation_score = self._analyze_with_ffmpeg_edges(frames, animation_score)
            analysis_success = success
        
        # Method 3: Final fallback - simple color count analysis using FFmpeg
        if not analysis_success:
            success, animation_score = self._analyze_with_ffmpeg_color(frames, animation_score)
            analysis_success = success
        
        # Final content type determination
        if analysis_success:
            if animation_score >= 2:
                return "animation"
            elif animation_score >= 1:
                return "mixed"
        
        return "live_action"  # Default

    def _analyze_with_imagemagick(self, frames: List[str], animation_score: int) -> Tuple[bool, int]:
        """Analyze frames using ImageMagick."""
        success = False
        score = animation_score
        
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
                                score += 1
                                
                            success = True
                except Exception as e:
                    logger.debug(f"Error analyzing frame with ImageMagick {frame}: {str(e)}")
        except Exception as e:
            logger.debug(f"ImageMagick analysis failed: {str(e)}")
        
        return success, score

    def _analyze_with_ffmpeg_edges(self, frames: List[str], animation_score: int) -> Tuple[bool, int]:
        """Analyze frames using FFmpeg edge detection."""
        success = False
        score = animation_score
        
        try:
            for frame in frames[:3]:
                # Use FFmpeg for edge detection
                edge_frame = os.path.join(os.path.dirname(frame), f"edge_{os.path.basename(frame)}")
                
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
                    if "lavfi.histogram.0.level" in hist_result.stderr:
                        # Extract edge percentage from histogram data
                        match = re.search(r'lavfi\.histogram\.0\.level=(\d+\.\d+)', hist_result.stderr)
                        if match:
                            edge_percentage = float(match.group(1))
                            if edge_percentage > 0.15:  # Animation threshold
                                score += 1
                    
                    success = True
        except Exception as e:
            logger.debug(f"FFmpeg edge analysis failed: {str(e)}")
        
        return success, score

    def _analyze_with_ffmpeg_color(self, frames: List[str], animation_score: int) -> Tuple[bool, int]:
        """Analyze frames using FFmpeg color statistics."""
        success = False
        score = animation_score
        
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
                                score += 1
                        
                        success = True
        except Exception as e:
            logger.debug(f"Color analysis failed: {str(e)}")
        
        return success, score
    
    def _cleanup_frame_files(self, frames: List[str], frames_dir: str):
        """Clean up frame files after analysis."""
        for frame in frames:
            try:
                if os.path.exists(frame):
                    os.remove(frame)
            except OSError:
                pass
        
        try:
            if os.path.exists(frames_dir):
                shutil.rmtree(frames_dir, ignore_errors=True)
        except OSError:
            pass
    
    def _check_imagemagick_available(self) -> bool:
        """Check if ImageMagick is installed and available."""
        try:
            result = subprocess.run(["identify", "--version"], 
                                   capture_output=True, 
                                   text=True, 
                                   timeout=2)
            return result.returncode == 0 and "ImageMagick" in result.stdout
        except (subprocess.SubprocessError, FileNotFoundError):
            return False
    
    def get_compression_settings(self, file_path: str) -> Dict[str, Any]:
        """Get optimal compression settings based on content type."""
        if not self.config["compression"]["content_aware"]:
            return {"quality": 22, "preset": "slow", "content_type": "live_action"}
        
        content_type = self.detect_content_type(file_path)
        logger.info(f"Using settings for content type: {content_type}")
        
        if content_type == "animation":
            return {
                "quality": self.config["compression"]["animation_quality"],
                "preset": "slower",
                "content_type": content_type
            }
        elif content_type == "mixed":
            # For mixed content, use a value between animation and live action
            mixed_quality = (self.config["compression"]["animation_quality"] + 
                           self.config["compression"]["live_action_quality"]) / 2
            return {
                "quality": mixed_quality, 
                "preset": "slow",
                "content_type": content_type
            }
        else:  # live_action
            return {
                "quality": self.config["compression"]["live_action_quality"],
                "preset": "slow",
                "content_type": content_type
            }