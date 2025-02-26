"""
Constants and default configuration for the media compression system
"""

# File status constants
STATUS_NEW = "new"
STATUS_PENDING = "pending"
STATUS_IN_PROGRESS = "in_progress"
STATUS_COMPLETED = "completed"
STATUS_SKIPPED = "skipped"
STATUS_ERROR = "error"
STATUS_NEEDS_REPROCESSING = "needs_reprocessing"
STATUS_VALIDATING = "validating"
STATUS_PAUSED = "paused"

# Default configuration
DEFAULT_CONFIG = {
    "media_paths": [
        "/mnt/library/media/series",
        "/mnt/library/media/movies"
    ],
    "schedule": {
        "start_hour": 2,  # 2 AM
        "end_hour": 6,    # 6 AM
        "dynamic_scheduling": True  # Adjust schedule based on system load
    },
    "compression": {
        "handbrake_path": "HandBrakeCLI",
        "nvenc_options": "--encoder nvenc_h265 --encoder-preset slow --quality 22",
        "audio_options": "--aencoder copy --all-audio",
        "subtitle_options": "--all-subtitles --subtitle scan --subtitle-burned=none",
        "content_aware": True,  # Enable content-aware compression settings
        "animation_quality": 26,  # Higher CRF (more compression) for animation
        "live_action_quality": 21  # Lower CRF (better quality) for live action
    },
    "quality_validation": {
        "enabled": True,
        "method": "vmaf",  # or "ssim"
        "threshold": 90,   # Minimum quality score
        "sample_duration": 60  # Sample duration in seconds for quality check
    },
    "database_path": "media_compression.db",
    "backup_path": "media_compression_backup.db",  # Added backup path for DB
    "extensions": [".mp4", ".mkv", ".avi", ".m4v"],
    "min_size_mb": 200,   # Only compress files larger than this size
    "size_reduction_threshold": 0.2,  # Expected size reduction (20%)
    "max_concurrent_jobs": 2,
    "max_concurrent_scans": 4,        # How many directories to scan concurrently
    "scan_batch_size": 1000,          # How many files to process in each database batch
    "compression_queue_size": 1000,   # Maximum number of files in the compression queue
    "temp_dir": "/tmp/media_compression",
    "min_free_space_mb": 1000,  # Minimum free space required
    "min_memory_mb": 500,       # Minimum memory required to start a job
    "web_interface": {
        "enabled": True,
        "port": 8080,
        "host": "localhost",
        "secure": False,        # Enable/disable authentication
        "username": "admin",    # Basic auth username
        "password": "password"  # Basic auth password
    },
    "notifications": {
        "email": {
            "enabled": False,
            "smtp_server": "smtp.gmail.com",
            "smtp_port": 587,
            "username": "",
            "password": "",
            "from_addr": "",
            "to_addr": "",
            "on_error": True,
            "on_completion": True
        },
        "webhook": {
            "enabled": False,
            "url": "",
            "on_error": True,
            "on_completion": True
        }
    },
    "recovery": {
        "db_backup_interval": 24,  # Hours between database backups
        "auto_repair": True,       # Attempt automatic database repair
        "verify_files": True       # Verify file integrity after compression
    }
}