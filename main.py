#!/usr/bin/env python3
"""
Enhanced Media Server Compression Orchestrator - Main Entry Point
"""
import argparse
import sys
import logging

# Configure logging before importing other modules
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('media_compressor.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('MediaCompressor')

from manager import MediaCompressionManager

def main():
    """Main entry point for the media compressor script."""
    parser = argparse.ArgumentParser(description="Enhanced Media Server Compression Orchestrator")
    parser.add_argument("--config", "-c", help="Path to configuration file (JSON)")
    parser.add_argument("--scan-only", "-s", action="store_true", help="Only scan for files, don't compress")
    parser.add_argument("--compress-only", "-p", action="store_true", help="Only compress pending files, don't scan")
    parser.add_argument("--now", "-n", action="store_true", help="Run now regardless of schedule")
    parser.add_argument("--daemon", "-d", action="store_true", help="Run as a daemon, checking schedule periodically")
    parser.add_argument("--limit", "-l", type=int, help="Limit number of files to process")
    parser.add_argument("--reload-config", "-r", action="store_true", help="Reload configuration")
    parser.add_argument("--check-deps", action="store_true", help="Check dependencies and exit")
    args = parser.parse_args()
    
    # Initialize the manager
    manager = MediaCompressionManager(args.config)
    
    # Just check dependencies if requested
    if args.check_deps:
        if manager.compressor.check_dependencies():
            logger.info("All dependencies are available")
            sys.exit(0)
        else:
            logger.error("Some dependencies are missing")
            sys.exit(1)
    
    # Reload configuration if requested
    if args.reload_config:
        result = manager.reload_config()
        if result["success"]:
            logger.info("Configuration reloaded successfully")
            sys.exit(0)
        else:
            logger.error(f"Failed to reload configuration: {result['message']}")
            sys.exit(1)
    
    if args.daemon:
        manager.run_daemon()
    elif args.scan_only:
        manager.run_scan()
    elif args.compress_only:
        manager.run_compression(args.limit, args.now)
    else:
        # Run both scan and compression
        logger.info("Running scan followed by compression")
        manager.run_scan()
        manager.run_compression(args.limit, args.now)


def signal_handler(signum, frame):
    """Handle keyboard interrupts and termination signals."""
    logger.info(f"Received signal {signum}, initiating immediate shutdown")
    sys.exit(1)

if __name__ == "__main__":
    # Register signal handlers directly in main
    import signal
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrupted by user, exiting immediately")
        sys.exit(1)