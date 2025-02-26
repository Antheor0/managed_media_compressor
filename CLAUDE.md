# Managed Media Compressor - Development Guide

## Commands
- Run application: `python main.py --config /path/to/config.json`
- Run quality validation only: `python quality_validator.py /path/to/original.mp4 /path/to/compressed.mp4`
- Manual compression: `python media_compressor.py /path/to/file.mp4 /path/to/output.mp4`
- Debug mode: `python main.py --config /path/to/config.json --debug`

## Code Style Guidelines
- **Imports**: Standard library first, then project imports
- **Formatting**: 4-space indentation, max line length 120 characters
- **Type Hints**: Use typing module (Dict, List, Optional) for all functions
- **Naming**: PascalCase for classes, snake_case for functions/variables, UPPER_SNAKE_CASE for constants
- **Docstrings**: Triple double-quotes with descriptive summaries
- **Error Handling**: Use specific exceptions, log detailed errors, provide graceful fallbacks

## Dependencies
- FFmpeg/FFprobe for media analysis
- HandBrakeCLI for video compression
- ImageMagick (optional) for image manipulation
- Python 3.8+ with standard library modules