#!/bin/bash

# Exit on error
set -e

echo "Setting up Managed Media Compressor environment..."

# Check for Homebrew and install if not present
if ! command -v brew &> /dev/null; then
    echo "Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
else
    echo "Homebrew already installed. Updating..."
    brew update
fi

# Install system dependencies
echo "Installing system dependencies..."
brew install ffmpeg imagemagick

# Install HandBrakeCLI
if ! command -v HandBrakeCLI &> /dev/null; then
    echo "Installing HandBrakeCLI..."
    brew install handbrake
else
    echo "HandBrakeCLI already installed."
fi

# Create and activate Python virtual environment
echo "Setting up Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "-------------------------------------"
echo "Setup completed successfully!"
echo "To activate the environment, run:"
echo "source venv/bin/activate"
echo "-------------------------------------"