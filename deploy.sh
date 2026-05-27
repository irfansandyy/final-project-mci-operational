#!/bin/bash
set -e

echo "Starting Zero-Downtime Deployment Script..."

# Navigate to project directory
# Assume script is run from the project root
PROJECT_DIR=$(pwd)
cd $PROJECT_DIR

# Pull latest changes
echo "Pulling latest code from git..."
git pull origin main

# Verify .env exists
if [ ! -f .env ]; then
    echo "WARNING: .env file not found. Copying from .env.example..."
    cp .env.example .env
fi

# Build and restart containers gracefully
echo "Deploying Docker containers in detached mode..."
docker compose up -d --build

echo "Deployment complete! Containers should now be running and auto-restarting."
