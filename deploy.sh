#!/bin/bash
set -e

echo "Starting Deployment Script..."

PROJECT_DIR=$(pwd)
cd $PROJECT_DIR

echo "Pulling latest code from git..."
git pull origin main

if [ ! -f .env ]; then
    echo "WARNING: .env file not found. Copying from .env.example..."
    cp .env.example .env
fi

echo "Deploying Docker containers in detached mode..."
docker compose up -d --build

echo "Deployment complete! Containers should now be running and auto-restarting."
