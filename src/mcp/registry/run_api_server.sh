#!/bin/bash
# Script to run the Metadata Registry API Server

# Install required packages
pip install flask pyyaml

# Set current directory to project root
cd "$(dirname "$0")/../../.."

# Create required directories
mkdir -p src/mcp/registry/data/services

# Run the API server
echo "Starting Metadata Registry API Server..."
python -m src.mcp.registry.api_server