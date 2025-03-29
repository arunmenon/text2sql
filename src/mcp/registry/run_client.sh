#!/bin/bash
# Script to run the Metadata Registry MCP Client example

# Install required packages
pip install neo4j pyyaml mcp requests

# Set current directory to project root
cd "$(dirname "$0")/../../.."

# Run the client
echo "Starting Metadata Registry MCP Client..."
python -m src.mcp.registry.client_example