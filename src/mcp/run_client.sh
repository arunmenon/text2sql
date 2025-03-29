#!/bin/bash
# Script to run the simplified MCP client

# Install required packages if not already installed
pip install requests

# Run the client
echo "Running simplified MCP client against server at http://localhost:8234..."
python src/mcp/simplified_client.py

# Use this instead if your server is running on a different port or host
# python src/mcp/simplified_client.py --url http://localhost:8000