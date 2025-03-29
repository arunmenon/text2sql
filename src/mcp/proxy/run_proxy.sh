#!/bin/bash
# Script to run the MCP Proxy

# Export Neo4j credentials as environment variables
export NEO4J_URI="neo4j://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="your-password"  # Replace with your actual password

# Install required packages
pip install neo4j pyyaml mcp

# Set current directory to project root
cd "$(dirname "$0")/../../.."

# Run the proxy
echo "Starting MCP Proxy..."
python -m src.mcp.proxy.run

# Use these for overriding config values:
# python -m src.mcp.proxy.run --host 0.0.0.0 --port 8234