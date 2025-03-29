#!/bin/bash
# Script to run the Service Registry MCP Server

# Set Neo4j credentials
export NEO4J_URI="neo4j://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="your-password"  # Replace with your actual password

# Install required packages
pip install neo4j pyyaml mcp

# Set current directory to project root
cd "$(dirname "$0")/../../.."

# Run the server
echo "Starting Service Registry MCP Server..."
python -m src.mcp.registry.run --debug

# Use these to override config values:
# python -m src.mcp.registry.run --host 0.0.0.0 --port 8234