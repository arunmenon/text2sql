#!/bin/bash
# Script to run the Metadata Registry MCP Server

# Set Neo4j credentials
export NEO4J_URI="neo4j://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="neo4j"  # Replace with your actual password

# Install required packages
pip install neo4j pyyaml mcp requests

# Set current directory to project root
cd "$(dirname "$0")/../../.."

# Create required directories
mkdir -p src/mcp/registry/data/services

# Ensure the service definition exists
if [ ! -f "src/mcp/registry/data/services/neo4j.yaml" ]; then
  cp src/mcp/registry/services/neo4j.yaml src/mcp/registry/data/services/
fi

# Run the server
echo "Starting Metadata Registry MCP Server..."
python -m src.mcp.registry.run_metadata_registry_server --debug

# Use these to override config values:
# python -m src.mcp.registry.run_metadata_registry_server --host 0.0.0.0 --port 8234