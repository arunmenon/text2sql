#!/bin/bash
# Script to run the simplified MCP server

# Set Neo4j connection parameters (change these to match your setup)
export NEO4J_URI="neo4j://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="your-password"  # Replace with your actual password

# Install required packages if not already installed
pip install fastapi uvicorn neo4j pydantic requests

# Run the server
echo "Starting simplified MCP server on port 8234..."
python src/mcp/simplified_server.py

# Use this instead if you want to specify a different port
# python src/mcp/simplified_server.py --port 8000