# Neo4j Setup for MCP Knowledge Graph Server

To properly test and run the MCP Knowledge Graph server, you need to install the Neo4j Python driver and ensure your Neo4j database is running.

## Setting Up the Environment

### Option 1: Using a Virtual Environment (Recommended)

```bash
# Create a virtual environment
python -m venv kg-mcp-venv

# Activate the virtual environment
source kg-mcp-venv/bin/activate

# Install required packages
pip install neo4j mcp-server mcp-client

# Run the test script
python src/mcp/test_neo4j_connection.py
```

### Option 2: Using System Python with Homebrew (macOS)

```bash
# Install Neo4j driver via Homebrew
brew install python-neo4j

# Run the test script
python src/mcp/test_neo4j_connection.py
```

## Neo4j Connection Configuration

The MCP server and test script use the following environment variables for Neo4j connection:

```bash
# Set Neo4j connection details
export NEO4J_URI="neo4j://localhost:7687"  # Replace with your Neo4j URI
export NEO4J_USER="neo4j"                  # Replace with your Neo4j username
export NEO4J_PASSWORD="your-password"      # Replace with your Neo4j password
```

## Testing Neo4j Connectivity

Run the test script to verify that your MCP server can connect to Neo4j:

```bash
python src/mcp/test_neo4j_connection.py
```

If successful, you should see output confirming connectivity and operation tests.

## Running the MCP Server

Once Neo4j connectivity is verified, you can run the MCP server:

```bash
# Run basic MCP server
python -m src.mcp.server

# Or run enhanced MCP server with more operations
python -m src.mcp.enhanced_server
```

The server will be available at http://localhost:8234 by default.

## Connecting to Claude

After starting the MCP server, you can connect it to Claude by:

1. Opening Claude Desktop
2. Navigating to MCP settings
3. Adding a new server with URL http://localhost:8234
4. Starting a conversation that uses the Knowledge Graph operations

## Troubleshooting

If you encounter connection issues:

1. Verify Neo4j is running: `nc -zv localhost 7687`
2. Check credentials are correct
3. Ensure Neo4j allows Bolt connections
4. Confirm your database contains the expected graph data