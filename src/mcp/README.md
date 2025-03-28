# Knowledge Graph MCP Server

This module provides a Model Context Protocol (MCP) server implementation for exposing the Knowledge Graph functionality to MCP-compatible LLMs like Claude.

## Overview

The Knowledge Graph MCP Server enables LLMs to:

1. Query the semantic graph directly using Cypher
2. Retrieve information about entities, tables, and business terms
3. Discover relationships between entities
4. Enhance SQL generation with semantic knowledge

## Setup

### Installation

Install the required packages:

```bash
pip install mcp-server mcp-client
```

### Environment Variables

Configure the Neo4j connection settings:

```bash
export NEO4J_URI=neo4j://localhost:7687
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=your_password
```

## Running the Server

Start the server:

```bash
python -m src.mcp.server --port 8234
```

## MCP Operations

The server exposes the following operations:

| Operation | Description |
|-----------|-------------|
| `knowledge_graph.query` | Run a Cypher query against the graph |
| `knowledge_graph.get_entity` | Get detailed information about an entity |
| `knowledge_graph.find_relationships` | Find paths between two entities |
| `knowledge_graph.get_table_schema` | Get schema information for a table |
| `knowledge_graph.search_business_terms` | Search for business terms by keyword |
| `knowledge_graph.get_column_metadata` | Get detailed metadata for a column |

## Client Example

```python
from mcp_client import Client

# Connect to MCP server
client = Client(base_url="http://localhost:8234")

# Get table schema
response = client.invoke(
    "knowledge_graph.get_table_schema",
    {"table_name": "WorkOrders"}
)

print(response)
```

## Integration with Text2SQL

The MCP server can be used with Claude or other MCP-compatible LLMs to enhance SQL generation:

1. LLM receives natural language query
2. LLM uses MCP to query the knowledge graph for relevant entities and relationships
3. LLM generates SQL based on semantic understanding from the graph

This approach leverages the semantic knowledge in the graph while keeping the implementation flexible and modular.