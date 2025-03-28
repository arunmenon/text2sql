# Enhanced Knowledge Graph MCP Server

This module provides an extended Model Context Protocol (MCP) server implementation for exposing the Knowledge Graph functionality to MCP-compatible LLMs like Claude, with comprehensive operation discovery and additional functionality.

## Overview

The Enhanced Knowledge Graph MCP Server enables LLMs to:

1. Query the semantic graph directly using Cypher
2. Retrieve information about entities, tables, and business terms
3. Discover relationships between entities
4. Recommend tables and join paths for queries
5. Translate between business and technical concepts
6. Enhance SQL generation with semantic knowledge

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

Start the basic server:

```bash
python -m src.mcp.server --port 8234
```

Or start the enhanced server with more operations:

```bash
python -m src.mcp.enhanced_server --port 8234
```

## MCP Operations

### Basic Operations

| Operation | Description |
|-----------|-------------|
| `knowledge_graph.query` | Run a Cypher query against the graph |
| `knowledge_graph.get_entity` | Get detailed information about an entity |
| `knowledge_graph.find_relationships` | Find paths between two entities |
| `knowledge_graph.get_table_schema` | Get schema information for a table |
| `knowledge_graph.search_business_terms` | Search for business terms by keyword |
| `knowledge_graph.get_column_metadata` | Get detailed metadata for a column |

### Enhanced Operations

The enhanced server adds these additional operations:

| Operation | Description |
|-----------|-------------|
| `knowledge_graph.recommend_tables_for_query` | Suggest relevant tables for a natural language query |
| `knowledge_graph.find_join_paths` | Discover possible join paths between multiple tables |
| `knowledge_graph.get_entity_neighborhood` | Return all entities connected to a given entity |
| `knowledge_graph.translate_business_term` | Map business terms to technical entities |

## Operation Discovery

MCP clients can automatically discover available operations:

```python
from mcp_client import Client

# Connect to MCP server
client = Client(base_url="http://localhost:8234")

# Discover available operations
tools = client.list_tools()
for tool in tools:
    print(f"{tool.name}: {tool.description}")
    print(f"  Parameters: {tool.schema}")
```

## Client Examples

### Basic Usage

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

### Enhanced Client

See `enhanced_client_example.py` for a more comprehensive example that:
1. Discovers all available operations
2. Displays their schemas and documentation
3. Demonstrates how to use various operations

## Integration with Text2SQL

The MCP server can be used with Claude or other MCP-compatible LLMs to enhance SQL generation:

1. LLM receives natural language query
2. LLM uses MCP to query the knowledge graph for relevant entities and relationships
3. LLM generates SQL based on semantic understanding from the graph

Example Claude prompt with MCP:

```
Using MCP, please analyze this user query and generate appropriate SQL:

User query: "Show me all completed work orders from last month"

1. Use knowledge_graph.recommend_tables_for_query to find relevant tables
2. Use knowledge_graph.get_table_schema to understand table structure
3. Use knowledge_graph.translate_business_term if business terms are used
4. Generate a SQL query based on the graph information
```

This approach leverages the semantic knowledge in the graph while keeping the implementation flexible and modular, with automatic discovery of available operations.

## Advanced Use Cases

### Entity Resolution

```python
# Find a business term and its technical mappings
response = client.invoke(
    "knowledge_graph.translate_business_term",
    {"term_name": "Active Asset"}
)

# Use the mappings to resolve entities in a query
```

### Join Path Discovery

```python
# Find how to join tables for a complex query
response = client.invoke(
    "knowledge_graph.find_join_paths",
    {
        "table_names": ["WorkOrders", "Locations", "Proposals"],
        "max_hops": 2
    }
)

# Use the join paths to construct SQL queries
```