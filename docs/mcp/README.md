# Model Context Protocol (MCP) Documentation

> **Comprehensive documentation for the Model Context Protocol implementation in Text2SQL**

## Documentation Overview

This directory contains comprehensive documentation for the Model Context Protocol (MCP) implementation in the Text2SQL project. The documentation is organized into several documents, each focusing on different aspects of the MCP architecture and implementation.

### Key Documents

- **[ARCHITECTURE.md](./ARCHITECTURE.md)**: High-level overview of the MCP architecture, design patterns, and theoretical foundations
- **[IMPLEMENTATION.md](./IMPLEMENTATION.md)**: Detailed implementation guide with code examples, configuration, and practical aspects

## What is MCP?

The Model Context Protocol (MCP) is a standardized way for Large Language Models (LLMs) like Claude to interact with external tools, services, and data sources. It enables LLMs to:

- Discover available tools and their capabilities
- Understand tool inputs and outputs through schemas
- Invoke tools with appropriate parameters
- Receive structured responses
- Operate within defined trust boundaries

## MCP in Text2SQL

In the Text2SQL project, MCP is used to expose the Neo4j Knowledge Graph to LLMs, enabling them to:

- Query the semantic schema
- Retrieve table and column metadata
- Discover relationships between entities
- Search for business terms and concepts
- Generate more accurate SQL based on the knowledge graph

## Key Components

The MCP implementation consists of several key components:

1. **Metadata Registry**: Central registry for service metadata
2. **Discovery Feeds**: Mechanisms for finding and registering services
3. **MCP Protocol Handler**: Implements the MCP specification
4. **Service Adapters**: Connect MCP to service implementations
5. **Trust Manager**: Enforces security boundaries

## Getting Started

To get started with MCP in Text2SQL:

1. **Setup Neo4j**: Ensure the Neo4j database is running with the Text2SQL knowledge graph
2. **Start the MCP Server**: Run `./src/mcp/registry/run_metadata_registry_server.sh`
3. **Connect Claude**: Configure Claude to access the MCP server at `http://localhost:8234`
4. **Test Functionality**: Try queries that leverage the knowledge graph

## Adding New Services

To add a new service to the MCP implementation:

1. Create a service definition in YAML format
2. Place it in the `data/services/` directory
3. Optionally implement a service adapter for complex operations
4. The service will be automatically discovered and registered

## Additional Resources

- **[Dynamic Service Discovery](../mcp/registry/docs/dynamic_service_discovery.md)**: Detailed documentation on service discovery mechanisms
- **[Service API](../mcp/registry/api/service_api.py)**: API for programmatic service registration
- **[Example Code](../mcp/registry/examples/)**: Example implementations and usage
- **[Configuration](../mcp/registry/config/)**: Configuration files and options

## Quick Reference

### Running the MCP Server

```bash
# Start the MCP server
./src/mcp/registry/run_metadata_registry_server.sh

# Start the API server
./src/mcp/registry/run_api_server.sh
```

### Basic Client Usage

```python
from mcp.client import Client

# Connect to the MCP server
client = Client("http://localhost:8234")

# Get available resources
resources = await client.get_resources()

# Find the Knowledge Graph resource
kg = next(r for r in resources if r.name == "knowledge_graph")

# Execute a query
result = await kg.query(
    query="MATCH (t:Table) RETURN t.name LIMIT 5"
)
```

### Service Definition Template

```yaml
service_id: my-service-id
name: my_service
description: "Description of my service"
service_type: "service_type"
version: "1.0.0"
endpoints:
  uri: "${SERVICE_URI:-http://localhost:8080}"
operations:
  - name: operation_name
    description: "Operation description"
    schema:
      type: object
      properties:
        param_name:
          type: string
          description: "Parameter description"
      required:
        - param_name
```

## Support and Contribution

For support or to contribute to the MCP implementation:

- **Issues**: Report issues on the GitHub repository
- **Contributions**: Submit pull requests with improvements or new features
- **Questions**: Reach out to the Text2SQL team for clarification

---

This documentation aims to provide a comprehensive understanding of the MCP implementation in Text2SQL. For further details on specific aspects, please refer to the individual documents linked above.