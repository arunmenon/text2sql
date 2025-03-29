# Metadata Registry MCP Server

This directory contains a metadata-centric MCP server implementation that exposes a Knowledge Graph service to LLMs via the Model Context Protocol (MCP).

## Architecture

The Metadata Registry MCP Server uses a metadata-centric approach for service discovery and exposure through MCP. The key components are:

1. **Metadata Registry**: Central registry for service metadata that supports multiple discovery mechanisms
2. **Discovery Feeds**: Multiple mechanisms for discovering and registering services (files, HTTP, plugins)
3. **Trust Manager**: Handles security boundaries and consent for service operations
4. **Metadata Registry Server**: Exposes services through the MCP protocol

## Neo4j Knowledge Graph Service

The primary service exposed is the Neo4j Knowledge Graph service, which provides operations for:

- Running Cypher queries
- Getting table schemas
- Finding relationships between entities
- Searching for business terms
- Recommending tables for natural language queries

## Getting Started

### Prerequisites

- Python 3.8+
- Neo4j 4.4+ running locally or remotely
- `mcp` Python package

### Setup

1. Ensure Neo4j is running and accessible
2. Edit `run_metadata_registry_server.sh` with your Neo4j credentials
3. Run the server: `./run_metadata_registry_server.sh`
4. Connect a client: `./run_client.sh`

### Configuration

The server can be configured through:

1. Environment variables:
   - `NEO4J_URI`: URI for Neo4j connection
   - `NEO4J_USER`: Neo4j username
   - `NEO4J_PASSWORD`: Neo4j password

2. Command-line arguments:
   - `--config`: Path to configuration file
   - `--storage`: Path to storage directory
   - `--neo4j-uri`: Neo4j connection URI
   - `--neo4j-user`: Neo4j username
   - `--neo4j-password`: Neo4j password
   - `--host`: Host to bind the server to
   - `--port`: Port to bind the server to
   - `--debug`: Enable debug logging

## Service Definition

Services are defined in YAML files in the `services` directory. The Neo4j service definition (`services/neo4j.yaml`) includes:

- Service metadata (ID, name, description)
- Endpoints and connection information
- Available operations and their schemas
- Tags for discovery

## Using with Claude

1. Start the server: `./run_metadata_registry_server.sh`
2. Connect Claude to the local MCP server at `http://localhost:8234`
3. Claude can now use the Knowledge Graph service for:
   - Exploring database schema
   - Finding relationships between entities
   - Running semantic queries
   - Generating SQL based on natural language

## Extending

To add new services:

1. Create a new service definition in `services/`
2. Implement an adapter in `adapters/`
3. Register the service operations

The discovery system will automatically find and expose the new service.

## Decoupled Design

The implementation follows a decoupled design with:

1. Clear separation between service implementations and MCP protocol
2. Metadata-centric approach focusing on capabilities, not implementations
3. Multiple discovery mechanisms for flexibility
4. Trust boundaries for security and consent

This design makes it easy to add new services without modifying the core system.