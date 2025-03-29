# MCP Implementation Guide

> **Detailed implementation guide for the Model Context Protocol in the Text2SQL project**

## 1. Implementation Overview

This document provides a detailed look at how the Model Context Protocol (MCP) is implemented in the Text2SQL project, focusing on the practical aspects, code organization, and specific implementation choices.

```
/src/mcp/
├── __init__.py
├── registry/                    # Metadata Registry implementation
│   ├── __init__.py
│   ├── api/                     # API server for service registration
│   ├── config/                  # Configuration files
│   ├── data/                    # Service data storage
│   ├── docs/                    # Documentation
│   ├── examples/                # Example code
│   ├── feeds.py                 # Discovery feed implementations
│   ├── metadata_registry.py     # Core registry implementation
│   ├── metadata_registry_server.py  # MCP server implementation
│   ├── run_api_server.sh        # Script to run API server
│   ├── run_metadata_registry_server.py  # Main entry point
│   ├── run_metadata_registry_server.sh  # Script to run MCP server
│   ├── services/                # Built-in service definitions
│   └── trust.py                 # Trust and security implementation
├── client_example.py            # Example client code
└── check_neo4j.py               # Neo4j connection test utility
```

## 2. Core Components Implementation

### 2.1 Metadata Registry

The Metadata Registry is implemented in `metadata_registry.py` as a class that manages services and their metadata:

```python
class MetadataRegistry:
    """
    Lightweight, metadata-focused service registry.
    
    This registry focuses on service discovery and metadata management,
    acting as a connector between MCP and service implementations.
    """
    
    def __init__(self, config_path: Optional[str] = None, 
                storage_path: Optional[str] = None):
        """Initialize the metadata registry."""
        self.services: Dict[str, ServiceMetadata] = {}
        self.lock = threading.RLock()
        self.feeds: Optional[FeedManager] = None
        self.trust_manager = get_trust_manager(...)
        self.storage_path = storage_path
        
        # Load configuration if provided
        self.config = self._load_config(config_path) if config_path else {}
        
        # Load registry state if storage path provided
        if storage_path:
            self._load_registry_state()
        
        # Initialize feed manager
        self.feeds = FeedManager(self._handle_service_discovered)
        
        # Configure feeds from config
        self._configure_feeds()
```

Key methods include:
- `register_service(service)`: Registers a service with the registry
- `unregister_service(service_id)`: Removes a service from the registry
- `find_services(criteria)`: Finds services matching specific criteria
- `get_operation_metadata(service_id, operation_name)`: Gets metadata for a specific operation
- `refresh_discovery()`: Refreshes all discovery feeds
- `_handle_service_discovered(service)`: Called when a feed discovers a service

The registry is implemented as a singleton for consistent access across the application.

### 2.2 Service Metadata

Service metadata is encapsulated in the `ServiceMetadata` class in `feeds.py`:

```python
class ServiceMetadata:
    """Metadata about a service for discovery purposes."""
    
    def __init__(self, 
                 service_id: str, 
                 name: str, 
                 description: str = "", 
                 version: str = "1.0.0",
                 service_type: str = "unknown", 
                 endpoints: Dict[str, str] = None,
                 operations: List[Dict[str, Any]] = None,
                 tags: List[str] = None,
                 metadata: Dict[str, Any] = None,
                 source: str = "unknown"):
        """Initialize service metadata."""
        self.service_id = service_id
        self.name = name
        self.description = description
        # ... other properties ...
```

This class provides methods for serialization and deserialization:
- `to_dict()`: Converts metadata to a dictionary for storage
- `from_dict(data)`: Creates metadata from a dictionary

### 2.3 Discovery Feeds

Discovery feeds are implemented as abstract and concrete classes in `feeds.py`:

```python
class DiscoveryFeed(ABC):
    """
    Abstract base class for service discovery feeds.
    
    A discovery feed is a source of service metadata that can be
    used to discover and register services with the registry.
    """
    
    def __init__(self, name: str, enabled: bool = True):
        """Initialize the discovery feed."""
        self.name = name
        self.enabled = enabled
        self.callbacks: List[Callable[[ServiceMetadata], None]] = []
    
    @abstractmethod
    def start(self):
        """Start the discovery feed."""
        pass
    
    @abstractmethod
    def stop(self):
        """Stop the discovery feed."""
        pass
    
    @abstractmethod
    def refresh(self):
        """Refresh service discoveries."""
        pass
```

Concrete implementations include:
- `FileSystemFeed`: Discovers services from configuration files
- `HttpDiscoveryFeed`: Discovers services from HTTP endpoints
- `PluginDiscoveryFeed`: Discovers services from Python modules

#### 2.3.1 File System Feed

The File System Feed monitors directories for service definitions:

```python
class FileSystemFeed(DiscoveryFeed):
    """
    File system based service discovery feed.
    
    Discovers services from configuration files in a directory.
    Supports both polling and event-based notification (where available).
    """
    
    def start(self):
        """Start the file system feed."""
        # ... implementation ...
        
        # Try to set up file system watcher if available and requested
        if self.use_events and self._setup_file_watcher():
            logger.info(f"Using event-based file watching for feed {self.name}")
        # Fall back to polling if events not available or failed
        elif self.poll_interval > 0:
            self.running = True
            self.poll_thread = threading.Thread(
                target=self._poll_loop,
                daemon=True
            )
            self.poll_thread.start()
```

It supports both event-based notification (using the `watchdog` library) and polling for maximum compatibility.

### 2.4 MCP Server Implementation

The MCP server is implemented in `metadata_registry_server.py`:

```python
class MetadataRegistryServer:
    """
    Metadata Registry MCP Server with metadata-centric approach.
    
    This server connects to the metadata registry to discover services
    and exposes them via the MCP protocol, with security boundaries.
    """
    
    def __init__(self, registry: Optional[MetadataRegistry] = None, 
                config_path: Optional[str] = None):
        """Initialize the MCP Protocol Handler."""
        self.registry = registry or get_metadata_registry(config_path)
        self.trust_manager = get_trust_manager()
        self.server = None
    
    def create_server(self) -> Server:
        """Create the MCP server with resources based on registry services."""
        # Create resources for MCP based on registered services
        resources = []
        
        # Group services by name to create resources
        service_map: Dict[str, List[ServiceMetadata]] = {}
        
        for service in self.registry.services.values():
            if service.name not in service_map:
                service_map[service.name] = []
            service_map[service.name].append(service)
        
        # Create a resource for each service name
        for service_name, services in service_map.items():
            # ... implementation ...
            
        # Create MCP server
        self.server = Server(resources=resources)
        return self.server
```

The server translates between the registry's metadata model and the MCP protocol's resource/operation model.

### 2.5 Neo4j Adapter

The Neo4j adapter in `adapters/neo4j_adapter.py` connects the MCP layer to the Neo4j database:

```python
class Neo4jAdapter:
    """
    Neo4j Adapter for the Service Registry.
    
    This adapter provides operations for interacting with the Neo4j
    knowledge graph from the service registry.
    """
    
    def __init__(self):
        """Initialize the Neo4j adapter."""
        self.registry = None
        self.service_id = None
        self.driver = None
    
    def initialize(self, service: ServiceDefinition, registry: ServiceRegistry):
        """Initialize the adapter with service and registry information."""
        self.registry = registry
        self.service_id = service.id
        
        # Connect to Neo4j
        uri = service.endpoints.get("uri", "neo4j://localhost:7687")
        username = service.metadata.get("username", "neo4j")
        password = service.metadata.get("password", "password")
        
        try:
            self.driver = GraphDatabase.driver(uri, auth=(username, password))
            # ... implementation ...
```

The adapter implements operations like:
- `query_graph`: Executes a Cypher query
- `get_table_schema`: Gets schema information for a table
- `find_relationships`: Finds paths between entities
- `search_business_terms`: Searches for business terms

## 3. Configuration Implementation

Configuration is handled through YAML files and environment variables:

### 3.1 Registry Configuration

The registry configuration in `config/registry.yaml`:

```yaml
# Metadata Registry Configuration

# Storage configuration
storage:
  path: "${STORAGE_PATH:-./data}"
  persistence: true

# Service discovery configuration
discovery:
  # Automatic discovery on startup
  auto_discover: true
  
  # Feeds for discovering services
  feeds:
    # Local filesystem feed - discovers services from YAML/JSON files
    - name: "local_files"
      type: "file"
      enabled: true
      directory: "${SERVICES_DIR:-./data/services}"
      pattern: "*.{json,yaml,yml}"
      poll_interval: 30  # Check for changes every 30 seconds
    
    # ... other feeds ...
```

Environment variables can override settings using the `${VAR:-default}` syntax.

### 3.2 Service Definition

Service definitions follow a standard format:

```yaml
# Neo4j Knowledge Graph Service Definition

service_id: kg-neo4j
name: knowledge_graph
description: "Knowledge Graph for text-to-SQL operations"
service_type: "neo4j"
version: "1.0.0"
endpoints:
  uri: ${NEO4J_URI:-neo4j://localhost:7687}
metadata:
  username: ${NEO4J_USER:-neo4j}
  password: ${NEO4J_PASSWORD:-password}
  verification:
    type: "local"
    host: "localhost"
tags:
  - knowledge_graph
  - database
operations:
  - name: query
    description: "Run a Cypher query against the knowledge graph"
    schema:
      type: object
      properties:
        query:
          type: string
          description: "Valid Cypher query string to execute"
        parameters:
          type: object
          description: "Optional parameters for the Cypher query"
      required:
        - query
  
  # ... other operations ...
```

Each operation includes a schema that defines its inputs and outputs.

## 4. API Server Implementation

The API server is implemented in `api/service_api.py` using Flask:

```python
app = Flask(__name__)
registry = None

@app.route('/services', methods=['GET'])
def list_services():
    """List all registered services."""
    if not registry:
        return jsonify({"error": "Registry not initialized"}), 500
    
    services = []
    for service_id, service in registry.services.items():
        services.append(service.to_dict())
    
    return jsonify({"services": services})

@app.route('/services', methods=['POST'])
def register_service():
    """Register a new service."""
    if not registry:
        return jsonify({"error": "Registry not initialized"}), 500
    
    try:
        # Get service data from request
        service_data = request.json
        # ... implementation ...
```

The API server provides endpoints for:
- `GET /services`: List all services
- `GET /services/{id}`: Get a specific service
- `POST /services`: Register a new service
- `PUT /services/{id}`: Update a service
- `DELETE /services/{id}`: Remove a service
- `POST /discover`: Trigger service discovery

## 5. Trust and Security Implementation

Trust management is implemented in `trust.py`:

```python
class TrustManager:
    """Trust Manager for Service Registry."""
    
    def __init__(self, storage_path: Optional[str] = None):
        """Initialize the Trust Manager."""
        self.storage_path = storage_path
        self.verified_services: Dict[str, Dict[str, Any]] = {}
        self.consent_store: Dict[str, Dict[str, bool]] = {}
        
        # Load trust state
        if storage_path:
            self._load_trust_state()
    
    def verify_service(self, service_id: str, verification_data: Dict[str, Any]) -> bool:
        """Verify a service based on provided verification data."""
        # ... implementation ...
```

The Trust Manager provides:
- Service verification
- Consent management
- Permission checking
- Audit logging

## 6. Entry Points and Scripts

The main entry points are implemented as Python scripts with shell wrappers:

### 6.1 MCP Server

`run_metadata_registry_server.py`:

```python
def main():
    """Main entry point for the Metadata Registry MCP Server."""
    parser = argparse.ArgumentParser(description="Metadata Registry MCP Server")
    # ... argument parsing ...
    
    # Initialize the registry
    registry = get_metadata_registry(args.config, args.storage)
    
    # Run the server
    try:
        logger.info(f"Starting Metadata Registry MCP Server on {args.host}:{args.port}")
        run_metadata_registry_server(
            config_path=args.config,
            storage_path=args.storage,
            host=args.host,
            port=args.port
        )
    # ... exception handling ...
```

`run_metadata_registry_server.sh`:

```bash
#!/bin/bash
# Script to run the Metadata Registry MCP Server

# Set Neo4j credentials
export NEO4J_URI="neo4j://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="neo4j"  # Replace with your actual password

# Install required packages
pip install neo4j pyyaml mcp requests

# Run the server
echo "Starting Metadata Registry MCP Server..."
python -m src.mcp.registry.run_metadata_registry_server --debug
```

### 6.2 API Server

`api_server.py`:

```python
def main():
    """Main entry point for the Metadata Registry API Server."""
    parser = argparse.ArgumentParser(description="Metadata Registry API Server")
    # ... argument parsing ...
    
    # Initialize the registry
    registry = get_metadata_registry(args.config, args.storage)
    
    # Start discovery
    registry.start_discovery()
    
    # Run the API server
    try:
        logger.info(f"Starting Metadata Registry API Server on {args.host}:{args.port}")
        run_api(
            host=args.host,
            port=args.port,
            registry_instance=registry
        )
    # ... exception handling ...
```

`run_api_server.sh`:

```bash
#!/bin/bash
# Script to run the Metadata Registry API Server

# Install required packages
pip install flask pyyaml

# Run the API server
echo "Starting Metadata Registry API Server..."
python -m src.mcp.registry.api_server
```

## 7. Extension Points

The implementation includes several extension points:

### 7.1 Custom Discovery Feeds

Custom discovery feeds can be implemented by subclassing `DiscoveryFeed`:

```python
class CustomDiscoveryFeed(DiscoveryFeed):
    """Custom discovery feed implementation."""
    
    def start(self):
        """Start the discovery feed."""
        # Implementation-specific startup logic
    
    def stop(self):
        """Stop the discovery feed."""
        # Implementation-specific shutdown logic
    
    def refresh(self):
        """Refresh service discoveries."""
        # Implementation-specific discovery logic
        
        # Create service metadata
        service = ServiceMetadata(
            service_id="custom-service",
            name="custom_service",
            # ... other properties ...
        )
        
        # Notify callbacks
        self.notify_service_discovered(service)
```

### 7.2 Custom Service Adapters

Custom service adapters can be implemented to connect different services:

```python
class CustomServiceAdapter:
    """Custom service adapter implementation."""
    
    def initialize(self, service, registry):
        """Initialize the adapter."""
        # Implementation-specific initialization
        
        # Register operations
        registry.register_operation_handler(
            service.id, "custom_operation", self.custom_operation
        )
    
    def custom_operation(self, **params):
        """Custom operation implementation."""
        # Implementation-specific operation logic
        return result
```

### 7.3 Enhanced Trust Management

Trust management can be extended with custom policies:

```python
class EnhancedTrustManager(TrustManager):
    """Enhanced trust manager with custom policies."""
    
    def verify_service(self, service_id, verification_data):
        """Verify a service with enhanced policies."""
        # Check base verification
        if not super().verify_service(service_id, verification_data):
            return False
        
        # Implement enhanced policies
        # ...
        
        return result
```

## 8. Integration with Text2SQL

The MCP implementation integrates with Text2SQL through several touchpoints:

### 8.1 Knowledge Graph Access

The primary integration is through the Neo4j Knowledge Graph:

```python
# In MCP Neo4j adapter
def get_table_schema(self, table_name: str, include_relationships: bool = False) -> Dict[str, Any]:
    """Get table schema."""
    # Build schema query
    schema_query = """
    MATCH (t:Table {name: $table_name})
    OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
    WITH t, collect(c) as columns
    """
    
    # ... query execution ...
```

This query accesses the same semantic schema built by Text2SQL.

### 8.2 Shared Configuration

Configuration is shared through environment variables:

```python
# In both MCP and Text2SQL
uri = os.environ.get("NEO4J_URI", "neo4j://localhost:7687")
username = os.environ.get("NEO4J_USER", "neo4j")
password = os.environ.get("NEO4J_PASSWORD", "password")
```

### 8.3 Complementary Workflows

MCP and Text2SQL complement each other in workflows:

1. Text2SQL builds the Knowledge Graph from schemas and data
2. MCP exposes the Knowledge Graph to LLMs
3. LLMs query the Knowledge Graph to understand data relationships
4. LLMs generate SQL based on this understanding
5. The SQL is executed against the original database

## 9. Testing and Debugging

The implementation includes tools for testing and debugging:

### 9.1 Client Example

`client_example.py` demonstrates how to use the MCP client:

```python
async def main():
    """Run the client example."""
    try:
        # Connect to the MCP server
        client = Client("http://localhost:8234")
        
        # Get available resources
        resources = await client.get_resources()
        logger.info(f"Available resources: {[r.name for r in resources]}")
        
        # Find the Knowledge Graph resource
        kg_resource = None
        for resource in resources:
            if resource.name == "knowledge_graph":
                kg_resource = resource
                break
        
        # ... usage examples ...
```

### 9.2 Neo4j Connection Test

`check_neo4j.py` tests the Neo4j connection:

```python
def check_neo4j_connection(uri, user, password):
    """Check connection to Neo4j database."""
    try:
        # Create driver
        driver = GraphDatabase.driver(uri, auth=(user, password))
        
        # Test connection
        with driver.session() as session:
            result = session.run("MATCH (n) RETURN count(n) as count LIMIT 1")
            count = result.single()["count"]
        
        logger.info(f"Successfully connected to Neo4j at {uri}")
        logger.info(f"Database contains {count} nodes")
        
        # Close driver
        driver.close()
        
        return True
    except Exception as e:
        logger.error(f"Failed to connect to Neo4j: {str(e)}")
        return False
```

### 9.3 Debugging Tools

The implementation includes several debugging tools:

- Verbose logging with configurable levels
- Command-line flags for enabling debug mode
- Informative error messages with context
- Explicit state transitions for easier tracking

## 10. Deployment Considerations

When deploying the MCP implementation, consider:

### 10.1 Environment Configuration

Set environment variables for configuration:

```bash
# Neo4j connection
export NEO4J_URI="neo4j://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="your-password"

# MCP server
export HOST="0.0.0.0"
export PORT="8234"

# Discovery
export SERVICES_DIR="/path/to/services"
export SERVICE_REGISTRY_URL="http://registry-service:8235/services"
```

### 10.2 Service Directory Structure

Organize service definitions in a structured way:

```
/services/
├── core/
│   ├── knowledge_graph.yaml
│   └── vector_store.yaml
├── analytics/
│   ├── reporting.yaml
│   └── metrics.yaml
└── integration/
    ├── external_apis.yaml
    └── webhooks.yaml
```

### 10.3 Security Hardening

Implement security best practices:

- Run the MCP server behind a reverse proxy
- Use HTTPS for all connections
- Implement proper authentication and authorization
- Set up firewall rules to restrict access
- Use read-only database users where appropriate

### 10.4 Monitoring and Logging

Set up monitoring and logging:

- Configure structured logging
- Implement health checks
- Set up alerting for errors
- Monitor resource usage
- Track performance metrics

## 11. Example Walkthroughs

### 11.1 Adding a New Service

To add a new service:

1. Create a service definition file:

```yaml
# services/vector_store.yaml
service_id: vector-store
name: vector_store
description: "Vector database for semantic search"
service_type: "vector"
version: "1.0.0"
endpoints:
  uri: "${VECTOR_DB_URI:-http://localhost:6333}"
operations:
  - name: search_vectors
    description: "Search for similar vectors"
    schema:
      type: object
      properties:
        query_vector:
          type: array
          items:
            type: number
          description: "Query vector to search for"
        top_k:
          type: integer
          description: "Number of results to return"
      required:
        - query_vector
```

2. Implement an adapter (if needed):

```python
class VectorStoreAdapter:
    """Adapter for vector store service."""
    
    def initialize(self, service, registry):
        """Initialize the adapter."""
        # ... initialization ...
        
        # Register operations
        registry.register_operation_handler(
            service.id, "search_vectors", self.search_vectors
        )
    
    def search_vectors(self, query_vector, top_k=10):
        """Search for similar vectors."""
        # ... implementation ...
        return results
```

3. Place the service definition in a discovered directory:

```bash
cp services/vector_store.yaml /path/to/services/
```

The service will be automatically discovered and registered.

### 11.2 Using the API for Registration

To register a service via the API:

```python
import requests

# Service definition
service = {
    "service_id": "api-registered-service",
    "name": "api_service",
    "description": "Service registered via API",
    "operations": [
        {
            "name": "api_operation",
            "description": "Operation registered via API",
            "schema": {
                "type": "object",
                "properties": {
                    "param": {"type": "string"}
                }
            }
        }
    ]
}

# Register via API
response = requests.post(
    "http://localhost:8235/services",
    json=service
)

print(f"Response: {response.status_code}")
print(f"Data: {response.json()}")
```

## 12. Troubleshooting

Common issues and their solutions:

### 12.1 Neo4j Connection Issues

**Problem**: Unable to connect to Neo4j database.

**Solution**:
- Check Neo4j is running: `curl -v http://localhost:7474`
- Verify credentials in environment variables
- Check network connectivity and firewall rules
- Use `check_neo4j.py` to test connection

### 12.2 Service Discovery Problems

**Problem**: Services not being discovered.

**Solution**:
- Check file paths and permissions
- Verify YAML syntax in service definitions
- Enable debug logging for discovery feeds
- Check feed configuration in registry.yaml
- Manually trigger discovery with API: `POST /discover`

### 12.3 MCP Server Issues

**Problem**: MCP server not starting or responding.

**Solution**:
- Check port availability: `lsof -i :8234`
- Verify MCP package installation: `pip show mcp`
- Check for errors in server logs
- Try running with `--debug` flag
- Verify service registrations with the API server

## 13. Conclusion

The MCP implementation in Text2SQL provides a robust, extensible system for exposing the Knowledge Graph to LLMs. By following a metadata-centric approach and implementing dynamic service discovery, it creates a flexible foundation that can grow and adapt to changing needs.

The clean separation between discovery, registry, and protocol handling makes the system maintainable and extensible, while the integration with Text2SQL enhances the capabilities of both systems.

Through careful design and implementation, the MCP components provide not just current functionality but a platform for future innovation in AI-driven SQL generation.