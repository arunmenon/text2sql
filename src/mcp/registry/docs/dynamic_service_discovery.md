# Dynamic Service Discovery and Registration

This document explains how the Metadata Registry dynamically discovers and registers services.

## Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                     Metadata Registry                              │
│                                                                   │
│  ┌───────────────┐    ┌───────────────┐    ┌───────────────────┐  │
│  │ Service Store │◄───┤Service Registry├───┤ Trust Management │  │
│  └───────────────┘    └───────┬───────┘    └───────────────────┘  │
│                              │                                     │
│                     ┌────────┴───────┐                             │
│                     │   Feed Manager │                             │
│                     └────────┬───────┘                             │
│                              │                                     │
│  ┌─────────────┐   ┌─────────┴─────┐   ┌────────────┐   ┌───────┐ │
│  │ File System │   │     HTTP      │   │   Plugin   │   │ Custom│ │
│  │    Feed     │   │     Feed      │   │    Feed    │   │ Feeds │ │
│  └──────┬──────┘   └──────┬────────┘   └─────┬──────┘   └───┬───┘ │
└─────────┼───────────────┬─┼────────────────┬─┼─────────────┬─┼────┘
          │               │ │                │ │             │ │
          ▼               ▼ ▼                ▼ ▼             ▼ ▼
     ┌─────────┐    ┌────────────┐     ┌──────────┐    ┌──────────┐
     │ YAML/JSON│    │ HTTP Service│    │  Python  │    │  Custom  │
     │  Files   │    │ Endpoints   │    │  Modules │    │  Sources │
     └─────────┘    └────────────┘     └──────────┘    └──────────┘
```

## Discovery Flow

1. **Initialization**: The Metadata Registry initializes the Feed Manager

2. **Feed Registration**: Different discovery feeds register with the Feed Manager:
   - `FileSystemFeed`: Watches directories for YAML/JSON service definitions
   - `HttpDiscoveryFeed`: Polls HTTP endpoints for service metadata
   - `PluginDiscoveryFeed`: Loads Python modules that export service definitions
   - Custom feeds can be implemented to discover services from other sources

3. **Service Discovery**: Each feed watches its source for services:
   - File feed: Watches directories for changes (using filesystem events or polling)
   - HTTP feed: Polls configured endpoints periodically 
   - Plugin feed: Scans plugin directories for Python modules
   - Custom feeds: Implement their own discovery logic

4. **Service Registration**: When a feed discovers a service:
   - It notifies the Feed Manager via the callback mechanism
   - The Feed Manager passes the service to the Registry
   - The Registry validates and stores the service
   - The Trust Manager verifies the service's permissions
   - The service is now available for MCP operations

5. **Dynamic Discovery**: Services can be added at any time:
   - Adding a YAML file to the watched directory
   - Registering via the HTTP API
   - Adding a Python plugin
   - Through custom discovery mechanisms

## Event-Based vs. Polling Discovery

The system supports both event-based and polling discovery:

- **Event-based**: Uses filesystem events (via watchdog) to immediately detect changes
- **Polling**: Periodically checks for changes at configured intervals
- **Both**: Falls back to polling if event-based detection isn't available

## Service Lifecycle

Services go through the following lifecycle:

1. **Discovery**: The service is found by a discovery feed
2. **Validation**: The service definition is validated
3. **Registration**: The service is added to the registry
4. **Verification**: The Trust Manager verifies the service (optional)
5. **Exposure**: The service is exposed via MCP
6. **Updates**: The service can be updated if a newer version is discovered
7. **Removal**: The service can be removed if its source disappears

## API-Based Registration

Services can register themselves programmatically via the API server:

1. The API server exposes endpoints for service management:
   - `POST /services`: Register a new service
   - `PUT /services/{id}`: Update an existing service
   - `DELETE /services/{id}`: Unregister a service
   - `GET /services`: List all services
   - `GET /services/{id}`: Get a specific service

2. The API server is optional and can be started separately from the MCP server:
   - `run_api_server.sh`: Starts the API server
   - Default port: 8235
   - Shares the same registry instance as the MCP server

## Example: Adding a Custom Feed

```python
# Define custom feed
class DatabaseFeed(DiscoveryFeed):
    def __init__(self, name, db_connection):
        super().__init__(name)
        self.db = db_connection
    
    def refresh(self):
        # Query database for services
        services = self.db.query("SELECT * FROM services")
        for svc in services:
            # Create service metadata
            service = ServiceMetadata(
                service_id=svc["id"],
                name=svc["name"],
                # ... other properties ...
            )
            # Notify registry
            self.notify_service_discovered(service)

# Register with registry
feed = DatabaseFeed("database_feed", db_connection)
registry.feeds.register_feed(feed)
feed.start()
```