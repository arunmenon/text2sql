"""
MCP Proxy Core

This module implements the core functionality of the MCP proxy,
handling MCP protocol requirements and routing to service adapters.
"""
import os
import yaml
import logging
import importlib
from pathlib import Path
from typing import Dict, List, Any, Optional

# Import MCP SDK
from mcp.server import Server, Resource, resolver, Response

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ServiceAdapter:
    """Base class for service adapters."""
    
    def __init__(self, service_config: Dict[str, Any]):
        """Initialize the service adapter with its configuration."""
        self.service_config = service_config
        self.name = service_config.get("name", "unnamed")
        self.description = service_config.get("description", "")
        self.enabled = service_config.get("enabled", True)
        self.operations = service_config.get("operations", [])
        self.connection = service_config.get("connection", {})
        
        # Initialize service-specific client
        self.client = self._initialize_client()
        
        logger.info(f"Initialized adapter for service: {self.name}")
    
    def _initialize_client(self):
        """Initialize the service client (override in subclasses)."""
        raise NotImplementedError("Service adapters must implement _initialize_client")
    
    def create_resource(self) -> Resource:
        """Create an MCP resource for this service."""
        resource = Resource(
            name=self.name,
            description=self.description
        )
        
        # Register operations
        self._register_operations(resource)
        
        return resource
    
    def _register_operations(self, resource: Resource):
        """Register operations for this service (override in subclasses)."""
        raise NotImplementedError("Service adapters must implement _register_operations")


class ServiceRegistry:
    """Registry for service adapters."""
    
    def __init__(self):
        """Initialize the service registry."""
        self.adapters = {}
        self.resources = []
    
    def register_adapter(self, adapter: ServiceAdapter):
        """Register a service adapter."""
        if adapter.enabled:
            self.adapters[adapter.name] = adapter
            self.resources.append(adapter.create_resource())
            logger.info(f"Registered service: {adapter.name}")
    
    def get_adapter(self, name: str) -> Optional[ServiceAdapter]:
        """Get a service adapter by name."""
        return self.adapters.get(name)
    
    def get_resources(self) -> List[Resource]:
        """Get all resources."""
        return self.resources


class MCPProxy:
    """Main MCP Proxy class."""
    
    def __init__(self, config_path: str):
        """Initialize the MCP Proxy with a configuration file."""
        self.config = self._load_config(config_path)
        self.registry = ServiceRegistry()
        
        # Load service adapters
        self._load_adapters()
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from a YAML file with environment variable support."""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Process environment variables in the config
        config_str = str(config)
        for key, value in os.environ.items():
            placeholder = f"${{{key}}}"
            if placeholder in config_str:
                config_str = config_str.replace(placeholder, value)
        
        # Convert back to dict if environment variables were replaced
        if config_str != str(config):
            import ast
            config = ast.literal_eval(config_str)
        
        return config
    
    def _load_adapters(self):
        """Load service adapters based on the configuration."""
        for service_config in self.config.get("services", []):
            service_type = service_config.get("type")
            
            try:
                # Import the appropriate adapter module
                adapter_module = importlib.import_module(f"adapters.{service_type}")
                
                # Create adapter instance
                adapter_class = getattr(adapter_module, f"{service_type.capitalize()}Adapter")
                adapter = adapter_class(service_config)
                
                # Register the adapter
                self.registry.register_adapter(adapter)
            except (ImportError, AttributeError) as e:
                logger.error(f"Failed to load adapter for service type {service_type}: {str(e)}")
    
    def create_server(self) -> Server:
        """Create an MCP server with all registered resources."""
        resources = self.registry.get_resources()
        
        if not resources:
            logger.warning("No resources registered with the proxy")
        
        return Server(resources=resources)
    
    def run(self):
        """Run the MCP Proxy server."""
        server_config = self.config.get("server", {})
        host = server_config.get("host", "0.0.0.0")
        port = server_config.get("port", 8234)
        
        server = self.create_server()
        logger.info(f"Starting MCP Proxy on {host}:{port}")
        server.run(host=host, port=port)


def main():
    """Main entry point."""
    # Default to the config.yaml in the same directory
    config_path = os.environ.get("MCP_PROXY_CONFIG", 
                               str(Path(__file__).parent / "config.yaml"))
    
    proxy = MCPProxy(config_path)
    proxy.run()


if __name__ == "__main__":
    main()