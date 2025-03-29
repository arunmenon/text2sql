"""
MCP Protocol Handler Implementation

This module implements the MCP protocol handler that exposes
the service registry to Claude and other MCP-compatible LLMs.
"""
import os
import logging
from typing import Dict, Any, List, Optional
import asyncio

from mcp.server import Server, Resource, resolver, Response, JSONSchema

from .registry import ServiceRegistry, get_registry
from .gateway import ServiceGateway

logger = logging.getLogger(__name__)

class MCPProtocolHandler:
    """
    MCP Protocol Handler that exposes services via MCP.
    
    This class creates an MCP server that exposes all operations
    from the service registry as MCP operations.
    """
    
    def __init__(self, registry: Optional[ServiceRegistry] = None, config_path: Optional[str] = None):
        """Initialize the MCP Protocol Handler."""
        self.registry = registry or get_registry(config_path)
        self.gateway = ServiceGateway(self.registry)
        self.server = None
        logger.info("MCP Protocol Handler initialized")
    
    def create_server(self) -> Server:
        """Create the MCP server with resources for all services."""
        # Create resources for MCP
        resources = self._create_resources()
        
        # Create MCP server
        self.server = Server(resources=resources)
        return self.server
    
    def _create_resources(self) -> List[Resource]:
        """Create MCP resources for all services in the registry."""
        resources = []
        
        # Get all active services
        services = self.registry.get_services()
        
        # Group operations by service
        for service in services:
            # Create an MCP resource for each service
            resource = Resource(
                name=service.name,
                description=service.description
            )
            
            # Register operations
            self._register_operations(resource, service.id, service.operations)
            
            resources.append(resource)
        
        return resources
    
    def _register_operations(self, resource, service_id, operations):
        """Register operations from a service with an MCP resource."""
        for operation in operations:
            # Create operation schema
            schema = operation.schema
            
            # Define operation resolver
            @resolver(resource.operation(
                operation.name,
                description=operation.description,
                schema=schema
            ))
            async def operation_handler(**params):
                """Generic operation handler that invokes the underlying service."""
                # Pass the request to the gateway
                result = self.gateway.invoke(operation.name, params)
                
                # Convert to MCP response
                if "error" in result:
                    return Response(error=result["error"])
                else:
                    return Response(data=result.get("result"))
    
    def run(self, host: str = "0.0.0.0", port: int = 8234):
        """Run the MCP server."""
        if not self.server:
            self.create_server()
        
        logger.info(f"Starting MCP server on {host}:{port}")
        self.server.run(host=host, port=port)


def run_server(config_path: Optional[str] = None, host: str = "0.0.0.0", port: int = 8234):
    """Run the MCP server with the given configuration."""
    # Initialize the registry
    registry = get_registry(config_path)
    
    # Start health checks
    registry.start_health_checks()
    
    try:
        # Create and run the MCP server
        handler = MCPProtocolHandler(registry)
        handler.run(host=host, port=port)
    finally:
        # Stop health checks
        registry.stop_health_checks()