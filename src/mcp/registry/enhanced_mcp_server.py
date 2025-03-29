"""
Enhanced MCP Protocol Handler Implementation

This module implements an enhanced MCP protocol handler that connects
to the metadata registry to expose services via MCP.
"""
import os
import logging
import json
from typing import Dict, List, Any, Optional
import asyncio

from mcp.server import Server, Resource, resolver, Response, JSONSchema

from .metadata_registry import MetadataRegistry, get_metadata_registry
from .trust import get_trust_manager
from .feeds import ServiceMetadata

logger = logging.getLogger(__name__)

class EnhancedMCPHandler:
    """
    Enhanced MCP Protocol Handler with metadata-centric approach.
    
    This handler connects to the metadata registry to discover services
    and exposes them via the MCP protocol, with security boundaries.
    """
    
    def __init__(self, registry: Optional[MetadataRegistry] = None, 
                config_path: Optional[str] = None):
        """Initialize the MCP Protocol Handler."""
        self.registry = registry or get_metadata_registry(config_path)
        self.trust_manager = get_trust_manager()
        self.server = None
        logger.info("Enhanced MCP Protocol Handler initialized")
    
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
            # Use the first service for basic resource info
            first_service = services[0]
            
            resource = Resource(
                name=service_name,
                description=first_service.description
            )
            
            # Register operations from all services with this name
            self._register_operations(resource, services)
            
            resources.append(resource)
        
        # Create MCP server
        self.server = Server(resources=resources)
        return self.server
    
    def _register_operations(self, resource, services: List[ServiceMetadata]):
        """Register operations from services with the MCP resource."""
        # Track operations we've already registered to avoid duplicates
        registered_ops = set()
        
        for service in services:
            for operation_data in service.operations:
                op_name = operation_data['name']
                
                # Skip if already registered
                if op_name in registered_ops:
                    continue
                
                registered_ops.add(op_name)
                
                # Create operation schema
                schema = operation_data.get('schema', {})
                description = operation_data.get('description', '')
                
                # Create a closure over the current service and operation
                service_id = service.service_id
                
                # Define operation resolver
                @resolver(resource.operation(
                    op_name,
                    description=description,
                    schema=schema
                ))
                async def operation_handler(**params):
                    """Dynamic operation handler that routes to appropriate service."""
                    # This is where we'll actually route the request to the service
                    # For now, we'll just return a placeholder response
                    
                    # Check if service is verified
                    if not self.trust_manager.is_service_verified(service_id):
                        verification_data = {
                            'type': 'local',
                            'host': 'localhost'
                        }
                        if not self.trust_manager.verify_service(service_id, verification_data):
                            return Response(error=f"Service {service_id} is not verified")
                    
                    # Check for required permissions
                    required_permission = f"{op_name}"
                    if not self.trust_manager.check_consent(service_id, required_permission):
                        # Auto-grant consent for demo purposes
                        # In a real app, this would show a UI prompt
                        consent = self.trust_manager.request_consent(
                            service_id, 
                            [required_permission],
                            {'operation': op_name}
                        )
                        if not consent:
                            return Response(error=f"Consent denied for operation {op_name}")
                    
                    # In a real implementation, this would call the actual service
                    # For now, we'll just return the parameters
                    result = {
                        "status": "success",
                        "service_id": service_id,
                        "operation": op_name,
                        "params": params,
                        "note": "This is a placeholder response. In a real implementation, this would call the actual service."
                    }
                    
                    return Response(data=result)
    
    def run(self, host: str = "0.0.0.0", port: int = 8234):
        """Run the MCP server."""
        # Start discovery mechanisms
        self.registry.start_discovery()
        
        try:
            # Create the server
            if not self.server:
                self.create_server()
            
            logger.info(f"Starting Enhanced MCP server on {host}:{port}")
            
            # Start the server
            self.server.run(host=host, port=port)
        finally:
            # Stop discovery mechanisms
            self.registry.stop_discovery()


def run_enhanced_server(config_path: Optional[str] = None, 
                       storage_path: Optional[str] = None,
                       host: str = "0.0.0.0", 
                       port: int = 8234):
    """
    Run the enhanced MCP server with the given configuration.
    
    Args:
        config_path: Path to configuration file
        storage_path: Path to storage directory
        host: Host to bind the server to
        port: Port to bind the server to
    """
    # Initialize the registry
    registry = get_metadata_registry(config_path, storage_path)
    
    # Start discovery
    registry.start_discovery()
    
    try:
        # Create and run the MCP server
        handler = EnhancedMCPHandler(registry)
        handler.run(host=host, port=port)
    finally:
        # Stop discovery
        registry.stop_discovery()