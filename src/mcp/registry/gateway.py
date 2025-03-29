"""
Service Gateway Implementation

This module implements the service gateway, which routes requests
to the appropriate service based on the operation being invoked.
"""
import logging
from typing import Dict, Any, Optional, List, Tuple

from .registry import ServiceRegistry, ServiceOperation, ServiceDefinition, ServiceStatus

logger = logging.getLogger(__name__)

class ServiceGateway:
    """
    Service Gateway for routing requests to appropriate services.
    
    The gateway handles request routing, conversion between protocols,
    and provides a unified interface for invoking operations on services.
    """
    
    def __init__(self, registry: ServiceRegistry):
        """Initialize the service gateway with a registry."""
        self.registry = registry
        logger.info("Service Gateway initialized")
    
    def invoke(self, operation_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Invoke an operation by name, routing to the appropriate service.
        
        Args:
            operation_name: Name of the operation to invoke
            params: Parameters to pass to the operation
            
        Returns:
            Result from the operation or error information
        """
        # First, find which service provides this operation
        service = self.find_service_for_operation(operation_name)
        if not service:
            logger.warning(f"No service found providing operation: {operation_name}")
            return {"error": f"Operation not found: {operation_name}"}
        
        # Check if service is available
        if service.status not in [ServiceStatus.ACTIVE, ServiceStatus.UNKNOWN]:
            logger.warning(f"Service {service.name} is not active (status: {service.status})")
            return {"error": f"Service {service.name} is not available"}
        
        # Find the operation itself
        operation = None
        for op in service.operations:
            if op.name == operation_name:
                operation = op
                break
        
        if not operation:
            # This shouldn't happen if find_service_for_operation worked
            logger.error(f"Operation {operation_name} not found in service {service.name}")
            return {"error": f"Operation not found: {operation_name}"}
        
        # Validate params against schema if needed
        # This would check that required parameters are provided, types match, etc.
        # validation_result = self._validate_params(operation, params)
        # if validation_result.get("error"):
        #     return validation_result
        
        # Invoke the operation
        return self.registry.invoke_operation(service.id, operation_name, params)
    
    def find_service_for_operation(self, operation_name: str) -> Optional[ServiceDefinition]:
        """Find the service that provides a specific operation."""
        return self.registry.get_service_by_operation(operation_name)
    
    def get_available_operations(self) -> List[Dict[str, Any]]:
        """Get all available operations from active services."""
        operations = []
        
        for service in self.registry.get_services():
            if service.status in [ServiceStatus.ACTIVE, ServiceStatus.UNKNOWN]:
                for operation in service.operations:
                    operations.append({
                        "name": operation.name,
                        "description": operation.description,
                        "schema": operation.schema,
                        "service": service.name,
                        "service_id": service.id,
                        "deprecated": operation.deprecated
                    })
        
        return operations
    
    def get_operation_details(self, operation_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific operation."""
        service = self.find_service_for_operation(operation_name)
        if not service:
            return None
        
        for operation in service.operations:
            if operation.name == operation_name:
                return {
                    "name": operation.name,
                    "description": operation.description,
                    "schema": operation.schema,
                    "service": service.name,
                    "service_id": service.id,
                    "version": operation.version,
                    "deprecated": operation.deprecated,
                    "tags": operation.tags
                }
        
        return None  # Should not reach here
    
    def batch_invoke(self, operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Invoke multiple operations in a batch.
        
        Args:
            operations: List of operation invocations, each with 'name' and 'params'
                
        Returns:
            List of results, one for each operation
        """
        results = []
        
        for op in operations:
            operation_name = op.get("name")
            params = op.get("params", {})
            
            if not operation_name:
                results.append({"error": "Operation name is required"})
                continue
            
            result = self.invoke(operation_name, params)
            results.append(result)
        
        return results