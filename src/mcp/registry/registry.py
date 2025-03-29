"""
Service Registry Core Implementation

This module implements the central Service Registry component that manages
registration, discovery, and routing for all services.
"""
import os
import json
import yaml
import logging
import threading
import time
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
import importlib.util
import inspect
from dataclasses import dataclass, field, asdict
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Service status constants
class ServiceStatus:
    ACTIVE = "active"
    INACTIVE = "inactive"
    DEGRADED = "degraded"
    ERROR = "error"
    UNKNOWN = "unknown"

@dataclass
class ServiceOperation:
    """Definition of a service operation."""
    id: str
    name: str
    description: str
    schema: Dict[str, Any]
    handler: Optional[Callable] = None
    version: str = "1.0"
    deprecated: bool = False
    tags: List[str] = field(default_factory=list)

@dataclass
class ServiceDefinition:
    """Definition of a registered service."""
    id: str
    name: str
    description: str
    version: str
    service_type: str
    operations: List[ServiceOperation] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    endpoints: Dict[str, str] = field(default_factory=dict)
    status: str = ServiceStatus.UNKNOWN
    registered_at: datetime = field(default_factory=datetime.now)
    last_health_check: Optional[datetime] = None
    tags: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with serializable values."""
        result = asdict(self)
        # Convert datetime objects to ISO format strings
        result['registered_at'] = self.registered_at.isoformat()
        if self.last_health_check:
            result['last_health_check'] = self.last_health_check.isoformat()
        # Remove handler references as they're not serializable
        for op in result['operations']:
            op.pop('handler', None)
        return result


class ServiceRegistry:
    """
    Central registry for service registration and discovery.
    
    This class maintains a registry of all available services and their
    capabilities, handling registration, discovery, and health checking.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the service registry."""
        self.services: Dict[str, ServiceDefinition] = {}
        self.lock = threading.RLock()
        self.config = self._load_config(config_path) if config_path else {}
        self.health_check_interval = self.config.get('health_check_interval', 60)  # seconds
        self.health_check_thread = None
        self.running = False
        
        # Load services from configuration
        if config_path:
            self._load_services_from_config()
            
        logger.info("Service Registry initialized")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from file."""
        if not os.path.exists(config_path):
            logger.warning(f"Config file not found: {config_path}")
            return {}
            
        with open(config_path, 'r') as f:
            if config_path.endswith('.yaml') or config_path.endswith('.yml'):
                return yaml.safe_load(f)
            elif config_path.endswith('.json'):
                return json.load(f)
            else:
                logger.warning(f"Unknown config file format: {config_path}")
                return {}
    
    def _load_services_from_config(self):
        """Load and register services from configuration."""
        services_config = self.config.get('services', [])
        for service_config in services_config:
            try:
                # Extract basic service info
                service_type = service_config.get('type')
                service_name = service_config.get('name')
                
                if not service_type or not service_name:
                    logger.warning(f"Skipping service with missing type or name: {service_config}")
                    continue
                
                # Generate a unique ID if not provided
                service_id = service_config.get('id', f"{service_name}-{uuid.uuid4()}")
                
                # Create service definition
                service = ServiceDefinition(
                    id=service_id,
                    name=service_name,
                    description=service_config.get('description', ''),
                    version=service_config.get('version', '1.0'),
                    service_type=service_type,
                    metadata=service_config.get('metadata', {}),
                    endpoints=service_config.get('endpoints', {}),
                    tags=service_config.get('tags', [])
                )
                
                # Register operations
                for op_config in service_config.get('operations', []):
                    operation = ServiceOperation(
                        id=op_config.get('id', str(uuid.uuid4())),
                        name=op_config.get('name'),
                        description=op_config.get('description', ''),
                        schema=op_config.get('schema', {}),
                        version=op_config.get('version', '1.0'),
                        deprecated=op_config.get('deprecated', False),
                        tags=op_config.get('tags', [])
                    )
                    service.operations.append(operation)
                
                # Register the service
                self.register_service(service)
                
                # Try to load service adapter if specified
                adapter_path = service_config.get('adapter')
                if adapter_path:
                    self._load_service_adapter(service_id, adapter_path)
                
            except Exception as e:
                logger.error(f"Error loading service from config: {str(e)}")
    
    def _load_service_adapter(self, service_id: str, adapter_path: str):
        """Load a service adapter module and initialize it."""
        try:
            # Resolve adapter path
            if adapter_path.startswith('.'):
                # Relative path, resolve relative to this file
                base_dir = os.path.dirname(os.path.abspath(__file__))
                adapter_path = os.path.normpath(os.path.join(base_dir, adapter_path))
            
            # Import the adapter module
            module_name = os.path.basename(adapter_path).replace('.py', '')
            spec = importlib.util.spec_from_file_location(module_name, adapter_path)
            if not spec or not spec.loader:
                logger.error(f"Could not load adapter spec from {adapter_path}")
                return
                
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Look for adapter class
            for name, obj in inspect.getmembers(module):
                if inspect.isclass(obj) and hasattr(obj, 'initialize'):
                    # Initialize adapter with service
                    adapter = obj()
                    service = self.get_service(service_id)
                    if service:
                        adapter.initialize(service, self)
                        logger.info(f"Initialized adapter for service {service_id}: {name}")
                        return
            
            logger.warning(f"No valid adapter class found in {adapter_path}")
        
        except Exception as e:
            logger.error(f"Error loading adapter {adapter_path}: {str(e)}")
    
    def register_service(self, service: ServiceDefinition) -> bool:
        """Register a new service with the registry."""
        with self.lock:
            if service.id in self.services:
                logger.warning(f"Service already registered with ID {service.id}")
                return False
            
            self.services[service.id] = service
            logger.info(f"Registered service: {service.name} ({service.id})")
            return True
    
    def update_service(self, service_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing service's properties."""
        with self.lock:
            if service_id not in self.services:
                logger.warning(f"Service not found: {service_id}")
                return False
            
            service = self.services[service_id]
            
            # Update basic properties
            for key, value in updates.items():
                if key in ['name', 'description', 'version', 'status', 'metadata', 'endpoints', 'tags']:
                    setattr(service, key, value)
            
            logger.info(f"Updated service: {service.name} ({service_id})")
            return True
    
    def deregister_service(self, service_id: str) -> bool:
        """Remove a service from the registry."""
        with self.lock:
            if service_id not in self.services:
                logger.warning(f"Service not found: {service_id}")
                return False
            
            service = self.services.pop(service_id)
            logger.info(f"Deregistered service: {service.name} ({service_id})")
            return True
    
    def get_service(self, service_id: str) -> Optional[ServiceDefinition]:
        """Get a service by ID."""
        return self.services.get(service_id)
    
    def get_services(self, filter_criteria: Optional[Dict[str, Any]] = None) -> List[ServiceDefinition]:
        """Get all services, optionally filtered by criteria."""
        if not filter_criteria:
            return list(self.services.values())
        
        result = []
        for service in self.services.values():
            matches = True
            for key, value in filter_criteria.items():
                if key == 'tags' and isinstance(value, list):
                    # For tags, check if any requested tag is present
                    if not any(tag in service.tags for tag in value):
                        matches = False
                        break
                elif key == 'status' and value != service.status:
                    matches = False
                    break
                elif key == 'service_type' and value != service.service_type:
                    matches = False
                    break
                elif key == 'name' and value not in service.name:
                    matches = False
                    break
            
            if matches:
                result.append(service)
        
        return result
    
    def get_operation(self, service_id: str, operation_name: str) -> Optional[ServiceOperation]:
        """Get an operation from a service."""
        service = self.get_service(service_id)
        if not service:
            return None
        
        for operation in service.operations:
            if operation.name == operation_name:
                return operation
        
        return None
    
    def update_service_status(self, service_id: str, status: str) -> bool:
        """Update a service's status."""
        with self.lock:
            if service_id not in self.services:
                logger.warning(f"Service not found: {service_id}")
                return False
            
            self.services[service_id].status = status
            self.services[service_id].last_health_check = datetime.now()
            return True
    
    def get_service_by_operation(self, operation_name: str) -> Optional[ServiceDefinition]:
        """Find a service that provides a specific operation."""
        for service in self.services.values():
            for operation in service.operations:
                if operation.name == operation_name:
                    return service
        return None
    
    def register_operation_handler(self, service_id: str, operation_name: str, 
                                 handler: Callable) -> bool:
        """Register a handler function for a service operation."""
        operation = self.get_operation(service_id, operation_name)
        if not operation:
            logger.warning(f"Operation {operation_name} not found for service {service_id}")
            return False
        
        operation.handler = handler
        logger.info(f"Registered handler for operation {operation_name} of service {service_id}")
        return True
    
    def invoke_operation(self, service_id: str, operation_name: str, 
                       params: Dict[str, Any]) -> Dict[str, Any]:
        """Invoke an operation on a service."""
        operation = self.get_operation(service_id, operation_name)
        if not operation:
            return {"error": f"Operation {operation_name} not found for service {service_id}"}
        
        if not operation.handler:
            return {"error": f"No handler registered for operation {operation_name}"}
        
        try:
            result = operation.handler(**params)
            return {"result": result}
        except Exception as e:
            logger.error(f"Error invoking operation {operation_name}: {str(e)}")
            return {"error": str(e)}
    
    def start_health_checks(self):
        """Start periodic health checks for registered services."""
        if self.health_check_thread:
            logger.warning("Health check thread already running")
            return
        
        self.running = True
        self.health_check_thread = threading.Thread(
            target=self._health_check_loop,
            daemon=True
        )
        self.health_check_thread.start()
        logger.info("Service health checks started")
    
    def stop_health_checks(self):
        """Stop periodic health checks."""
        self.running = False
        if self.health_check_thread:
            self.health_check_thread.join(timeout=5)
            self.health_check_thread = None
        logger.info("Service health checks stopped")
    
    def _health_check_loop(self):
        """Main loop for periodic health checks."""
        while self.running:
            try:
                self._check_all_services()
            except Exception as e:
                logger.error(f"Error in health check loop: {str(e)}")
            
            # Sleep for the configured interval
            time.sleep(self.health_check_interval)
    
    def _check_all_services(self):
        """Perform health check on all registered services."""
        for service_id, service in list(self.services.items()):
            try:
                # Health check logic depends on service type
                # For now, just update the last_health_check timestamp
                service.last_health_check = datetime.now()
                
                # TODO: Implement actual health checks
                # This could call an endpoint, check a database connection, etc.
                
            except Exception as e:
                logger.error(f"Health check failed for service {service_id}: {str(e)}")
                service.status = ServiceStatus.ERROR
    
    def save_registry_state(self, path: str):
        """Save the current registry state to a file."""
        with self.lock:
            try:
                # Convert services to serializable format
                services_dict = {
                    service_id: service.to_dict()
                    for service_id, service in self.services.items()
                }
                
                state = {
                    "services": services_dict,
                    "saved_at": datetime.now().isoformat()
                }
                
                with open(path, 'w') as f:
                    json.dump(state, f, indent=2)
                
                logger.info(f"Registry state saved to {path}")
                return True
            
            except Exception as e:
                logger.error(f"Error saving registry state: {str(e)}")
                return False
    
    def load_registry_state(self, path: str):
        """Load registry state from a file."""
        if not os.path.exists(path):
            logger.warning(f"Registry state file not found: {path}")
            return False
        
        try:
            with open(path, 'r') as f:
                state = json.load(f)
            
            services_dict = state.get("services", {})
            
            with self.lock:
                # Clear current services
                self.services.clear()
                
                # Load services from state
                for service_id, service_data in services_dict.items():
                    # Create operations
                    operations = []
                    for op_data in service_data.get("operations", []):
                        operation = ServiceOperation(
                            id=op_data.get("id"),
                            name=op_data.get("name"),
                            description=op_data.get("description", ""),
                            schema=op_data.get("schema", {}),
                            version=op_data.get("version", "1.0"),
                            deprecated=op_data.get("deprecated", False),
                            tags=op_data.get("tags", [])
                        )
                        operations.append(operation)
                    
                    # Parse dates
                    registered_at = datetime.fromisoformat(service_data.get("registered_at"))
                    last_health_check = None
                    if service_data.get("last_health_check"):
                        last_health_check = datetime.fromisoformat(service_data.get("last_health_check"))
                    
                    # Create service
                    service = ServiceDefinition(
                        id=service_id,
                        name=service_data.get("name"),
                        description=service_data.get("description", ""),
                        version=service_data.get("version", "1.0"),
                        service_type=service_data.get("service_type"),
                        operations=operations,
                        metadata=service_data.get("metadata", {}),
                        endpoints=service_data.get("endpoints", {}),
                        status=service_data.get("status", ServiceStatus.UNKNOWN),
                        registered_at=registered_at,
                        last_health_check=last_health_check,
                        tags=service_data.get("tags", [])
                    )
                    
                    # Add to registry
                    self.services[service_id] = service
            
            logger.info(f"Registry state loaded from {path}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading registry state: {str(e)}")
            return False


# Singleton instance
_registry_instance = None

def get_registry(config_path: Optional[str] = None) -> ServiceRegistry:
    """Get the singleton ServiceRegistry instance."""
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = ServiceRegistry(config_path)
    return _registry_instance