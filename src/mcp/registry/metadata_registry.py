"""
Metadata-Focused Service Registry

This module implements a lightweight metadata registry that focuses on
service discovery and metadata management, not implementation details.
"""
import os
import json
import logging
import threading
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Callable
from pathlib import Path

from .feeds import ServiceMetadata, FeedManager, create_feed_from_config
from .trust import TrustManager, get_trust_manager

logger = logging.getLogger(__name__)

class MetadataRegistry:
    """
    Lightweight, metadata-focused service registry.
    
    This registry focuses on service discovery and metadata management,
    acting as a connector between MCP and service implementations.
    """
    
    def __init__(self, 
                config_path: Optional[str] = None,
                storage_path: Optional[str] = None):
        """
        Initialize the metadata registry.
        
        Args:
            config_path: Path to configuration file
            storage_path: Path to storage directory for persistence
        """
        self.services: Dict[str, ServiceMetadata] = {}
        self.lock = threading.RLock()
        self.feeds: Optional[FeedManager] = None
        self.trust_manager = get_trust_manager(
            storage_path=os.path.join(storage_path, "trust") if storage_path else None
        )
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
        
        logger.info("Metadata Registry initialized")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from file.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Configuration dictionary
        """
        if not os.path.exists(config_path):
            logger.warning(f"Config file not found: {config_path}")
            return {}
        
        try:
            with open(config_path, 'r') as f:
                if config_path.endswith('.json'):
                    return json.load(f)
                elif config_path.endswith(('.yaml', '.yml')):
                    import yaml
                    return yaml.safe_load(f)
                else:
                    logger.warning(f"Unknown config format: {config_path}")
                    return {}
        except Exception as e:
            logger.error(f"Error loading config: {str(e)}")
            return {}
    
    def _configure_feeds(self):
        """Configure discovery feeds from configuration."""
        if not self.feeds:
            return
            
        feeds_config = self.config.get("feeds", [])
        
        for feed_config in feeds_config:
            try:
                feed = create_feed_from_config(feed_config)
                if feed:
                    self.feeds.register_feed(feed)
            except Exception as e:
                logger.error(f"Error creating feed: {str(e)}")
    
    def _handle_service_discovered(self, service: ServiceMetadata):
        """
        Handle a service discovered by a feed.
        
        Args:
            service: Discovered service metadata
        """
        with self.lock:
            # Check if service already exists
            if service.service_id in self.services:
                existing = self.services[service.service_id]
                
                # Check if discovered service is newer
                if (hasattr(service, 'version') and 
                    hasattr(existing, 'version') and 
                    service.version > existing.version):
                    # Update with newer version
                    self.services[service.service_id] = service
                    logger.info(f"Updated service {service.name} ({service.service_id}) to version {service.version}")
                
                # Otherwise keep existing service
                return
            
            # Add new service
            self.services[service.service_id] = service
            logger.info(f"Discovered new service: {service.name} ({service.service_id})")
            
            # Save registry state
            if self.storage_path:
                self._save_registry_state()
    
    def _load_registry_state(self):
        """Load registry state from storage."""
        state_path = self._get_state_path()
        
        if not os.path.exists(state_path):
            logger.info("No registry state file found")
            return
        
        try:
            with open(state_path, 'r') as f:
                state = json.load(f)
            
            services_data = state.get("services", {})
            
            with self.lock:
                # Clear current services
                self.services.clear()
                
                # Load services from state
                for service_id, data in services_data.items():
                    service = ServiceMetadata.from_dict(data)
                    self.services[service_id] = service
            
            logger.info(f"Loaded {len(self.services)} services from registry state")
        except Exception as e:
            logger.error(f"Error loading registry state: {str(e)}")
    
    def _save_registry_state(self):
        """Save registry state to storage."""
        if not self.storage_path:
            return
        
        try:
            os.makedirs(self.storage_path, exist_ok=True)
            
            state_path = self._get_state_path()
            
            with self.lock:
                # Convert services to serializable format
                services_dict = {
                    service_id: service.to_dict()
                    for service_id, service in self.services.items()
                }
                
                state = {
                    "services": services_dict,
                    "saved_at": datetime.now().isoformat()
                }
            
            with open(state_path, 'w') as f:
                json.dump(state, f, indent=2)
            
            logger.debug(f"Registry state saved to {state_path}")
            
        except Exception as e:
            logger.error(f"Error saving registry state: {str(e)}")
    
    def _get_state_path(self) -> str:
        """Get path to registry state file."""
        return os.path.join(self.storage_path, "registry_state.json")
    
    def start_discovery(self):
        """Start all discovery feeds."""
        if self.feeds:
            self.feeds.start_all()
    
    def stop_discovery(self):
        """Stop all discovery feeds."""
        if self.feeds:
            self.feeds.stop_all()
    
    def refresh_discovery(self):
        """Refresh all discovery feeds."""
        if self.feeds:
            self.feeds.refresh_all()
    
    def register_service(self, service: ServiceMetadata) -> bool:
        """
        Register a service with the registry.
        
        Args:
            service: Service metadata to register
            
        Returns:
            True if registered successfully, False otherwise
        """
        # Verify the service if possible
        if hasattr(service, 'metadata') and 'verification' in service.metadata:
            verification_data = service.metadata['verification']
            if not self.trust_manager.verify_service(service.service_id, verification_data):
                logger.warning(f"Service verification failed: {service.service_id}")
                return False
        
        with self.lock:
            self.services[service.service_id] = service
            logger.info(f"Registered service: {service.name} ({service.service_id})")
            
            # Save registry state
            if self.storage_path:
                self._save_registry_state()
            
            return True
    
    def unregister_service(self, service_id: str) -> bool:
        """
        Unregister a service from the registry.
        
        Args:
            service_id: ID of service to unregister
            
        Returns:
            True if unregistered, False if not found
        """
        with self.lock:
            if service_id not in self.services:
                return False
            
            service = self.services.pop(service_id)
            logger.info(f"Unregistered service: {service.name} ({service_id})")
            
            # Save registry state
            if self.storage_path:
                self._save_registry_state()
            
            return True
    
    def get_service(self, service_id: str) -> Optional[ServiceMetadata]:
        """
        Get a service by ID.
        
        Args:
            service_id: Service ID to look up
            
        Returns:
            ServiceMetadata if found, None otherwise
        """
        return self.services.get(service_id)
    
    def find_services(self, criteria: Dict[str, Any]) -> List[ServiceMetadata]:
        """
        Find services matching specified criteria.
        
        Args:
            criteria: Dictionary of criteria to match against
            
        Returns:
            List of matching services
        """
        results = []
        
        for service in self.services.values():
            matches = True
            
            for key, value in criteria.items():
                # Special case for tags
                if key == 'tags' and isinstance(value, list):
                    if not any(tag in service.tags for tag in value):
                        matches = False
                        break
                # Special case for service type
                elif key == 'service_type':
                    if service.service_type != value:
                        matches = False
                        break
                # Special case for name (partial match)
                elif key == 'name':
                    if not value.lower() in service.name.lower():
                        matches = False
                        break
                # Special case for operations
                elif key == 'operation':
                    if not any(op['name'] == value for op in service.operations):
                        matches = False
                        break
                # Default case
                elif not hasattr(service, key) or getattr(service, key) != value:
                    matches = False
                    break
            
            if matches:
                results.append(service)
        
        return results
    
    def get_operation_metadata(self, service_id: str, operation_name: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a specific operation.
        
        Args:
            service_id: ID of the service
            operation_name: Name of the operation
            
        Returns:
            Operation metadata if found, None otherwise
        """
        service = self.get_service(service_id)
        if not service:
            return None
        
        for operation in service.operations:
            if operation['name'] == operation_name:
                return operation
        
        return None
    
    def find_service_by_operation(self, operation_name: str) -> Optional[ServiceMetadata]:
        """
        Find a service that provides a specific operation.
        
        Args:
            operation_name: Name of the operation to find
            
        Returns:
            ServiceMetadata if found, None otherwise
        """
        for service in self.services.values():
            for operation in service.operations:
                if operation['name'] == operation_name:
                    return service
        
        return None
    
    def get_all_operations(self) -> List[Dict[str, Any]]:
        """
        Get all available operations across all services.
        
        Returns:
            List of operation metadata with service information
        """
        operations = []
        
        for service in self.services.values():
            for operation in service.operations:
                op_info = dict(operation)
                op_info.update({
                    'service_id': service.service_id,
                    'service_name': service.name,
                    'service_type': service.service_type
                })
                operations.append(op_info)
        
        return operations
    
    def get_service_count(self) -> int:
        """Get the number of registered services."""
        return len(self.services)
    
    def clear(self):
        """Clear all registered services."""
        with self.lock:
            self.services.clear()
            logger.info("Registry cleared")
            
            # Save registry state
            if self.storage_path:
                self._save_registry_state()
    
    def watch_for_changes(self, interval: int = 30):
        """
        Start a background thread to watch for changes in service definitions.
        
        This is useful when you want more frequent updates than the normal
        polling interval of a feed, or to detect services that register
        themselves via API.
        
        Args:
            interval: Polling interval in seconds
        """
        def _watch_thread():
            logger.info(f"Starting service definition watch thread (interval: {interval}s)")
            while True:
                try:
                    time.sleep(interval)
                    self.refresh_discovery()
                except Exception as e:
                    logger.error(f"Error in watch thread: {str(e)}")
                    # Keep thread running despite errors
        
        # Start watch thread
        watch_thread = threading.Thread(
            target=_watch_thread,
            daemon=True
        )
        watch_thread.start()
    
    def add_service_from_dict(self, service_data: Dict[str, Any]) -> Optional[str]:
        """
        Add a service from a dictionary of service data.
        
        This is useful for programmatically registering services
        or for adding services via an API endpoint.
        
        Args:
            service_data: Dictionary of service metadata
            
        Returns:
            Service ID if registered successfully, None otherwise
        """
        try:
            # Generate service ID if not provided
            service_id = service_data.get("service_id", service_data.get("id"))
            if not service_id:
                name = service_data.get("name", "unnamed")
                service_id = f"{name}-{uuid.uuid4()}"
                service_data["service_id"] = service_id
            
            # Create service metadata
            service = ServiceMetadata.from_dict(service_data)
            service.source = "api"
            
            # Register the service
            if self.register_service(service):
                return service_id
            return None
        except Exception as e:
            logger.error(f"Error adding service from dict: {str(e)}")
            return None


# Singleton instance
_registry_instance = None

def get_metadata_registry(config_path: Optional[str] = None, 
                         storage_path: Optional[str] = None) -> MetadataRegistry:
    """Get the singleton MetadataRegistry instance."""
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = MetadataRegistry(config_path, storage_path)
    return _registry_instance