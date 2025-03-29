"""
Service Discovery Feeds for Service Registry

This module implements multiple discovery mechanisms (feeds) for
finding and registering services, enabling extensible service discovery.
"""
import os
import json
import logging
import yaml
import time
import threading
import uuid
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
import importlib
import inspect
from pathlib import Path
import requests

logger = logging.getLogger(__name__)

# Service metadata structure
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
        self.version = version
        self.service_type = service_type
        self.endpoints = endpoints or {}
        self.operations = operations or []
        self.tags = tags or []
        self.metadata = metadata or {}
        self.source = source
        self.discovered_at = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "service_id": self.service_id,
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "service_type": self.service_type,
            "endpoints": self.endpoints,
            "operations": self.operations,
            "tags": self.tags,
            "metadata": self.metadata,
            "source": self.source,
            "discovered_at": self.discovered_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ServiceMetadata':
        """Create from dictionary."""
        service = cls(
            service_id=data["service_id"],
            name=data["name"],
            description=data.get("description", ""),
            version=data.get("version", "1.0.0"),
            service_type=data.get("service_type", "unknown"),
            endpoints=data.get("endpoints", {}),
            operations=data.get("operations", []),
            tags=data.get("tags", []),
            metadata=data.get("metadata", {}),
            source=data.get("source", "unknown")
        )
        
        if "discovered_at" in data:
            service.discovered_at = datetime.fromisoformat(data["discovered_at"])
        
        return service


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
    
    def register_callback(self, callback: Callable[[ServiceMetadata], None]):
        """Register a callback for when a service is discovered."""
        self.callbacks.append(callback)
    
    def notify_service_discovered(self, service: ServiceMetadata):
        """Notify all registered callbacks about a discovered service."""
        for callback in self.callbacks:
            try:
                callback(service)
            except Exception as e:
                logger.error(f"Error in discovery callback: {str(e)}")
    
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


class FileSystemFeed(DiscoveryFeed):
    """
    File system based service discovery feed.
    
    Discovers services from configuration files in a directory.
    Supports both polling and event-based notification (where available).
    """
    
    def __init__(self, name: str, directory: str, pattern: str = "*.{json,yaml,yml}", 
                 poll_interval: int = 0, enabled: bool = True, 
                 use_events: bool = True):
        """
        Initialize the file system feed.
        
        Args:
            name: Name of the feed
            directory: Directory to scan for service definitions
            pattern: File pattern to match
            poll_interval: Interval in seconds to poll for changes (0 = no polling)
            enabled: Whether the feed is enabled
            use_events: Whether to use event-based notification when available (faster than polling)
        """
        super().__init__(name, enabled)
        self.directory = directory
        self.pattern = pattern
        self.poll_interval = poll_interval
        self.use_events = use_events
        self.running = False
        self.poll_thread = None
        self.watch_thread = None
        self.last_scan_time = 0
        self.known_files: Dict[str, float] = {}  # path -> mtime
        
        # Track if we're using an event-based watcher
        self.using_events = False
        self.watcher = None
    
    def start(self):
        """Start the file system feed."""
        if not self.enabled:
            logger.info(f"Feed {self.name} is disabled")
            return
        
        if not os.path.exists(self.directory):
            logger.warning(f"Directory does not exist: {self.directory}")
            return
        
        # Perform initial scan
        self.refresh()
        
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
            logger.info(f"Started polling for feed {self.name} every {self.poll_interval} seconds")
    
    def _setup_file_watcher(self) -> bool:
        """
        Set up file system event watcher if available.
        
        Returns:
            True if event watcher was set up, False otherwise
        """
        try:
            # Try to import watchdog for file system events
            try:
                from watchdog.observers import Observer
                from watchdog.events import FileSystemEventHandler
            except ImportError:
                logger.debug("Watchdog package not available, falling back to polling")
                return False
            
            # Define event handler
            class ServiceFileHandler(FileSystemEventHandler):
                def __init__(self, feed):
                    self.feed = feed
                
                def on_created(self, event):
                    if not event.is_directory:
                        logger.debug(f"File created: {event.src_path}")
                        self.feed.refresh()
                
                def on_modified(self, event):
                    if not event.is_directory:
                        logger.debug(f"File modified: {event.src_path}")
                        self.feed.refresh()
                
                def on_moved(self, event):
                    if not event.is_directory:
                        logger.debug(f"File moved: {event.dest_path}")
                        self.feed.refresh()
            
            # Create observer and start watching
            observer = Observer()
            event_handler = ServiceFileHandler(self)
            observer.schedule(event_handler, self.directory, recursive=False)
            observer.start()
            
            # Store reference to observer
            self.watcher = observer
            self.using_events = True
            
            return True
        except Exception as e:
            logger.warning(f"Error setting up file watcher: {str(e)}")
            return False
    
    def stop(self):
        """Stop the file system feed."""
        self.running = False
        
        # Stop polling thread if active
        if self.poll_thread:
            self.poll_thread.join(timeout=5)
            self.poll_thread = None
        
        # Stop file watcher if active
        if self.watcher:
            self.watcher.stop()
            self.watcher.join(timeout=5)
            self.watcher = None
            self.using_events = False
    
    def refresh(self):
        """
        Scan for service definitions in the configured directory.
        
        This looks for JSON and YAML files containing service definitions
        and notifies about any found services.
        """
        try:
            current_time = time.time()
            self.last_scan_time = current_time
            
            # Track current files to detect removed ones
            current_files = set()
            
            # Find all matching files
            for pattern in self.pattern.split(','):
                pattern = pattern.strip()
                glob_pattern = os.path.join(self.directory, pattern)
                for path in Path(self.directory).glob(pattern):
                    path_str = str(path)
                    current_files.add(path_str)
                    
                    # Check if file has been modified
                    mtime = os.path.getmtime(path_str)
                    if path_str in self.known_files and mtime <= self.known_files[path_str]:
                        continue
                    
                    # Load and process the file
                    try:
                        services = self._load_service_file(path_str)
                        for service in services:
                            self.notify_service_discovered(service)
                        
                        # Update known files
                        self.known_files[path_str] = mtime
                    except Exception as e:
                        logger.error(f"Error loading service file {path_str}: {str(e)}")
            
            # Check for removed files
            removed_files = set(self.known_files.keys()) - current_files
            for path in removed_files:
                del self.known_files[path]
                logger.info(f"Service file removed: {path}")
                
        except Exception as e:
            logger.error(f"Error in file system feed refresh: {str(e)}")
    
    def _poll_loop(self):
        """Polling loop for file system feed."""
        while self.running:
            try:
                # Sleep first to avoid hammering if refresh crashes
                time.sleep(self.poll_interval)
                
                if self.running:  # Check again after sleep
                    self.refresh()
            except Exception as e:
                logger.error(f"Error in file system feed poll loop: {str(e)}")
    
    def _load_service_file(self, path: str) -> List[ServiceMetadata]:
        """
        Load service definitions from a file.
        
        Args:
            path: Path to the file
            
        Returns:
            List of ServiceMetadata objects
        """
        with open(path, 'r') as f:
            # Determine file type from extension
            if path.endswith('.json'):
                data = json.load(f)
            elif path.endswith(('.yaml', '.yml')):
                data = yaml.safe_load(f)
            else:
                raise ValueError(f"Unsupported file type: {path}")
        
        services = []
        
        # Handle different file formats
        if "services" in data:
            # Multiple services in one file
            for service_data in data["services"]:
                try:
                    service = self._create_service_from_data(service_data)
                    service.source = f"file:{os.path.basename(path)}"
                    services.append(service)
                except Exception as e:
                    logger.error(f"Error creating service from data in {path}: {str(e)}")
        elif "service_id" in data or "name" in data:
            # Single service in file
            try:
                service = self._create_service_from_data(data)
                service.source = f"file:{os.path.basename(path)}"
                services.append(service)
            except Exception as e:
                logger.error(f"Error creating service from data in {path}: {str(e)}")
        else:
            logger.warning(f"No service definitions found in {path}")
        
        return services
    
    def _create_service_from_data(self, data: Dict[str, Any]) -> ServiceMetadata:
        """
        Create a ServiceMetadata object from dictionary data.
        
        Args:
            data: Service data dictionary
            
        Returns:
            ServiceMetadata object
        """
        # Generate service ID if not provided
        service_id = data.get("service_id", data.get("id"))
        if not service_id:
            name = data.get("name", "unnamed")
            service_id = f"{name}-{uuid.uuid4()}"
        
        # Extract operations
        operations = data.get("operations", [])
        
        # Create service metadata
        service = ServiceMetadata(
            service_id=service_id,
            name=data.get("name", "unnamed"),
            description=data.get("description", ""),
            version=data.get("version", "1.0.0"),
            service_type=data.get("service_type", data.get("type", "unknown")),
            endpoints=data.get("endpoints", {}),
            operations=operations,
            tags=data.get("tags", []),
            metadata=data.get("metadata", {})
        )
        
        return service


class HttpDiscoveryFeed(DiscoveryFeed):
    """
    HTTP-based service discovery feed.
    
    Discovers services from HTTP/HTTPS endpoints.
    """
    
    def __init__(self, name: str, urls: List[str], 
                 poll_interval: int = 300, enabled: bool = True):
        """
        Initialize the HTTP discovery feed.
        
        Args:
            name: Name of the feed
            urls: List of URLs to poll for service definitions
            poll_interval: Interval in seconds to poll (default: 5 minutes)
            enabled: Whether the feed is enabled
        """
        super().__init__(name, enabled)
        self.urls = urls
        self.poll_interval = poll_interval
        self.running = False
        self.poll_thread = None
    
    def start(self):
        """Start the HTTP discovery feed."""
        if not self.enabled:
            logger.info(f"Feed {self.name} is disabled")
            return
        
        # Perform initial scan
        self.refresh()
        
        # Start polling if interval > 0
        if self.poll_interval > 0:
            self.running = True
            self.poll_thread = threading.Thread(
                target=self._poll_loop,
                daemon=True
            )
            self.poll_thread.start()
            logger.info(f"Started polling for feed {self.name} every {self.poll_interval} seconds")
    
    def stop(self):
        """Stop the HTTP discovery feed."""
        self.running = False
        if self.poll_thread:
            self.poll_thread.join(timeout=5)
            self.poll_thread = None
    
    def refresh(self):
        """
        Poll all configured URLs for service definitions.
        
        This fetches JSON from each URL and processes any service
        definitions found.
        """
        for url in self.urls:
            try:
                logger.debug(f"Polling URL: {url}")
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                
                # Parse response as JSON
                data = response.json()
                
                if "services" in data:
                    # Multiple services in response
                    for service_data in data["services"]:
                        try:
                            service = self._create_service_from_data(service_data)
                            service.source = f"http:{url}"
                            self.notify_service_discovered(service)
                        except Exception as e:
                            logger.error(f"Error creating service from data in {url}: {str(e)}")
                elif "service_id" in data or "name" in data:
                    # Single service in response
                    try:
                        service = self._create_service_from_data(data)
                        service.source = f"http:{url}"
                        self.notify_service_discovered(service)
                    except Exception as e:
                        logger.error(f"Error creating service from data in {url}: {str(e)}")
                else:
                    logger.warning(f"No service definitions found at {url}")
                
            except requests.RequestException as e:
                logger.error(f"Error fetching service definitions from {url}: {str(e)}")
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON from {url}: {str(e)}")
            except Exception as e:
                logger.error(f"Error processing service definitions from {url}: {str(e)}")
    
    def _poll_loop(self):
        """Polling loop for HTTP feed."""
        while self.running:
            try:
                # Sleep first to avoid hammering if refresh crashes
                time.sleep(self.poll_interval)
                
                if self.running:  # Check again after sleep
                    self.refresh()
            except Exception as e:
                logger.error(f"Error in HTTP feed poll loop: {str(e)}")
    
    def _create_service_from_data(self, data: Dict[str, Any]) -> ServiceMetadata:
        """
        Create a ServiceMetadata object from dictionary data.
        
        Args:
            data: Service data dictionary
            
        Returns:
            ServiceMetadata object
        """
        # Generate service ID if not provided
        service_id = data.get("service_id", data.get("id"))
        if not service_id:
            name = data.get("name", "unnamed")
            service_id = f"{name}-{uuid.uuid4()}"
        
        # Extract operations
        operations = data.get("operations", [])
        
        # Create service metadata
        service = ServiceMetadata(
            service_id=service_id,
            name=data.get("name", "unnamed"),
            description=data.get("description", ""),
            version=data.get("version", "1.0.0"),
            service_type=data.get("service_type", data.get("type", "unknown")),
            endpoints=data.get("endpoints", {}),
            operations=operations,
            tags=data.get("tags", []),
            metadata=data.get("metadata", {})
        )
        
        return service


class PluginDiscoveryFeed(DiscoveryFeed):
    """
    Plugin-based discovery feed.
    
    Discovers services by loading Python modules/plugins.
    """
    
    def __init__(self, name: str, plugin_dirs: List[str], 
                 pattern: str = "*.py", enabled: bool = True):
        """
        Initialize the plugin discovery feed.
        
        Args:
            name: Name of the feed
            plugin_dirs: List of directories to scan for plugins
            pattern: File pattern to match
            enabled: Whether the feed is enabled
        """
        super().__init__(name, enabled)
        self.plugin_dirs = plugin_dirs
        self.pattern = pattern
        self.loaded_plugins: Dict[str, Any] = {}
    
    def start(self):
        """Start the plugin discovery feed."""
        if not self.enabled:
            logger.info(f"Feed {self.name} is disabled")
            return
        
        # Load plugins
        self.refresh()
    
    def stop(self):
        """Stop the plugin discovery feed."""
        # Nothing to do for this feed
        pass
    
    def refresh(self):
        """
        Scan for and load plugins from the configured directories.
        
        This looks for Python modules that define service metadata.
        """
        for directory in self.plugin_dirs:
            if not os.path.exists(directory):
                logger.warning(f"Plugin directory does not exist: {directory}")
                continue
            
            try:
                # Find all matching Python files
                for path in Path(directory).glob(self.pattern):
                    path_str = str(path)
                    
                    # Skip already loaded plugins
                    if path_str in self.loaded_plugins:
                        continue
                    
                    # Load the module
                    try:
                        module_name = os.path.basename(path_str).replace('.py', '')
                        spec = importlib.util.spec_from_file_location(module_name, path_str)
                        if not spec or not spec.loader:
                            logger.error(f"Could not load plugin spec from {path_str}")
                            continue
                            
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                        
                        # Look for service metadata
                        if hasattr(module, 'SERVICE_METADATA'):
                            service_data = module.SERVICE_METADATA
                            service = self._create_service_from_data(service_data)
                            service.source = f"plugin:{os.path.basename(path_str)}"
                            self.notify_service_discovered(service)
                        
                        # Look for get_service_metadata function
                        elif hasattr(module, 'get_service_metadata') and callable(module.get_service_metadata):
                            service_data = module.get_service_metadata()
                            service = self._create_service_from_data(service_data)
                            service.source = f"plugin:{os.path.basename(path_str)}"
                            self.notify_service_discovered(service)
                        
                        # Look for service classes
                        else:
                            for name, obj in inspect.getmembers(module):
                                if (inspect.isclass(obj) and 
                                    hasattr(obj, 'get_service_metadata') and 
                                    callable(obj.get_service_metadata)):
                                    try:
                                        instance = obj()
                                        service_data = instance.get_service_metadata()
                                        service = self._create_service_from_data(service_data)
                                        service.source = f"plugin:{os.path.basename(path_str)}.{name}"
                                        self.notify_service_discovered(service)
                                    except Exception as e:
                                        logger.error(f"Error creating service from class {name} in {path_str}: {str(e)}")
                        
                        # Store loaded plugin
                        self.loaded_plugins[path_str] = module
                        
                    except Exception as e:
                        logger.error(f"Error loading plugin {path_str}: {str(e)}")
                
            except Exception as e:
                logger.error(f"Error in plugin discovery feed: {str(e)}")
    
    def _create_service_from_data(self, data: Dict[str, Any]) -> ServiceMetadata:
        """
        Create a ServiceMetadata object from dictionary data.
        
        Args:
            data: Service data dictionary
            
        Returns:
            ServiceMetadata object
        """
        # Generate service ID if not provided
        service_id = data.get("service_id", data.get("id"))
        if not service_id:
            name = data.get("name", "unnamed")
            service_id = f"{name}-{uuid.uuid4()}"
        
        # Extract operations
        operations = data.get("operations", [])
        
        # Create service metadata
        service = ServiceMetadata(
            service_id=service_id,
            name=data.get("name", "unnamed"),
            description=data.get("description", ""),
            version=data.get("version", "1.0.0"),
            service_type=data.get("service_type", data.get("type", "unknown")),
            endpoints=data.get("endpoints", {}),
            operations=operations,
            tags=data.get("tags", []),
            metadata=data.get("metadata", {})
        )
        
        return service


class FeedManager:
    """
    Manager for multiple discovery feeds.
    
    Coordinates multiple discovery feeds and routes discovered
    services to the registry.
    """
    
    def __init__(self, service_callback: Callable[[ServiceMetadata], None]):
        """
        Initialize the feed manager.
        
        Args:
            service_callback: Callback function when a service is discovered
        """
        self.feeds: Dict[str, DiscoveryFeed] = {}
        self.service_callback = service_callback
    
    def register_feed(self, feed: DiscoveryFeed):
        """
        Register a discovery feed.
        
        Args:
            feed: Discovery feed to register
        """
        feed.register_callback(self.service_callback)
        self.feeds[feed.name] = feed
        logger.info(f"Registered discovery feed: {feed.name}")
    
    def start_all(self):
        """Start all registered feeds."""
        for feed in self.feeds.values():
            try:
                feed.start()
            except Exception as e:
                logger.error(f"Error starting feed {feed.name}: {str(e)}")
    
    def stop_all(self):
        """Stop all registered feeds."""
        for feed in self.feeds.values():
            try:
                feed.stop()
            except Exception as e:
                logger.error(f"Error stopping feed {feed.name}: {str(e)}")
    
    def refresh_all(self):
        """Refresh all registered feeds."""
        for feed in self.feeds.values():
            try:
                feed.refresh()
            except Exception as e:
                logger.error(f"Error refreshing feed {feed.name}: {str(e)}")
    
    def get_feed(self, name: str) -> Optional[DiscoveryFeed]:
        """Get a feed by name."""
        return self.feeds.get(name)
    
    def get_feeds(self) -> List[str]:
        """Get list of all feed names."""
        return list(self.feeds.keys())


def create_feed_from_config(config: Dict[str, Any]) -> Optional[DiscoveryFeed]:
    """
    Create a discovery feed from configuration.
    
    Args:
        config: Feed configuration dictionary
        
    Returns:
        DiscoveryFeed instance or None if config is invalid
    """
    feed_type = config.get("type")
    name = config.get("name")
    enabled = config.get("enabled", True)
    
    if not feed_type or not name:
        logger.error("Feed configuration missing type or name")
        return None
    
    if feed_type == "file":
        directory = config.get("directory")
        if not directory:
            logger.error(f"File feed {name} missing directory")
            return None
        
        return FileSystemFeed(
            name=name,
            directory=directory,
            pattern=config.get("pattern", "*.{json,yaml,yml}"),
            poll_interval=config.get("poll_interval", 0),
            enabled=enabled
        )
    
    elif feed_type == "http":
        urls = config.get("urls", [])
        if not urls:
            logger.error(f"HTTP feed {name} missing URLs")
            return None
        
        return HttpDiscoveryFeed(
            name=name,
            urls=urls,
            poll_interval=config.get("poll_interval", 300),
            enabled=enabled
        )
    
    elif feed_type == "plugin":
        plugin_dirs = config.get("plugin_dirs", [])
        if not plugin_dirs:
            logger.error(f"Plugin feed {name} missing plugin_dirs")
            return None
        
        return PluginDiscoveryFeed(
            name=name,
            plugin_dirs=plugin_dirs,
            pattern=config.get("pattern", "*.py"),
            enabled=enabled
        )
    
    else:
        logger.error(f"Unknown feed type: {feed_type}")
        return None