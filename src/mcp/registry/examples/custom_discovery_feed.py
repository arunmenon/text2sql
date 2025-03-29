#!/usr/bin/env python3
"""
Example of creating a custom discovery feed for the Metadata Registry.

This script demonstrates how to implement a custom discovery feed
that can find service definitions from alternative sources.
"""
import os
import sys
import time
import threading
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path

# Add the parent directory to the Python path
parent_dir = str(Path(__file__).parent.parent.parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from src.mcp.registry.feeds import DiscoveryFeed, ServiceMetadata
from src.mcp.registry.metadata_registry import get_metadata_registry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Sample service definitions for demonstration
SAMPLE_SERVICES = [
    {
        "service_id": "database-query-service",
        "name": "database_query",
        "description": "Service for executing database queries",
        "service_type": "database",
        "version": "1.0.0",
        "tags": ["database", "query", "sql"],
        "operations": [
            {
                "name": "execute_query",
                "description": "Execute a SQL query",
                "schema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string", 
                            "description": "SQL query to execute"
                        },
                        "params": {
                            "type": "object",
                            "description": "Query parameters"
                        }
                    },
                    "required": ["query"]
                }
            }
        ]
    },
    {
        "service_id": "file-storage-service",
        "name": "file_storage",
        "description": "Service for managing file storage",
        "service_type": "storage",
        "version": "1.0.0",
        "tags": ["storage", "file", "document"],
        "operations": [
            {
                "name": "upload_file",
                "description": "Upload a file to storage",
                "schema": {
                    "type": "object",
                    "properties": {
                        "file_data": {
                            "type": "string",
                            "description": "Base64 encoded file data"
                        },
                        "file_name": {
                            "type": "string",
                            "description": "Name of the file"
                        },
                        "content_type": {
                            "type": "string",
                            "description": "Content type of the file"
                        }
                    },
                    "required": ["file_data", "file_name"]
                }
            },
            {
                "name": "download_file",
                "description": "Download a file from storage",
                "schema": {
                    "type": "object",
                    "properties": {
                        "file_id": {
                            "type": "string",
                            "description": "ID of the file to download"
                        }
                    },
                    "required": ["file_id"]
                }
            }
        ]
    }
]

class CustomDiscoveryFeed(DiscoveryFeed):
    """
    Custom discovery feed for demonstration purposes.
    
    This feed simulates discovering services from an external source,
    such as a database, API, or service mesh.
    """
    
    def __init__(self, name: str, discovery_interval: int = 60, enabled: bool = True):
        """
        Initialize the custom discovery feed.
        
        Args:
            name: Name of the feed
            discovery_interval: Interval (in seconds) for service discovery
            enabled: Whether the feed is enabled
        """
        super().__init__(name, enabled)
        self.discovery_interval = discovery_interval
        self.running = False
        self.discovery_thread = None
        
        # In a real implementation, this might be a connection to an external system
        self.external_source = SAMPLE_SERVICES
    
    def start(self):
        """Start the discovery feed."""
        if not self.enabled:
            logger.info(f"Feed {self.name} is disabled")
            return
        
        # Perform initial discovery
        self.refresh()
        
        # Start discovery thread
        self.running = True
        self.discovery_thread = threading.Thread(
            target=self._discovery_loop,
            daemon=True
        )
        self.discovery_thread.start()
        logger.info(f"Started custom discovery feed {self.name} with interval {self.discovery_interval}s")
    
    def stop(self):
        """Stop the discovery feed."""
        self.running = False
        if self.discovery_thread:
            self.discovery_thread.join(timeout=5)
            self.discovery_thread = None
    
    def refresh(self):
        """
        Refresh service discoveries from the external source.
        
        In a real implementation, this would query an external system
        for service definitions, such as a service mesh registry,
        database, or API.
        """
        try:
            # In a real implementation, query external source for services
            # For demo, we'll use the sample services
            services = self.external_source
            
            logger.info(f"Discovered {len(services)} services from custom source")
            
            # Notify about each discovered service
            for service_data in services:
                logger.info(f"Found service: {service_data['name']} ({service_data['service_id']})")
                
                # Create service metadata
                service = ServiceMetadata(
                    service_id=service_data["service_id"],
                    name=service_data["name"],
                    description=service_data.get("description", ""),
                    version=service_data.get("version", "1.0.0"),
                    service_type=service_data.get("service_type", "unknown"),
                    operations=service_data.get("operations", []),
                    tags=service_data.get("tags", []),
                    source=f"custom:{self.name}"
                )
                
                # Notify callbacks
                self.notify_service_discovered(service)
        
        except Exception as e:
            logger.error(f"Error in custom discovery feed refresh: {str(e)}")
    
    def _discovery_loop(self):
        """Discovery loop for the feed."""
        while self.running:
            try:
                # Sleep first to avoid immediate re-discovery
                time.sleep(self.discovery_interval)
                
                if self.running:  # Check again after sleep
                    logger.info(f"Running scheduled discovery for {self.name}")
                    self.refresh()
            except Exception as e:
                logger.error(f"Error in discovery loop: {str(e)}")

def main():
    """Main function to demonstrate custom discovery feed."""
    parser = argparse.ArgumentParser(description="Custom Discovery Feed Example")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).parent.parent / "config" / "registry.yaml"),
        help="Path to configuration file"
    )
    parser.add_argument(
        "--storage",
        default=str(Path(__file__).parent.parent / "data"),
        help="Path to storage directory"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Discovery interval in seconds"
    )
    parser.add_argument(
        "--run-time",
        type=int,
        default=120,
        help="Time to run the example (in seconds)"
    )
    
    args = parser.parse_args()
    
    # Initialize the registry
    registry = get_metadata_registry(args.config, args.storage)
    
    # Start built-in discovery
    registry.start_discovery()
    
    try:
        # Create custom discovery feed
        custom_feed = CustomDiscoveryFeed(
            name="example_custom_feed",
            discovery_interval=args.interval
        )
        
        # Register feed with registry
        if registry.feeds:
            registry.feeds.register_feed(custom_feed)
            
            # Start the feed
            custom_feed.start()
            
            logger.info(f"Custom discovery feed registered and started")
            logger.info(f"Running for {args.run_time} seconds...")
            
            # Wait for specified time
            time.sleep(args.run_time)
            
            # Show discovered services
            logger.info(f"Discovered services: {registry.get_service_count()}")
            for service_id, service in registry.services.items():
                logger.info(f"- {service.name} ({service_id}) [source: {service.source}]")
        
        else:
            logger.error("Registry feeds not initialized")
    
    finally:
        # Stop discovery
        registry.stop_discovery()
        logger.info("Discovery stopped")

if __name__ == "__main__":
    main()