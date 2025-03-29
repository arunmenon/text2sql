#!/usr/bin/env python3
"""
Run script for the Metadata Registry API Server.

This script provides a command-line interface to start the
Metadata Registry API Server, which allows services to
register themselves programmatically.
"""
import os
import sys
import argparse
import logging
from pathlib import Path

# Add the parent directory to the Python path
parent_dir = str(Path(__file__).parent.parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from src.mcp.registry.metadata_registry import get_metadata_registry
from src.mcp.registry.api import run_api

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for the Metadata Registry API Server."""
    parser = argparse.ArgumentParser(description="Metadata Registry API Server")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).parent / "config" / "registry.yaml"),
        help="Path to configuration file"
    )
    parser.add_argument(
        "--storage",
        default=str(Path(__file__).parent / "data"),
        help="Path to storage directory"
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind the server to"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8235,
        help="Port to bind the server to"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    
    args = parser.parse_args()
    
    # Set log level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create required directories
    os.makedirs(args.storage, exist_ok=True)
    services_dir = os.path.join(args.storage, "services")
    os.makedirs(services_dir, exist_ok=True)
    
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
    except Exception as e:
        logger.error(f"Failed to start Metadata Registry API Server: {str(e)}")
        sys.exit(1)
    finally:
        # Stop discovery
        registry.stop_discovery()

if __name__ == "__main__":
    main()