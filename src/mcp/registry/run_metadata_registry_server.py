#!/usr/bin/env python3
"""
Run script for the Metadata Registry with MCP.

This script provides a command-line interface to start the
Metadata Registry with MCP protocol support.
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
from src.mcp.registry.metadata_registry_server import run_metadata_registry_server

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for the Metadata Registry MCP Server."""
    parser = argparse.ArgumentParser(description="Metadata Registry MCP Server")
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
        "--neo4j-uri",
        help="Neo4j connection URI (overrides config)"
    )
    parser.add_argument(
        "--neo4j-user",
        help="Neo4j username (overrides config)"
    )
    parser.add_argument(
        "--neo4j-password",
        help="Neo4j password (overrides config)"
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind the server to"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8234,
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
    
    # Set environment variables from command-line arguments
    if args.neo4j_uri:
        os.environ["NEO4J_URI"] = args.neo4j_uri
    if args.neo4j_user:
        os.environ["NEO4J_USER"] = args.neo4j_user
    if args.neo4j_password:
        os.environ["NEO4J_PASSWORD"] = args.neo4j_password
    
    # Create required directories
    os.makedirs(args.storage, exist_ok=True)
    services_dir = os.path.join(args.storage, "services")
    os.makedirs(services_dir, exist_ok=True)
    
    # Run the server
    try:
        logger.info(f"Starting Metadata Registry MCP Server on {args.host}:{args.port}")
        run_metadata_registry_server(
            config_path=args.config,
            storage_path=args.storage,
            host=args.host,
            port=args.port
        )
    except Exception as e:
        logger.error(f"Failed to start Metadata Registry MCP Server: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()