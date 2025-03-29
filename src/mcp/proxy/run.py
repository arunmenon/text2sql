#!/usr/bin/env python3
"""
Run script for the MCP Proxy.

This script provides a command-line interface to start the MCP Proxy.
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

from src.mcp.proxy.core import MCPProxy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for the MCP Proxy."""
    parser = argparse.ArgumentParser(description="MCP Proxy Server")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).parent / "config.yaml"),
        help="Path to configuration file"
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
        help="Host to bind the server to (overrides config)"
    )
    parser.add_argument(
        "--port",
        type=int,
        help="Port to bind the server to (overrides config)"
    )
    
    args = parser.parse_args()
    
    # Set environment variables from command-line arguments
    if args.neo4j_uri:
        os.environ["NEO4J_URI"] = args.neo4j_uri
    if args.neo4j_user:
        os.environ["NEO4J_USER"] = args.neo4j_user
    if args.neo4j_password:
        os.environ["NEO4J_PASSWORD"] = args.neo4j_password
    
    # Create and run the proxy
    try:
        proxy = MCPProxy(args.config)
        
        # Override host/port if provided
        if args.host:
            proxy.config["server"]["host"] = args.host
        if args.port:
            proxy.config["server"]["port"] = args.port
        
        proxy.run()
    except Exception as e:
        logger.error(f"Failed to start MCP Proxy: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()