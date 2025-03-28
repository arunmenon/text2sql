"""
MCP Server for Knowledge Graph

This module implements an MCP server that exposes Knowledge Graph
functionality as MCP resources and operations.
"""
import argparse
import logging
import os
from typing import Dict, Any

from mcp_server import Server
from neo4j import GraphDatabase

from src.graph_storage.neo4j_client import Neo4jClient
from src.mcp.kg_resource import KnowledgeGraphResource

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_server(config: Dict[str, Any] = None) -> Server:
    """
    Create and configure the MCP server.
    
    Args:
        config: Optional configuration dictionary
        
    Returns:
        Configured MCP server instance
    """
    if config is None:
        config = {}
    
    # Get Neo4j connection details from config or environment
    neo4j_uri = config.get('neo4j_uri', os.environ.get('NEO4J_URI', 'neo4j://localhost:7687'))
    neo4j_user = config.get('neo4j_user', os.environ.get('NEO4J_USER', 'neo4j'))
    neo4j_password = config.get('neo4j_password', os.environ.get('NEO4J_PASSWORD', 'password'))
    
    # Initialize Neo4j client
    try:
        logger.info(f"Connecting to Neo4j at {neo4j_uri}")
        neo4j_client = Neo4jClient(
            uri=neo4j_uri,
            username=neo4j_user,
            password=neo4j_password
        )
        logger.info("Connected to Neo4j successfully")
    except Exception as e:
        logger.error(f"Failed to connect to Neo4j: {str(e)}")
        raise
    
    # Create Knowledge Graph resource
    kg_resource = KnowledgeGraphResource(neo4j_client)
    
    # Create server with resources
    server = Server(resources=[kg_resource.get_resource()])
    
    return server


def main():
    """
    Run the MCP server as a standalone application.
    """
    parser = argparse.ArgumentParser(description="Knowledge Graph MCP Server")
    parser.add_argument("--host", default="0.0.0.0", help="Server host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8234, help="Server port (default: 8234)")
    parser.add_argument("--neo4j-uri", help="Neo4j connection URI")
    parser.add_argument("--neo4j-user", help="Neo4j username")
    parser.add_argument("--neo4j-password", help="Neo4j password")
    
    args = parser.parse_args()
    
    # Create config from args
    config = {
        'neo4j_uri': args.neo4j_uri,
        'neo4j_user': args.neo4j_user,
        'neo4j_password': args.neo4j_password
    }
    # Remove None values
    config = {k: v for k, v in config.items() if v is not None}
    
    try:
        # Create and run server
        server = create_server(config)
        logger.info(f"Starting MCP server on {args.host}:{args.port}")
        server.run(host=args.host, port=args.port)
    except Exception as e:
        logger.error(f"Failed to start server: {str(e)}")


if __name__ == "__main__":
    main()