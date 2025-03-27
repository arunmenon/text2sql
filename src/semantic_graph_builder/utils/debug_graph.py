#!/usr/bin/env python3
"""
Neo4j Graph Debugging Utility

Simple utility to check graph structure and tenant data.

Usage:
    python -m src.semantic_graph_builder.utils.debug_graph --tenant-id tenant-name
"""

import os
import sys
import argparse
import logging
import asyncio
from dotenv import load_dotenv

# Import Neo4j client
from src.graph_storage.neo4j_client import Neo4jClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def check_tenant_data(neo4j_client, tenant_id=None):
    """Check data availability for a tenant."""
    
    # Check if tenant exists
    if tenant_id:
        query = """
        MATCH (t:Tenant {id: $tenant_id})
        RETURN t
        """
        result = neo4j_client._execute_query(query, {"tenant_id": tenant_id})
        if not result:
            logger.error(f"Tenant '{tenant_id}' not found in the database.")
            return False
        logger.info(f"Tenant '{tenant_id}' exists in the database.")
    
    # List all tenants
    query = """
    MATCH (t:Tenant)
    RETURN t.id as id, count{(t)-->()} as connection_count
    """
    result = neo4j_client._execute_query(query)
    logger.info(f"Found {len(result)} tenants in the database:")
    for r in result:
        logger.info(f"  - {r['id']} (has {r['connection_count']} outgoing connections)")
    
    # Get node counts by label
    if tenant_id:
        query = """
        MATCH (n)
        WHERE n.tenant_id = $tenant_id
        RETURN labels(n) as labels, count(n) as count
        """
        result = neo4j_client._execute_query(query, {"tenant_id": tenant_id})
    else:
        query = """
        MATCH (n)
        RETURN labels(n) as labels, count(n) as count
        """
        result = neo4j_client._execute_query(query)
    
    logger.info("Node counts by label:")
    for r in result:
        label_str = ":".join(r["labels"]) if isinstance(r["labels"], list) else r["labels"]
        logger.info(f"  - {label_str}: {r['count']}")
    
    # Get relationship counts by type
    if tenant_id:
        query = """
        MATCH (n)-[r]->(m)
        WHERE n.tenant_id = $tenant_id AND m.tenant_id = $tenant_id
        RETURN type(r) as type, count(r) as count
        """
        result = neo4j_client._execute_query(query, {"tenant_id": tenant_id})
    else:
        query = """
        MATCH ()-[r]->()
        RETURN type(r) as type, count(r) as count
        """
        result = neo4j_client._execute_query(query)
    
    logger.info("Relationship counts by type:")
    for r in result:
        logger.info(f"  - {r['type']}: {r['count']}")
    
    # Check for any data
    if tenant_id:
        query = """
        MATCH (n)
        WHERE n.tenant_id = $tenant_id
        RETURN count(n) > 0 as has_data
        """
        result = neo4j_client._execute_query(query, {"tenant_id": tenant_id})
        if result and result[0].get("has_data"):
            logger.info(f"Tenant '{tenant_id}' has data in the database.")
            return True
        else:
            logger.warning(f"Tenant '{tenant_id}' exists but has no data.")
            return False
    
    return True

async def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Debug Neo4j graph structure")
    
    parser.add_argument("--tenant-id", help="Tenant ID to check")
    
    parser.add_argument(
        "--uri", 
        default=os.getenv("NEO4J_URI", "neo4j://localhost:7687"),
        help="Neo4j connection URI"
    )
    
    parser.add_argument(
        "--user", 
        default=os.getenv("NEO4J_USERNAME", "neo4j"),
        help="Neo4j username"
    )
    
    parser.add_argument(
        "--password", 
        default=os.getenv("NEO4J_PASSWORD", "password"),
        help="Neo4j password"
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize Neo4j client
        neo4j_client = Neo4jClient(args.uri, args.user, args.password)
        
        # Check tenant data
        success = check_tenant_data(neo4j_client, args.tenant_id)
        
        # Close Neo4j client
        neo4j_client.close()
        
        return 0 if success else 1
        
    except Exception as e:
        logger.error(f"Error checking graph: {e}")
        return 1

if __name__ == "__main__":
    asyncio.run(main())