"""
Test script for TableNeighborhoodProvider

This script tests the functionality of the TableNeighborhoodProvider
by connecting to Neo4j and retrieving relationship context for tables.
"""
import os
import logging
import asyncio
from typing import Dict, List, Any, Optional

from src.graph_storage.neo4j_client import Neo4jClient
from src.semantic_graph_builder.table_context_gen.utils.table_neighborhood_provider import TableNeighborhoodProvider

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_table_neighborhood_provider():
    """Test the TableNeighborhoodProvider with a Neo4j database."""
    try:
        # Get Neo4j connection details from environment variables
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Initialize Neo4j client
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        logger.info(f"Connected to Neo4j at {neo4j_uri}")
        
        # Create the TableNeighborhoodProvider
        provider = TableNeighborhoodProvider(neo4j_client)
        
        # Test with a specific tenant and table
        tenant_id = "walmart-stores"  # Using known tenant ID from the database
        
        # Get all tables for the tenant
        tables = neo4j_client.get_tables_for_tenant(tenant_id)
        
        if not tables:
            logger.warning(f"No tables found for tenant {tenant_id}")
            return
            
        logger.info(f"Found {len(tables)} tables for tenant {tenant_id}")
        
        # Test with a few tables
        for table in tables[:3]:  # Test with up to 3 tables
            table_name = table.get("name")
            if not table_name:
                continue
                
            logger.info(f"\nTesting neighborhood for table: {table_name}")
            
            # Get table neighborhood context
            context = provider.get_table_neighborhood(tenant_id, table_name)
            
            # Log the result
            if context:
                logger.info(f"Table neighborhood context for {table_name}:\n{context}")
            else:
                logger.info(f"No neighborhood context found for {table_name}")
            
            # Get raw relationships for comparison
            relationships = neo4j_client.get_relationships_for_table(tenant_id, table_name)
            logger.info(f"Found {len(relationships)} relationships for {table_name}")
        
        # Test with a table that has many relationships (if you know one)
        rich_relationship_table = find_table_with_most_relationships(neo4j_client, tenant_id)
        if rich_relationship_table:
            logger.info(f"\nTesting table with many relationships: {rich_relationship_table}")
            context = provider.get_table_neighborhood(tenant_id, rich_relationship_table)
            logger.info(f"Rich neighborhood context:\n{context}")
        
        # Test with a table that has no relationships
        no_relationship_table = find_table_with_no_relationships(neo4j_client, tenant_id)
        if no_relationship_table:
            logger.info(f"\nTesting table with no relationships: {no_relationship_table}")
            context = provider.get_table_neighborhood(tenant_id, no_relationship_table)
            logger.info(f"No relationship context:\n{context}")
        
        # Test with invalid table name
        logger.info("\nTesting with invalid table name")
        context = provider.get_table_neighborhood(tenant_id, "nonexistent_table")
        logger.info(f"Invalid table context result: {context!r}")
        
        # Test with different max_tables values
        if rich_relationship_table:
            logger.info(f"\nTesting with different max_tables values for {rich_relationship_table}")
            for max_tables in [1, 3, 10]:
                context = provider.get_table_neighborhood(tenant_id, rich_relationship_table, max_tables)
                logger.info(f"Context with max_tables={max_tables}:\n{context}")
            
            # Test with descriptions included
            logger.info(f"\nTesting with descriptions for {rich_relationship_table}")
            context = provider.get_table_neighborhood(
                tenant_id, 
                rich_relationship_table, 
                max_tables=3, 
                max_relationships_per_table=2,
                include_descriptions=True
            )
            logger.info(f"Context with descriptions:\n{context}")
        
        # Close Neo4j client
        neo4j_client.close()
        logger.info("Test completed successfully")
        
    except Exception as e:
        logger.error(f"Error testing TableNeighborhoodProvider: {e}", exc_info=True)

def find_table_with_most_relationships(neo4j_client: Neo4jClient, tenant_id: str) -> Optional[str]:
    """Find a table with the most relationships."""
    try:
        tables = neo4j_client.get_tables_for_tenant(tenant_id)
        
        max_relationships = 0
        max_relationship_table = None
        
        for table in tables:
            table_name = table.get("name")
            if not table_name:
                continue
                
            relationships = neo4j_client.get_relationships_for_table(tenant_id, table_name)
            
            if len(relationships) > max_relationships:
                max_relationships = len(relationships)
                max_relationship_table = table_name
        
        return max_relationship_table
    except Exception as e:
        logger.error(f"Error finding table with most relationships: {e}")
        return None

def find_table_with_no_relationships(neo4j_client: Neo4jClient, tenant_id: str) -> Optional[str]:
    """Find a table with no relationships."""
    try:
        tables = neo4j_client.get_tables_for_tenant(tenant_id)
        
        for table in tables:
            table_name = table.get("name")
            if not table_name:
                continue
                
            relationships = neo4j_client.get_relationships_for_table(tenant_id, table_name)
            
            if not relationships:
                return table_name
        
        return None
    except Exception as e:
        logger.error(f"Error finding table with no relationships: {e}")
        return None

if __name__ == "__main__":
    # Run the async test function
    asyncio.run(test_table_neighborhood_provider())