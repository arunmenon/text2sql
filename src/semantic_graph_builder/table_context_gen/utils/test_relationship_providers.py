"""
Test Relationship Providers

This script tests both TableNeighborhoodProvider and ColumnRelationshipProvider
to demonstrate how they enhance context for metadata descriptions.
"""
import os
import logging
import asyncio
from typing import Dict, List, Any

from src.graph_storage.neo4j_client import Neo4jClient
from src.semantic_graph_builder.table_context_gen.utils.table_neighborhood_provider import TableNeighborhoodProvider
from src.semantic_graph_builder.table_context_gen.utils.column_relationship_provider import ColumnRelationshipProvider

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_relationship_providers():
    """Test both relationship providers to show their context enhancement."""
    try:
        # Get Neo4j connection details from environment variables
        neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Initialize Neo4j client
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        logger.info(f"Connected to Neo4j at {neo4j_uri}")
        
        # Create the providers
        table_provider = TableNeighborhoodProvider(neo4j_client)
        column_provider = ColumnRelationshipProvider(neo4j_client)
        
        # Test with a specific tenant
        tenant_id = "walmart-stores"
        
        # Test cases
        table_test_cases = [
            "Asset",
            "Locations",
            "WorkOrders"
        ]
        
        column_test_cases = [
            ("Asset", "location_id"),
            ("Locations", "location_id"),
            ("Asset", "store_type")
        ]
        
        # Test TableNeighborhoodProvider
        logger.info("\n=== TABLE NEIGHBORHOOD PROVIDER TEST ===\n")
        
        for table_name in table_test_cases:
            logger.info(f"\nTable Neighborhood for {table_name}:")
            
            # Get table relationships with descriptions
            context = table_provider.get_table_neighborhood(
                tenant_id=tenant_id,
                table_name=table_name,
                max_tables=5,
                max_relationships_per_table=3,
                include_descriptions=True
            )
            
            logger.info(context)
        
        # Test ColumnRelationshipProvider
        logger.info("\n=== COLUMN RELATIONSHIP PROVIDER TEST ===\n")
        
        for table_name, column_name in column_test_cases:
            logger.info(f"\nColumn Relationships for {table_name}.{column_name}:")
            
            # Get column relationships
            context = column_provider.get_column_relationships(
                tenant_id=tenant_id,
                table_name=table_name,
                column_name=column_name,
                include_table_info=True,
                max_relationships=5
            )
            
            logger.info(context)
        
        # Show sample combined use
        logger.info("\n=== COMBINED USE CASE EXAMPLE ===\n")
        
        # Choose a table and column to enhance
        example_table = "Asset"
        example_column = "location_id"
        
        # Get table context
        table_context = table_provider.get_table_neighborhood(
            tenant_id=tenant_id,
            table_name=example_table,
            include_descriptions=True
        )
        
        # Get column context
        column_context = column_provider.get_column_relationships(
            tenant_id=tenant_id,
            table_name=example_table,
            column_name=example_column,
            include_table_info=True
        )
        
        logger.info(f"Example of combined context for enhancing {example_table}.{example_column}:")
        logger.info(f"\nTABLE RELATIONSHIP CONTEXT:\n{table_context}")
        logger.info(f"\nCOLUMN RELATIONSHIP CONTEXT:\n{column_context}")
        
        # Close Neo4j client
        neo4j_client.close()
        logger.info("\nTest completed successfully")
        
    except Exception as e:
        logger.error(f"Error testing relationship providers: {e}", exc_info=True)

if __name__ == "__main__":
    # Run the async test function
    asyncio.run(test_relationship_providers())