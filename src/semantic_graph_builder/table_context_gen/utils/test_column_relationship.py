"""
Test script for ColumnRelationshipProvider

This script tests the functionality of the ColumnRelationshipProvider
by connecting to Neo4j and retrieving relationship context for columns.
"""
import os
import logging
import asyncio
from typing import Dict, List, Any, Optional

from src.graph_storage.neo4j_client import Neo4jClient
from src.semantic_graph_builder.table_context_gen.utils.column_relationship_provider import ColumnRelationshipProvider

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_column_relationship_provider():
    """Test the ColumnRelationshipProvider with a Neo4j database."""
    try:
        # Get Neo4j connection details from environment variables
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Initialize Neo4j client
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        logger.info(f"Connected to Neo4j at {neo4j_uri}")
        
        # Create the ColumnRelationshipProvider
        provider = ColumnRelationshipProvider(neo4j_client)
        
        # Test with a specific tenant and table
        tenant_id = "walmart-stores"  # Replace with your actual tenant ID
        
        # Get tables for the tenant
        tables = neo4j_client.get_tables_for_tenant(tenant_id)
        
        if not tables:
            logger.warning(f"No tables found for tenant {tenant_id}")
            return
            
        logger.info(f"Found {len(tables)} tables for tenant {tenant_id}")
        
        # Test with a few tables and columns
        test_cases = [
            # Table name, column name
            ("Asset", "location_id"),     # Likely a foreign key
            ("WorkOrders", "asset_id"),   # Likely a foreign key
            ("Locations", "location_id"),  # Likely a primary key referenced by others
            ("Asset", "id"),              # Likely a primary key
        ]
        
        for table_name, column_name in test_cases:
            logger.info(f"\nTesting column relationships for {table_name}.{column_name}")
            
            # Get column relationship context
            context = provider.get_column_relationships(
                tenant_id=tenant_id, 
                table_name=table_name,
                column_name=column_name,
                include_table_info=True
            )
            
            # Log the result
            if context:
                logger.info(f"Column relationship context:\n{context}")
            else:
                logger.info(f"No relationship context found for {table_name}.{column_name}")
        
        # Test edge cases
        
        # 1. Non-existent column
        logger.info("\nTesting with non-existent column")
        context = provider.get_column_relationships(
            tenant_id=tenant_id, 
            table_name="Asset",
            column_name="nonexistent_column"
        )
        logger.info(f"Non-existent column result: {context}")
        
        # 2. Column with many relationships
        # Find a column with many relationships
        rich_relationship_column = find_column_with_most_relationships(neo4j_client, tenant_id)
        if rich_relationship_column:
            table_name, column_name = rich_relationship_column
            logger.info(f"\nTesting column with many relationships: {table_name}.{column_name}")
            context = provider.get_column_relationships(
                tenant_id=tenant_id, 
                table_name=table_name,
                column_name=column_name
            )
            logger.info(f"Rich relationship context:\n{context}")
            
            # Test with different max_relationships values
            for max_rels in [1, 3, 10]:
                context = provider.get_column_relationships(
                    tenant_id=tenant_id, 
                    table_name=table_name,
                    column_name=column_name,
                    max_relationships=max_rels
                )
                logger.info(f"Context with max_relationships={max_rels}:\n{context}")
        
        # Close Neo4j client
        neo4j_client.close()
        logger.info("Test completed successfully")
        
    except Exception as e:
        logger.error(f"Error testing ColumnRelationshipProvider: {e}", exc_info=True)

def find_column_with_most_relationships(neo4j_client: Neo4jClient, tenant_id: str) -> Optional[tuple]:
    """Find a column that participates in many relationships."""
    try:
        tables = neo4j_client.get_tables_for_tenant(tenant_id)
        
        max_relationships = 0
        max_relationship_column = None
        
        for table in tables:
            table_name = table.get("name")
            if not table_name:
                continue
                
            relationships = neo4j_client.get_relationships_for_table(tenant_id, table_name)
            
            # Count relationships per column
            column_counts = {}
            
            for rel in relationships:
                if not isinstance(rel, dict):
                    continue
                    
                if not all(k in rel for k in ['source', 'target']):
                    continue
                    
                source = rel['source']
                target = rel['target']
                
                if not (isinstance(source, dict) and isinstance(target, dict)):
                    continue
                
                source_table = source.get('table_name')
                source_col = source.get('name')
                target_table = target.get('table_name')
                target_col = target.get('name')
                
                # Count as a relationship for both source and target columns
                if source_table == table_name and source_col:
                    key = (source_table, source_col)
                    column_counts[key] = column_counts.get(key, 0) + 1
                
                if target_table == table_name and target_col:
                    key = (target_table, target_col)
                    column_counts[key] = column_counts.get(key, 0) + 1
            
            # Find the column with the most relationships in this table
            for column_key, count in column_counts.items():
                if count > max_relationships:
                    max_relationships = count
                    max_relationship_column = column_key
        
        return max_relationship_column
    except Exception as e:
        logger.error(f"Error finding column with most relationships: {e}")
        return None

if __name__ == "__main__":
    # Run the async test function
    asyncio.run(test_column_relationship_provider())