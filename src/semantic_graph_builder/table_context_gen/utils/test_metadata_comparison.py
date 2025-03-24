"""
Test script to compare old table metadata with new enhanced metadata.

This script compares table descriptions before and after applying
the relationship context enhancement.
"""
import os
import logging
import asyncio
from typing import Dict, List, Any, Optional

from src.llm.client import LLMClient
from src.graph_storage.neo4j_client import Neo4jClient
from src.semantic_graph_builder.table_context_gen.utils.table_neighborhood_provider import TableNeighborhoodProvider
from src.semantic_graph_builder.utils.prompt_loader import PromptLoader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def compare_table_descriptions(tenant_id: str, table_name: str):
    """Compare old and new table descriptions."""
    try:
        # Get Neo4j connection details
        neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Get LLM API details
        llm_api_key = os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY")
        llm_model = os.getenv("LLM_MODEL", "gpt-4o")
        
        if not llm_api_key:
            logger.error("LLM API key not configured")
            return
        
        # Initialize clients
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        llm_client = LLMClient(api_key=llm_api_key, model=llm_model)
        
        # Initialize the prompt loader and neighborhood provider
        prompt_loader = PromptLoader()
        neighborhood_provider = TableNeighborhoodProvider(neo4j_client)
        
        # Get current table data
        table_query = """
        MATCH (t:Table {tenant_id: $tenant_id, name: $table_name})
        RETURN t
        """
        table_result = neo4j_client._execute_query(table_query, {
            "tenant_id": tenant_id,
            "table_name": table_name
        })
        
        if not table_result:
            logger.error(f"Table {table_name} not found for tenant {tenant_id}")
            return
            
        table = table_result[0]["t"]
        
        # Get current description
        current_description = table.get("description", "No description available")
        
        # Get columns for context
        columns = neo4j_client.get_columns_for_table(tenant_id, table_name)
        
        logger.info(f"Found {len(columns)} columns for table {table_name}")
        
        # Format columns for prompt
        columns_text = "\n".join([
            f"- {col.get('name') or col.get('column_name')}: {col.get('data_type')} - {col.get('description', 'No description')}"
            for col in columns if col.get('name') or col.get('column_name')
        ])
        
        # Get table neighborhood context
        table_relationships = neighborhood_provider.get_table_neighborhood(
            tenant_id=tenant_id, 
            table_name=table_name,
            max_tables=5,
            max_relationships_per_table=3,
            include_descriptions=True
        )
        
        if not table_relationships:
            table_relationships = "No direct relationships with other tables detected."
        
        # Create prompt for generating new description
        prompt_vars = {
            "table_name": table_name,
            "original_description": current_description,
            "columns_text": columns_text,
            "sample_data": "",  # We don't have sample data for this test
            "table_relationships": table_relationships
        }
        
        prompt = prompt_loader.format_prompt("table_description_enhancement", **prompt_vars)
        
        # Generate new description using LLM
        logger.info("Generating new description...")
        new_description = await llm_client.generate(prompt)
        
        # Display comparison
        logger.info(f"\n{'='*50}\nTABLE: {table_name}\n{'='*50}")
        
        logger.info(f"\nCURRENT DESCRIPTION:\n{'-'*50}\n{current_description}\n{'-'*50}")
        
        logger.info(f"\nRELATIONSHIP CONTEXT USED:\n{'-'*50}\n{table_relationships}\n{'-'*50}")
        
        logger.info(f"\nNEW ENHANCED DESCRIPTION:\n{'-'*50}\n{new_description}\n{'-'*50}")
        
        # Close clients
        neo4j_client.close()
        await llm_client.close()
        
    except Exception as e:
        logger.error(f"Error comparing table descriptions: {e}", exc_info=True)

async def compare_multiple_tables(tenant_id: str, num_tables: int = 3):
    """Compare descriptions for multiple tables."""
    try:
        # Connect to Neo4j
        neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        
        # Get tables
        tables = neo4j_client.get_tables_for_tenant(tenant_id)
        if not tables:
            logger.error(f"No tables found for tenant {tenant_id}")
            neo4j_client.close()
            return
            
        logger.info(f"Found {len(tables)} tables for tenant {tenant_id}")
        
        # Choose a subset of tables (prefer tables with existing descriptions if available)
        tables_with_descriptions = [t for t in tables if t.get("description") and len(t.get("description", "")) > 50]
        
        if len(tables_with_descriptions) >= num_tables:
            selected_tables = tables_with_descriptions[:num_tables]
        else:
            # If not enough tables with good descriptions, include some without
            remaining_needed = num_tables - len(tables_with_descriptions)
            tables_without_descriptions = [t for t in tables if t not in tables_with_descriptions]
            selected_tables = tables_with_descriptions + tables_without_descriptions[:remaining_needed]
        
        # Close Neo4j client
        neo4j_client.close()
        
        # Compare descriptions for each selected table
        for table in selected_tables:
            table_name = table.get("name")
            await compare_table_descriptions(tenant_id, table_name)
        
    except Exception as e:
        logger.error(f"Error comparing multiple tables: {e}", exc_info=True)

if __name__ == "__main__":
    # Run with the tenant ID from the test
    tenant_id = "walmart-stores"
    
    # Option 1: Compare a specific table
    # asyncio.run(compare_table_descriptions(tenant_id, "WorkOrders"))
    
    # Option 2: Compare multiple tables
    asyncio.run(compare_multiple_tables(tenant_id, 2))