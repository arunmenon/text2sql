#!/usr/bin/env python3
"""
Generate Concept Tags

This script generates concept tags for tables and columns using LLM
and outputs the result to the terminal.

Usage:
    python generate_concept_tags.py --tenant-id YOUR_TENANT_ID --api-key YOUR_API_KEY
"""
import argparse
import asyncio
import logging
import os
import json
import sys
from dotenv import load_dotenv

from src.graph_storage.neo4j_client import Neo4jClient
from src.llm.client import LLMClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout  # Output to stdout
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

async def generate_concept_tags(tenant_id, api_key=None):
    """Generate concept tags for tables and columns using LLM."""
    try:
        # Get Neo4j connection details
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Get LLM API details
        llm_api_key = api_key or os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY")
        llm_model = os.getenv("LLM_MODEL", "gpt-4o")
        
        if not llm_api_key:
            logger.error("No LLM API key found. Please provide an API key.")
            return False
        
        # Initialize clients
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        llm_client = LLMClient(api_key=llm_api_key, model=llm_model)
        
        # Get tables for this tenant
        tables = neo4j_client.get_tables_for_tenant(tenant_id)
        if not tables:
            logger.error(f"No tables found for tenant {tenant_id}")
            return False
            
        logger.info(f"Found {len(tables)} tables to analyze")
        
        # Process all tables
        for table in tables:
            table_name = table.get("name")
            if not table_name:
                continue
                
            # Get columns for context
            columns = neo4j_client.get_columns_for_table(tenant_id, table_name)
            
            # Create prompt for concept tagging
            prompt = f"""
            Analyze this database table and its columns to generate concept tags:
            
            Table: {table_name}
            Description: {table.get('description', 'No description')}
            
            Columns:
            {json.dumps([{
                "name": col.get('name') or col.get('column_name', ''),
                "data_type": col.get('data_type', 'unknown'),
                "description": col.get('description', 'No description')
            } for col in columns if col.get('name') or col.get('column_name')], indent=2)}
            
            For this table and its columns, provide:
            1. A list of business concept tags for the table itself (e.g., Transaction, Customer Profile, Product Catalog)
            2. For each column, assign concept tags that represent the semantic meaning (e.g., Personal Identifier, Timestamp, Amount, Status)
            
            Format your response as a structured JSON with table_tags and column_tags.
            """
            
            logger.info(f"Generating concept tags for table: {table_name}")
            
            # Generate tags
            try:
                schema = {
                    "table_tags": ["string"],
                    "column_tags": [
                        {
                            "column_name": "string",
                            "tags": ["string"]
                        }
                    ]
                }
                
                response = await llm_client.generate_structured_output(prompt, schema)
                
                # Print the results
                print(f"\n========== CONCEPT TAGS FOR TABLE: {table_name} ==========\n")
                print("Table Tags:")
                for tag in response.get("table_tags", []):
                    print(f"  - {tag}")
                
                print("\nColumn Tags:")
                for col_tag in response.get("column_tags", []):
                    col_name = col_tag.get("column_name")
                    tags = col_tag.get("tags", [])
                    print(f"  {col_name}:")
                    for tag in tags:
                        print(f"    - {tag}")
                
                print("\n================================================\n")
                
                # Store table tags in Neo4j
                table_tags = response.get("table_tags", [])
                if table_tags:
                    # Update table metadata with concept tags
                    query = """
                    MATCH (t:Table {tenant_id: $tenant_id, name: $table_name})
                    SET t.concept_tags = $concept_tags
                    RETURN t
                    """
                    
                    params = {
                        "tenant_id": tenant_id,
                        "table_name": table_name,
                        "concept_tags": table_tags
                    }
                    
                    neo4j_client._execute_query(query, params)
                    logger.info(f"Stored concept tags for table: {table_name}")
                
                # Store column tags in Neo4j
                for col_tag in response.get("column_tags", []):
                    col_name = col_tag.get("column_name")
                    tags = col_tag.get("tags", [])
                    
                    if col_name and tags:
                        # Update column metadata with concept tags
                        query = """
                        MATCH (c:Column {tenant_id: $tenant_id, table_name: $table_name, name: $column_name})
                        SET c.concept_tags = $concept_tags
                        RETURN c
                        """
                        
                        params = {
                            "tenant_id": tenant_id,
                            "table_name": table_name,
                            "column_name": col_name,
                            "concept_tags": tags
                        }
                        
                        neo4j_client._execute_query(query, params)
                        logger.info(f"Stored concept tags for column: {table_name}.{col_name}")
                    
            except Exception as e:
                logger.error(f"Error generating concept tags for {table_name}: {e}")
        
        # Close clients
        neo4j_client.close()
        await llm_client.close()
        
        logger.info("Concept tag generation completed")
        return True
        
    except Exception as e:
        logger.error(f"Error generating concept tags: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate concept tags")
    parser.add_argument("--tenant-id", required=True, help="Tenant ID")
    parser.add_argument("--api-key", help="OpenAI API key")
    
    args = parser.parse_args()
    
    # Run the process
    success = asyncio.run(generate_concept_tags(args.tenant_id, args.api_key))
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)