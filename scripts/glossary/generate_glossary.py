#!/usr/bin/env python3
"""
Generate Business Glossary

This script specifically generates a business glossary using LLM
and outputs the result to the terminal.

Usage:
    python generate_glossary.py --tenant-id YOUR_TENANT_ID --api-key YOUR_API_KEY
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

async def generate_business_glossary(tenant_id, api_key=None):
    """Generate a business glossary using LLM."""
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
        
        # Get dataset_id from Neo4j
        datasets = neo4j_client.get_datasets_for_tenant(tenant_id)
        if not datasets:
            logger.error(f"No datasets found for tenant {tenant_id}")
            return False
        
        dataset_id = datasets[0]["name"]
        logger.info(f"Found dataset: {dataset_id}")
        
        # Get tables for this tenant
        tables = neo4j_client.get_tables_for_tenant(tenant_id)
        if not tables:
            logger.error(f"No tables found for tenant {tenant_id}")
            return False
            
        logger.info(f"Found {len(tables)} tables to analyze")
        
        # Extract all table and column names for context
        table_info = []
        for table in tables:
            table_name = table.get("name")
            if not table_name:
                continue
                
            description = table.get("description", "No description")
            columns = neo4j_client.get_columns_for_table(tenant_id, table_name)
            column_info = [f"{col.get('name') or col.get('column_name')} ({col.get('data_type', 'unknown')})" 
                          for col in columns if col.get('name') or col.get('column_name')]
            
            table_info.append({
                "table": table_name,
                "description": description,
                "columns": column_info
            })
        
        # Create prompt for glossary generation
        prompt = f"""
        Analyze the following database schema and generate a comprehensive business glossary.
        
        Database schema:
        {json.dumps(table_info, indent=2)}
        
        For this database, create a business glossary that includes:
        1. Important business terms found in the schema
        2. Their definitions in plain business language
        3. Technical mappings to tables/columns where relevant
        4. Commonly used business metrics and KPIs derivable from this data
        5. Common synonyms and related terms

        Format your response as a structured glossary with clear term definitions.
        """
        
        # Generate business glossary
        logger.info("Generating business glossary with LLM...")
        glossary = await llm_client.generate(prompt)
        
        # Output the glossary to the console
        print("\n========== GENERATED BUSINESS GLOSSARY ==========\n")
        print(glossary)
        print("\n================================================\n")
        
        # Store glossary in Neo4j
        logger.info("Storing business glossary in Neo4j...")
        
        # Create a custom store_business_glossary method
        query = """
        MATCH (d:Dataset {tenant_id: $tenant_id, name: $dataset_id})
        MERGE (g:BusinessGlossary {tenant_id: $tenant_id, dataset_id: $dataset_id})
        ON CREATE SET
            g.content = $glossary,
            g.created_at = datetime()
        ON MATCH SET
            g.content = $glossary,
            g.updated_at = datetime()
        
        MERGE (d)-[:HAS_GLOSSARY]->(g)
        RETURN g
        """
        
        params = {
            "tenant_id": tenant_id,
            "dataset_id": dataset_id,
            "glossary": glossary
        }
        
        result = neo4j_client._execute_query(query, params)
        
        # Close clients
        neo4j_client.close()
        await llm_client.close()
        
        if result:
            logger.info("Business glossary successfully stored in Neo4j")
            return True
        else:
            logger.error("Failed to store business glossary in Neo4j")
            return False
        
    except Exception as e:
        logger.error(f"Error generating business glossary: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate business glossary")
    parser.add_argument("--tenant-id", required=True, help="Tenant ID")
    parser.add_argument("--api-key", help="OpenAI API key")
    
    args = parser.parse_args()
    
    # Run the process
    success = asyncio.run(generate_business_glossary(args.tenant_id, args.api_key))
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)