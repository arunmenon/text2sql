#!/usr/bin/env python3
"""
Clear Neo4j Database and Reload Schema with LLM Enhancement

This script:
1. Clears all data from the Neo4j database
2. Loads the merchandising schema
3. Infers relationships using both pattern matching and LLM
4. Runs the metadata enhancement workflow

Usage:
    python clear_and_reload.py --tenant-id YOUR_TENANT_ID
"""
import argparse
import asyncio
import logging
import os
import sys
from dotenv import load_dotenv

from src.graph_storage.neo4j_client import Neo4jClient
from src.llm.client import LLMClient
from src.text2sql.metadata_enhancement import run_enhancement, SchemaEnhancementWorkflow

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

async def clear_neo4j_database():
    """Clear all data from Neo4j database."""
    try:
        # Get Neo4j connection details
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Initialize Neo4j client
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        
        logger.info("Clearing all data from Neo4j database...")
        neo4j_client._execute_query("MATCH (n) DETACH DELETE n")
        logger.info("Database cleared successfully")
        
        # Close Neo4j client
        neo4j_client.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error clearing Neo4j database: {e}")
        return False

async def reload_schema(tenant_id):
    """Reload the merchandising schema."""
    try:
        logger.info(f"Loading merchandising schema for tenant {tenant_id}...")
        
        # Use simulate_schema.py to load the schema
        import subprocess
        process = subprocess.Popen(
            ["python", "simulate_schema.py", "--tenant-id", tenant_id, "--schema-type", "merchandising"],
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True
        )
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            logger.error(f"Schema loading failed: {stderr}")
            return False
        
        logger.info("Merchandising schema loaded successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error loading schema: {e}")
        return False

async def infer_relationships(tenant_id):
    """Infer relationships using pattern matching and LLM."""
    try:
        logger.info("Inferring relationships...")
        
        # Use simulate_relationships.py with LLM enabled
        import subprocess
        process = subprocess.Popen(
            ["python", "simulate_relationships.py", "--tenant-id", tenant_id, "--schema-type", "merchandising", "--use-llm"],
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True
        )
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            logger.error(f"Relationship inference failed: {stderr}")
            return False
        
        logger.info("Relationships inferred successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error inferring relationships: {e}")
        return False

async def run_metadata_enhancement(tenant_id):
    """Run the metadata enhancement workflow."""
    try:
        logger.info("Running metadata enhancement workflow...")
        
        # Get Neo4j connection details
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Get LLM API details
        llm_api_key = os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY")
        llm_model = os.getenv("LLM_MODEL", "gpt-4o")
        
        if not llm_api_key:
            logger.error("No LLM API key found in environment variables")
            return False
        
        # Initialize clients
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        llm_client = LLMClient(api_key=llm_api_key, model=llm_model)
        
        # Get dataset_id from Neo4j
        datasets = neo4j_client.get_datasets_for_tenant(tenant_id)
        if not datasets:
            logger.error(f"No datasets found for tenant {tenant_id}")
            return False
        
        dataset_id = datasets[0].get("name")
        
        # Run enhancement workflow
        workflow = SchemaEnhancementWorkflow(neo4j_client, llm_client)
        success = await workflow.run(tenant_id, dataset_id)
        
        # Close clients
        neo4j_client.close()
        await llm_client.close()
        
        if success:
            logger.info("Metadata enhancement workflow completed successfully")
        else:
            logger.error("Metadata enhancement workflow failed")
        
        return success
        
    except Exception as e:
        logger.error(f"Error in metadata enhancement: {e}")
        return False

async def show_neo4j_queries():
    """Show useful Neo4j queries for inspection."""
    print("\nUseful Neo4j queries to run in Neo4j Browser:")
    print("---------------------------------------------")
    print("// View all nodes")
    print("MATCH (n) RETURN n LIMIT 100")
    print("\n// View business glossary")
    print("MATCH (g:BusinessGlossary) RETURN g.content")
    print("\n// View enhanced table descriptions")
    print("MATCH (t:Table) WHERE t.description_enhanced = true RETURN t.name, t.description")
    print("\n// View concept tags for tables")
    print("MATCH (t:Table) WHERE t.concept_tags IS NOT NULL RETURN t.name, t.concept_tags")
    print("\n// View semantic relationships discovered by LLM")
    print("MATCH (source:Column)-[r:LIKELY_REFERENCES]->(target:Column) WHERE r.detection_method = 'llm_semantic'")
    print("RETURN source.table_name, source.name, target.table_name, target.name, r.confidence, r.metadata_str")
    print("\n// View all relationships")
    print("MATCH (source:Column)-[r:LIKELY_REFERENCES]->(target:Column)")
    print("RETURN source.table_name, source.name, target.table_name, target.name, r.confidence, r.detection_method")

async def main(tenant_id):
    """Run the full process."""
    # Step 1: Clear Neo4j database
    if not await clear_neo4j_database():
        logger.error("Failed to clear database. Aborting.")
        return False
    
    # Step 2: Reload schema
    if not await reload_schema(tenant_id):
        logger.error("Failed to reload schema. Aborting.")
        return False
    
    # Step 3: Infer relationships
    if not await infer_relationships(tenant_id):
        logger.error("Failed to infer relationships. Aborting.")
        return False
    
    # Step 4: Run metadata enhancement
    if not await run_metadata_enhancement(tenant_id):
        logger.error("Failed to enhance metadata.")
        # Continue anyway to show queries
    
    # Step 5: Show useful Neo4j queries
    await show_neo4j_queries()
    
    logger.info("Process completed!")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clear Neo4j and reload schema with LLM enhancement")
    parser.add_argument("--tenant-id", required=True, help="Tenant ID")
    
    args = parser.parse_args()
    
    # Make script executable
    os.chmod(__file__, 0o755)
    
    # Run the process
    success = asyncio.run(main(args.tenant_id))
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)