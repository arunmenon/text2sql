"""
Simulate relationship inference for schema in Neo4j.

This script analyzes the simulated schema in Neo4j and
creates relationships between tables based on naming patterns and
LLM-based inference.

Usage:
    python simulate_relationships.py --tenant-id YOUR_TENANT_ID [--schema-type walmart] [--min-confidence 0.7] [--use-llm]
"""
import argparse
import asyncio
import logging
import os
from dotenv import load_dotenv

from src.graph_storage.neo4j_client import Neo4jClient
from src.relationship_inference.name_pattern.pattern_matcher import PatternMatcher
from src.relationship_inference.llm_inference.relationship_analyzer import LLMRelationshipAnalyzer
from src.llm.client import LLMClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

async def simulate_relationships(tenant_id, schema_type="merchandising", min_confidence=0.7, use_llm=False):
    """Simulate relationship inference for database schema."""
    try:
        # Get Neo4j connection details
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Initialize Neo4j client
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        
        # Get tables
        logger.info(f"Getting tables for tenant {tenant_id}")
        tables = neo4j_client.get_tables_for_tenant(tenant_id)
        
        # Prepare table data with columns
        table_data = []
        for table in tables:
            table_name = table["name"]
            columns = neo4j_client.get_columns_for_table(tenant_id, table_name)
            
            table_data.append({
                "table_name": table_name,
                "description": table.get("description", ""),
                "columns": columns
            })
        
        # Infer relationships using name patterns
        logger.info("Inferring relationships using name patterns")
        pattern_matcher = PatternMatcher()
        pattern_relationships = pattern_matcher.infer_relationships(table_data)
        
        # Store pattern-based relationships
        pattern_count = 0
        for rel in pattern_relationships:
            if rel["confidence"] >= min_confidence:
                neo4j_client.create_relationship(
                    tenant_id,
                    rel["source_table"],
                    rel["source_column"],
                    rel["target_table"],
                    rel["target_column"],
                    rel["confidence"],
                    rel["detection_method"]
                )
                pattern_count += 1
        
        logger.info(f"Created {pattern_count} pattern-based relationships")
        
        # LLM-based relationship inference (if enabled)
        llm_count = 0
        if use_llm:
            logger.info("Inferring relationships using LLM")
            
            # Get LLM API key
            openai_api_key = os.getenv("OPENAI_API_KEY")
            if not openai_api_key:
                logger.warning("No OpenAI API key found in environment. Skipping LLM inference.")
            else:
                # Initialize LLM client
                llm_client = LLMClient(api_key=openai_api_key, provider="openai", model="gpt-4o")
                
                # Initialize LLM relationship analyzer
                llm_analyzer = LLMRelationshipAnalyzer(llm_client)
                
                # Infer relationships
                llm_relationships = await llm_analyzer.infer_relationships(table_data, min_confidence)
                
                # Store LLM-based relationships
                for rel in llm_relationships:
                    neo4j_client.create_relationship(
                        tenant_id,
                        rel["source_table"],
                        rel["source_column"],
                        rel["target_table"],
                        rel["target_column"],
                        rel["confidence"],
                        rel["detection_method"],
                        relationship_type=rel.get("relationship_type"),
                        metadata={"explanation": rel.get("explanation", "")}
                    )
                    llm_count += 1
                
                # Close LLM client
                await llm_client.close()
                
            logger.info(f"Added {llm_count} LLM-inferred relationships")
        
        # Get summary
        summary = neo4j_client.get_schema_summary(tenant_id)
        logger.info(f"Schema summary: {summary}")
        
        # Close Neo4j client
        neo4j_client.close()
        
    except Exception as e:
        logger.error(f"Error in relationship simulation: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate relationship inference")
    parser.add_argument("--tenant-id", required=True, help="Tenant ID")
    parser.add_argument("--schema-type", default="merchandising", 
                      choices=["merchandising", "walmart"],
                      help="Type of schema to simulate")
    parser.add_argument("--min-confidence", type=float, default=0.7, 
                      help="Minimum confidence threshold (0.0-1.0)")
    parser.add_argument("--use-llm", action="store_true", 
                      help="Enable LLM-based relationship inference")
    
    args = parser.parse_args()
    
    asyncio.run(simulate_relationships(
        args.tenant_id, 
        args.schema_type,
        args.min_confidence,
        args.use_llm
    ))