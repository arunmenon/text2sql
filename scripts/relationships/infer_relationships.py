"""
Infer relationships between tables and store in Neo4j.

Usage:
    python infer_relationships.py --tenant-id YOUR_TENANT_ID --project-id YOUR_GCP_PROJECT --dataset-id YOUR_DATASET
"""
import argparse
import asyncio
import logging
import os
from dotenv import load_dotenv

from src.graph_storage.neo4j_client import Neo4jClient
from src.relationship_inference.statistical.overlap_analyzer import OverlapAnalyzer
from src.relationship_inference.name_pattern.pattern_matcher import PatternMatcher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

async def infer_relationships(tenant_id, project_id, dataset_id, min_confidence):
    """Infer relationships between tables and store in Neo4j."""
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
        
        # Infer relationships using statistical analysis
        logger.info("Inferring relationships using statistical analysis")
        overlap_analyzer = OverlapAnalyzer(project_id)
        stat_relationships = await overlap_analyzer.find_candidate_relationships(
            dataset_id,
            table_data,
            min_confidence
        )
        
        # Store statistical relationships
        stat_count = 0
        for rel in stat_relationships:
            neo4j_client.create_relationship(
                tenant_id,
                rel["source_table"],
                rel["source_column"],
                rel["target_table"],
                rel["target_column"],
                rel["confidence"],
                rel["detection_method"]
            )
            stat_count += 1
        
        logger.info(f"Created {stat_count} statistical relationships")
        
        # Get summary
        summary = neo4j_client.get_schema_summary(tenant_id)
        logger.info(f"Schema summary: {summary}")
        
        # Close Neo4j client
        neo4j_client.close()
        
    except Exception as e:
        logger.error(f"Error inferring relationships: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Infer relationships between tables")
    parser.add_argument("--tenant-id", required=True, help="Tenant ID")
    parser.add_argument("--project-id", required=True, help="Google Cloud project ID")
    parser.add_argument("--dataset-id", required=True, help="BigQuery dataset ID")
    parser.add_argument("--min-confidence", type=float, default=0.7, help="Minimum confidence threshold (0.0-1.0)")
    
    args = parser.parse_args()
    
    asyncio.run(infer_relationships(args.tenant_id, args.project_id, args.dataset_id, args.min_confidence))