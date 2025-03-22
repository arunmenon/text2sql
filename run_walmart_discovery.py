#!/usr/bin/env python3
"""
Walmart Schema Discovery Pipeline

This script runs the full schema discovery process for the Walmart retail schema:
1. Schema extraction
2. Relationship inference using pattern matching and optionally LLM
3. Join path discovery

Usage:
    python run_walmart_discovery.py --tenant-id YOUR_TENANT_ID [--use-llm]
"""
import argparse
import asyncio
import logging
import os
import sys
import subprocess
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_schema_extraction(tenant_id):
    """Run the schema extraction process."""
    logger.info("Starting schema extraction...")
    
    # Run the simulation script
    cmd = ["python", "simulate_schema.py", "--tenant-id", tenant_id, "--schema-type", "walmart"]
    logger.info(f"Executing: {' '.join(cmd)}")
    
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()
    
    if process.returncode != 0:
        logger.error(f"Schema extraction failed with exit code {process.returncode}")
        logger.error(f"Error: {stderr}")
        return False
    
    logger.info("Schema extraction completed successfully")
    return True

async def run_relationship_inference(tenant_id, use_llm=False):
    """Run the relationship inference process."""
    logger.info("Starting relationship inference...")
    
    # Run the simulation script
    cmd = ["python", "simulate_relationships.py", "--tenant-id", tenant_id, "--schema-type", "walmart"]
    
    if use_llm:
        cmd.append("--use-llm")
        
    logger.info(f"Executing: {' '.join(cmd)}")
    
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()
    
    if process.returncode != 0:
        logger.error(f"Relationship inference failed with exit code {process.returncode}")
        logger.error(f"Error: {stderr}")
        return False
    
    logger.info("Relationship inference completed successfully")
    return True

async def run_join_path_exploration(tenant_id):
    """Run the join path discovery process."""
    logger.info("Exploring join paths in the Walmart schema...")
    
    # Import Neo4j client here to avoid unnecessary dependency in case previous steps fail
    from src.graph_storage.neo4j_client import Neo4jClient
    
    # Get Neo4j connection details
    neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
    
    # Initialize Neo4j client
    neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
    
    # Get tables
    tables = neo4j_client.get_tables_for_tenant(tenant_id)
    table_names = [table["name"] for table in tables]
    logger.info(f"Found {len(tables)} tables in schema")
    
    # Sample table pairs to find join paths between
    sample_pairs = [
        ("Products", "Categories"),
        ("Products", "Suppliers"),
        ("Sales", "Products"),
        ("Products", "Inventory"),
        ("Products", "Returns"),
        ("Stores", "Inventory"),
        ("Products", "Departments"),
    ]
    
    # Filter pairs to only include tables that exist
    valid_pairs = [(src, tgt) for src, tgt in sample_pairs 
                 if src in table_names and tgt in table_names]
    
    # Try different join path strategies
    strategies = ["default", "weighted", "usage", "verified", "all"]
    
    for source_table, target_table in valid_pairs:
        logger.info(f"Finding join paths from {source_table} to {target_table}...")
        
        for strategy in strategies:
            logger.info(f"Using strategy: {strategy}")
            try:
                paths = neo4j_client.find_join_paths(
                    tenant_id, 
                    source_table, 
                    target_table,
                    strategy=strategy
                )
                
                if paths:
                    logger.info(f"Found {len(paths)} paths using {strategy} strategy")
                    # Log first path
                    if paths:
                        first_path = paths[0]
                        path_info = f"Path confidence: {first_path.get('path_confidence', 0)}, "
                        path_info += f"Weight: {first_path.get('path_weight', 0)}, "
                        path_info += f"Join conditions: {first_path.get('join_conditions', [])}"
                        logger.info(path_info)
                else:
                    logger.info(f"No paths found with {strategy} strategy")
            except Exception as e:
                logger.error(f"Error finding join paths with {strategy} strategy: {e}")
                
    # Close Neo4j client
    neo4j_client.close()
    
    logger.info("Join path exploration completed")
    return True

async def analyze_relationship_quality(tenant_id):
    """Analyze the quality of discovered relationships."""
    logger.info("Analyzing relationship discovery quality...")
    
    # Import Neo4j client here to avoid unnecessary dependency in case previous steps fail
    from src.graph_storage.neo4j_client import Neo4jClient
    
    # Get Neo4j connection details
    neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
    
    # Initialize Neo4j client
    neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
    
    # Get tables
    tables = neo4j_client.get_tables_for_tenant(tenant_id)
    
    # Expected key relationships that should be discovered
    expected_relationships = [
        ("Categories", "department_id", "Departments", "department_id"),
        ("SubCategories", "category_id", "Categories", "category_id"),
        ("SubCategories", "department_id", "Departments", "department_id"),
        ("Products", "department_id", "Departments", "department_id"),
        ("Products", "category_id", "Categories", "category_id"),
        ("Products", "sub_category_id", "SubCategories", "sub_category_id"),
        ("Products", "vendor_id", "Suppliers", "supplier_id"),
        ("Products", "primary_supplier_id", "Suppliers", "supplier_id"),
        ("DCInventory", "dc_id", "DistributionCenters", "dc_id"),
        ("DCInventory", "product_id", "Products", "product_id"),
        ("StoreEmployees", "store_id", "Stores", "store_id"),
        ("Inventory", "store_id", "Stores", "store_id"),
        ("Inventory", "product_id", "Products", "product_id"),
        ("Sales", "store_id", "Stores", "store_id"),
        ("Sales", "product_id", "Products", "product_id"),
        ("Sales", "promotion_id", "Promotions", "promotion_id"),
        ("Pricing", "product_id", "Products", "product_id"),
        ("Returns", "store_id", "Stores", "store_id"),
        ("Returns", "product_id", "Products", "product_id"),
    ]
    
    # Get all discovered relationships
    discovered_relationships = []
    for table in tables:
        table_rels = neo4j_client.get_relationships_for_table(tenant_id, table["name"])
        for rel in table_rels:
            source_table = rel["source"]["table_name"]
            source_column = rel["source"]["name"]
            target_table = rel["target"]["table_name"]
            target_column = rel["target"]["name"]
            confidence = rel["r"]["confidence"]
            detection = rel["r"]["detection_method"]
            
            discovered_relationships.append({
                "source_table": source_table,
                "source_column": source_column,
                "target_table": target_table,
                "target_column": target_column,
                "confidence": confidence,
                "detection_method": detection
            })
    
    # Check for true positives
    found_count = 0
    for expected in expected_relationships:
        src_table, src_col, tgt_table, tgt_col = expected
        
        # Check if this relationship was discovered
        found = False
        for rel in discovered_relationships:
            if (rel["source_table"] == src_table and rel["source_column"] == src_col and
                rel["target_table"] == tgt_table and rel["target_column"] == tgt_col):
                found = True
                logger.info(f"✓ Found: {src_table}.{src_col} -> {tgt_table}.{tgt_col} "
                          f"(confidence: {rel['confidence']}, method: {rel['detection_method']})")
                break
                
        if not found:
            logger.info(f"✗ Missing: {src_table}.{src_col} -> {tgt_table}.{tgt_col}")
        else:
            found_count += 1
    
    # Calculate discovery rate
    discovery_rate = found_count / len(expected_relationships) * 100
    logger.info(f"Discovered {found_count} out of {len(expected_relationships)} key relationships "
              f"({discovery_rate:.1f}% discovery rate)")
    
    # Log total relationships
    logger.info(f"Total relationships discovered: {len(discovered_relationships)}")
    
    # Close Neo4j client
    neo4j_client.close()
    
    return True

async def main(tenant_id, use_llm=False):
    """Run the full schema discovery pipeline."""
    start_time = time.time()
    
    # Step 1: Schema Extraction
    schema_extraction_success = await run_schema_extraction(tenant_id)
    if not schema_extraction_success:
        logger.error("Schema extraction failed. Aborting pipeline.")
        return False
    
    # Step 2: Relationship Inference
    relationship_inference_success = await run_relationship_inference(tenant_id, use_llm)
    if not relationship_inference_success:
        logger.error("Relationship inference failed. Aborting pipeline.")
        return False
    
    # Step 3: Analyze relationship quality
    analysis_success = await analyze_relationship_quality(tenant_id)
    if not analysis_success:
        logger.error("Relationship analysis failed.")
    
    # Step 4: Join Path Exploration
    join_path_success = await run_join_path_exploration(tenant_id)
    if not join_path_success:
        logger.error("Join path exploration failed.")
        return False
    
    # Calculate total time
    total_time = time.time() - start_time
    logger.info(f"Full schema discovery pipeline completed in {total_time:.2f} seconds")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Walmart schema discovery pipeline")
    parser.add_argument("--tenant-id", required=True, help="Tenant ID")
    parser.add_argument("--use-llm", action="store_true", help="Enable LLM-based relationship inference")
    
    args = parser.parse_args()
    
    # Make script executable
    os.chmod(__file__, 0o755)
    
    # Run the pipeline
    success = asyncio.run(main(args.tenant_id, args.use_llm))
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)