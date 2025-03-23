#!/bin/bash
# Supply Chain Schema Test Script
# This script tests the GraphAlchemy system with enhanced business glossary on the supply chain schema

set -e  # Exit on error

# Set the tenant ID
TENANT_ID="supply_chain_tenant"

echo "====== STEP 1: CLEAR DATABASE ======"
python3 scripts/database/clear_and_reload.py --tenant-id $TENANT_ID
# Alternative: direct graph database query to clear everything
# python3 -c 'from src.graph_storage.neo4j_client import Neo4jClient; import os; neo4j_client = Neo4jClient(os.getenv("NEO4J_URI", "neo4j://localhost:7687"), os.getenv("NEO4J_USERNAME", "neo4j"), os.getenv("NEO4J_PASSWORD", "password")); neo4j_client._execute_query("MATCH (n) DETACH DELETE n"); neo4j_client.close()'

echo ""
echo "====== STEP 2: LOAD SUPPLY CHAIN SCHEMA ======"
python3 scripts/schema/simulate_schema.py --tenant-id $TENANT_ID --schema-type supply_chain --schema-file ./schemas/supply_chain_simple.sql

echo ""
echo "====== STEP 3: DETECT RELATIONSHIPS ======"
python3 scripts/relationships/simulate_relationships.py --tenant-id $TENANT_ID --use-llm

echo ""
echo "====== STEP 4: RUN DIRECT ENHANCEMENT WITH ENHANCED BUSINESS GLOSSARY ======"
python3 scripts/enhancement/run_direct_enhancement.py --tenant-id $TENANT_ID --use-enhanced-glossary

echo ""
echo "====== STEP 5: CHECK DATABASE & GRAPH STRUCTURE ======"
python3 scripts/database/check_neo4j.py --tenant-id $TENANT_ID
python3 scripts/database/check_graph.py --tenant-id $TENANT_ID

echo ""
echo "====== STEP 6: RUN TEXT2SQL TEST QUERIES ======"
python3 tests/test_supply_chain_queries.py --tenant-id $TENANT_ID

# Optional: Run generic query evaluation suite as well
# python3 tests/run_query_evaluation.py --tenant-id $TENANT_ID

echo ""
echo "===== TEST COMPLETED ====="
echo "Check the results directory for query results and evaluation templates."