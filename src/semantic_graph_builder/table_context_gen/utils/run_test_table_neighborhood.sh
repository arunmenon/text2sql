#!/bin/bash
# Run the table neighborhood provider test

# Ensure we're in the project root directory
cd "$(dirname "$0")"/../../../../

# Set environment variables for Neo4j connection (modify as needed)
export NEO4J_URI=${NEO4J_URI:-"neo4j://localhost:7687"}
export NEO4J_USERNAME=${NEO4J_USERNAME:-"neo4j"}
export NEO4J_PASSWORD=${NEO4J_PASSWORD:-"password"}

# Set PYTHONPATH to include the project root
export PYTHONPATH=$PYTHONPATH:$(pwd)

# Run the test script
python src/semantic_graph_builder/table_context_gen/utils/test_table_neighborhood.py

# Print completion message
echo "Test complete. Check the output above for results."