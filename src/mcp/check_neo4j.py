"""
Simple Neo4j connectivity test script.

This script checks if Neo4j is accessible with the provided credentials.
"""
import os
import sys
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Neo4j connection parameters
NEO4J_URI = os.environ.get('NEO4J_URI', 'neo4j://localhost:7687')
NEO4J_USER = os.environ.get('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD', 'password')

def check_neo4j():
    """Check Neo4j connectivity."""
    try:
        # Try to import Neo4j
        from neo4j import GraphDatabase
        logger.info("✅ Neo4j driver is installed")
    except ImportError:
        logger.error("❌ Neo4j driver is not installed. Please run: pip install neo4j")
        return False

    try:
        # Connect to Neo4j
        logger.info(f"Connecting to Neo4j at {NEO4J_URI}")
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        
        # Test connection with a simple query
        with driver.session() as session:
            result = session.run("MATCH (n) RETURN count(n) as count LIMIT 1")
            node_count = result.single()["count"]
        
        # Close driver
        driver.close()
        
        logger.info(f"✅ Successfully connected to Neo4j: Found {node_count} nodes")
        return True
    except Exception as e:
        logger.error(f"❌ Neo4j connection failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = check_neo4j()
    sys.exit(0 if success else 1)