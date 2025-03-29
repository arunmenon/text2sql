"""
Test script to verify Neo4j connectivity and MCP server operations.

This script:
1. Tests direct Neo4j connection
2. Tests basic knowledge graph operations
3. Verifies result formats

Run this before deploying the MCP server to ensure Neo4j integration works.
"""
import os
import sys
import logging
from typing import Dict, Any, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add project root to path if needed
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Check for required dependencies
try:
    from neo4j import GraphDatabase, basic_auth
    neo4j_available = True
except ImportError:
    logger.error("❌ Neo4j driver is not installed. Please run: pip install neo4j")
    neo4j_available = False

# Only import these if Neo4j is available
if neo4j_available:
    from src.graph_storage.neo4j_client import Neo4jClient
    from src.mcp.enhanced_kg_resource import EnhancedKnowledgeGraphResource
else:
    # Define placeholder for type hints when Neo4j is not available
    class Neo4jClient:
        pass
    
    class EnhancedKnowledgeGraphResource:
        pass

# Neo4j connection parameters - use environment variables or defaults
NEO4J_URI = os.environ.get('NEO4J_URI', 'neo4j://localhost:7687')
NEO4J_USER = os.environ.get('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD', 'password')


def test_neo4j_direct_connection() -> bool:
    """
    Test direct connection to Neo4j.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    logger.info(f"Testing direct Neo4j connection to {NEO4J_URI}")
    try:
        client = Neo4jClient(
            uri=NEO4J_URI,
            username=NEO4J_USER,
            password=NEO4J_PASSWORD
        )
        
        # Test basic query
        result = client.run_query("MATCH (n) RETURN count(n) as count LIMIT 1")
        logger.info(f"Node count query result: {result}")
        
        # Close connection
        client.close()
        logger.info("✅ Direct Neo4j connection successful")
        return True
    except Exception as e:
        logger.error(f"❌ Neo4j connection failed: {str(e)}")
        return False


class MockResponse:
    """Mock Response class to simulate MCP response for testing"""
    
    def __init__(self, data=None, error=None, message=None):
        self.data = data
        self.error = error
        self.message = message
    
    def get_data(self):
        return self.data
    
    def get_error(self):
        return self.error
    
    def get_message(self):
        return self.message


async def test_kg_operations(client: Neo4jClient) -> Dict[str, bool]:
    """
    Test Knowledge Graph operations.
    
    Args:
        client: Neo4j client
        
    Returns:
        Dict mapping operation names to success status
    """
    logger.info("Testing Knowledge Graph operations")
    
    # Create resource
    kg_resource = EnhancedKnowledgeGraphResource(client)
    
    # Define test cases
    test_cases = [
        {
            "name": "query",
            "params": {"query": "MATCH (t:Table) RETURN t.name LIMIT 5"},
            "validation": lambda r: isinstance(r.data, list) and len(r.data) > 0
        },
        {
            "name": "get_table_schema",
            "params": {"table_name": "WorkOrders"},
            "validation": lambda r: r.data and "table" in r.data and "columns" in r.data
        },
        {
            "name": "find_relationships",
            "params": {"source": "WorkOrders", "target": "Locations", "max_depth": 2},
            "validation": lambda r: isinstance(r.data, list) 
        },
        {
            "name": "search_business_terms",
            "params": {"keyword": "invoice"},
            "validation": lambda r: isinstance(r.data, list)
        },
        {
            "name": "recommend_tables_for_query",
            "params": {"query": "work orders completed last month"},
            "validation": lambda r: isinstance(r.data, list)
        }
    ]
    
    # Run tests
    results = {}
    operations = kg_resource._register_operations.__wrapped__(kg_resource)
    
    for test_case in test_cases:
        name = test_case["name"]
        operation_name = f"knowledge_graph.{name}"
        logger.info(f"Testing operation: {operation_name}")
        
        try:
            # Get operation handler (this is a simplification; in reality MCP handles this)
            handler = operations.get(name)
            if not handler:
                logger.error(f"❌ Operation not found: {name}")
                results[operation_name] = False
                continue
            
            # Call operation
            response = await handler(**test_case["params"])
            
            # Validate response
            if test_case["validation"](response):
                logger.info(f"✅ Operation {operation_name} successful")
                logger.info(f"  Result preview: {str(response.data)[:200]}...")
                results[operation_name] = True
            else:
                logger.error(f"❌ Operation {operation_name} returned invalid data")
                results[operation_name] = False
        except Exception as e:
            logger.error(f"❌ Operation {operation_name} failed: {str(e)}")
            results[operation_name] = False
    
    return results


async def run_all_tests():
    """Run all tests and return overall status"""
    
    # Test 1: Direct Neo4j connection
    neo4j_connection = test_neo4j_direct_connection()
    if not neo4j_connection:
        logger.error("Neo4j connection test failed. Cannot proceed with operation tests.")
        return False
    
    # Create Neo4j client for operation tests
    client = Neo4jClient(
        uri=NEO4J_URI,
        username=NEO4J_USER,
        password=NEO4J_PASSWORD
    )
    
    try:
        # Test 2: Knowledge Graph operations
        operation_results = await test_kg_operations(client)
        
        # Calculate overall success
        success_count = sum(1 for success in operation_results.values() if success)
        total_count = len(operation_results)
        
        logger.info(f"Test summary: {success_count}/{total_count} operations successful")
        
        if success_count == total_count:
            logger.info("✅ All tests passed! MCP server should work correctly with Neo4j.")
            return True
        else:
            failed_ops = [op for op, success in operation_results.items() if not success]
            logger.warning(f"⚠️ Some tests failed: {', '.join(failed_ops)}")
            return False
    finally:
        # Clean up
        client.close()


if __name__ == "__main__":
    import asyncio
    
    if not neo4j_available:
        logger.error("❌ Cannot run tests: Neo4j driver is not installed")
        logger.error("Please install it with: pip install neo4j")
        sys.exit(1)
    
    # Run all tests
    success = asyncio.run(run_all_tests())
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)