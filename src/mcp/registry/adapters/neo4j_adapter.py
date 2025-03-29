"""
Neo4j Service Adapter for MCP Service Registry

This adapter implements operations for the Neo4j knowledge graph service.
"""
import logging
import uuid
from typing import Dict, List, Any, Optional

from neo4j import GraphDatabase

from ..registry import ServiceRegistry, ServiceDefinition, ServiceOperation, ServiceStatus

logger = logging.getLogger(__name__)

class Neo4jAdapter:
    """
    Neo4j Adapter for the Service Registry.
    
    This adapter provides operations for interacting with the Neo4j
    knowledge graph from the service registry.
    """
    
    def __init__(self):
        """Initialize the Neo4j adapter."""
        self.registry = None
        self.service_id = None
        self.driver = None
    
    def initialize(self, service: ServiceDefinition, registry: ServiceRegistry):
        """Initialize the adapter with service and registry information."""
        self.registry = registry
        self.service_id = service.id
        
        # Connect to Neo4j
        uri = service.endpoints.get("uri", "neo4j://localhost:7687")
        username = service.metadata.get("username", "neo4j")
        password = service.metadata.get("password", "password")
        
        try:
            self.driver = GraphDatabase.driver(uri, auth=(username, password))
            
            # Test connection
            with self.driver.session() as session:
                result = session.run("MATCH (n) RETURN count(n) as count LIMIT 1")
                count = result.single()["count"]
            
            logger.info(f"Connected to Neo4j at {uri}: {count} nodes found")
            
            # Update service status
            registry.update_service_status(service.id, ServiceStatus.ACTIVE)
            
            # Register operation handlers
            self._register_operation_handlers()
            
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {str(e)}")
            registry.update_service_status(service.id, ServiceStatus.ERROR)
    
    def _register_operation_handlers(self):
        """Register handlers for each operation."""
        # Register query operation
        self.registry.register_operation_handler(
            self.service_id, "query", self.query_graph
        )
        
        # Register table schema operation
        self.registry.register_operation_handler(
            self.service_id, "get_table_schema", self.get_table_schema
        )
        
        # Register relationship discovery operation
        self.registry.register_operation_handler(
            self.service_id, "find_relationships", self.find_relationships
        )
        
        # Register business term search operation
        self.registry.register_operation_handler(
            self.service_id, "search_business_terms", self.search_business_terms
        )
        
        # Register table recommendation operation
        self.registry.register_operation_handler(
            self.service_id, "recommend_tables_for_query", self.recommend_tables
        )
    
    def run_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Run a Cypher query and return the results."""
        params = params or {}
        try:
            with self.driver.session() as session:
                result = session.run(query, params)
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Query failed: {str(e)}")
            raise
    
    def query_graph(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a Cypher query and return results."""
        return self.run_query(query, parameters or {})
    
    def get_table_schema(self, table_name: str, include_relationships: bool = False) -> Dict[str, Any]:
        """Get table schema."""
        # Build schema query
        schema_query = """
        MATCH (t:Table {name: $table_name})
        OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
        WITH t, collect(c) as columns
        """
        
        if include_relationships:
            schema_query += """
            OPTIONAL MATCH (t)-[r]-(related)
            WHERE type(r) <> 'HAS_COLUMN' AND (related:Table OR related:BusinessMetric)
            WITH t, columns, collect({type: type(r), node: related}) as relationships
            RETURN {
                table: t,
                columns: columns,
                relationships: relationships
            } as result
            """
        else:
            schema_query += """
            RETURN {
                table: t,
                columns: columns
            } as result
            """
        
        result = self.run_query(schema_query, {"table_name": table_name})
        
        if not result or not result[0].get("result", {}).get("table"):
            raise ValueError(f"Table '{table_name}' not found")
            
        return result[0]["result"]
    
    def find_relationships(self, source: str, target: str, max_depth: int = 3) -> List[Dict[str, Any]]:
        """Find paths between two entities in the graph."""
        # Find paths between entities
        rel_query = f"""
        MATCH path = (source:Table)-[*1..{max_depth}]-(target:Table)
        WHERE source.name = $source AND target.name = $target
        RETURN path
        LIMIT 5
        """
        
        return self.run_query(rel_query, {
            "source": source, 
            "target": target
        })
    
    def search_business_terms(self, keyword: str) -> List[Dict[str, Any]]:
        """Search for business terms by keyword."""
        # First try with BusinessTerm label
        term_query_business_term = """
        MATCH (bt:BusinessTerm)
        WHERE bt.name CONTAINS $keyword OR bt.definition CONTAINS $keyword
        RETURN bt LIMIT 10
        """
        
        try:
            terms = self.run_query(term_query_business_term, {"keyword": keyword})
            if terms:
                return terms
        except Exception:
            logger.warning("BusinessTerm label not found, trying BusinessMetric")
        
        # Try with BusinessMetric label if BusinessTerm doesn't exist
        term_query_metric = """
        MATCH (bt:BusinessMetric)
        WHERE bt.name CONTAINS $keyword OR bt.definition CONTAINS $keyword
        RETURN bt LIMIT 10
        """
        
        return self.run_query(term_query_metric, {"keyword": keyword})
    
    def recommend_tables(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Recommend tables for a query."""
        # Extract keywords from the query
        keywords = [word.lower() for word in query.split() if len(word) > 3]
        
        # Build keyword match conditions for table names
        conditions = []
        for i, keyword in enumerate(keywords):
            conditions.append(f"t.name CONTAINS $keyword{i} OR t.description CONTAINS $keyword{i}")
        
        # Create parameters
        params = {f"keyword{i}": keyword for i, keyword in enumerate(keywords)}
        
        # Find matching tables
        if conditions:
            query_text = f"""
            MATCH (t:Table)
            WHERE {" OR ".join(conditions)}
            RETURN t
            LIMIT {limit}
            """
        else:
            query_text = f"""
            MATCH (t:Table)
            RETURN t
            LIMIT {limit}
            """
        
        return self.run_query(query_text, params)
    
    def close(self):
        """Close the Neo4j connection."""
        if self.driver:
            self.driver.close()