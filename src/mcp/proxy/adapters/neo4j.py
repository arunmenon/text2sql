"""
Neo4j Service Adapter for MCP Proxy

This adapter connects to a Neo4j database and exposes operations
through the MCP protocol.
"""
import logging
from typing import Dict, List, Any, Optional

from mcp.server import Resource, resolver, Response
from neo4j import GraphDatabase

from ..core import ServiceAdapter

# Configure logging
logger = logging.getLogger(__name__)

class Neo4jAdapter(ServiceAdapter):
    """Adapter for Neo4j graph database service."""
    
    def _initialize_client(self):
        """Initialize the Neo4j client."""
        uri = self.connection.get("url", "neo4j://localhost:7687")
        username = self.connection.get("username", "neo4j")
        password = self.connection.get("password", "password")
        
        try:
            driver = GraphDatabase.driver(uri, auth=(username, password))
            
            # Test connection
            with driver.session() as session:
                result = session.run("MATCH (n) RETURN count(n) as count LIMIT 1")
                count = result.single()["count"]
            
            logger.info(f"Connected to Neo4j at {uri}: {count} nodes found")
            return driver
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {str(e)}")
            raise
    
    def run_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Run a Cypher query and return the results."""
        params = params or {}
        try:
            with self.client.session() as session:
                result = session.run(query, params)
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Query failed: {str(e)}")
            raise
    
    def _register_operations(self, resource: Resource):
        """Register Neo4j operations with the resource."""
        
        # 1. Cypher Query Operation
        @resolver(resource.operation(
            "query",
            description="Run a Cypher query against the knowledge graph"
        ))
        async def query_graph(query: str, parameters: Optional[Dict[str, Any]] = None) -> Response:
            """Execute a Cypher query and return results."""
            try:
                results = self.run_query(query, parameters or {})
                return Response(data=results)
            except Exception as e:
                return Response(error=str(e))
        
        # 2. Table Schema Operation
        @resolver(resource.operation(
            "get_table_schema",
            description="Get detailed schema information for a table with column metadata"
        ))
        async def get_table_schema(table_name: str, include_relationships: bool = False) -> Response:
            """Get table schema."""
            try:
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
                    return Response(error=f"Table '{table_name}' not found")
                    
                return Response(data=result[0]["result"])
            except Exception as e:
                return Response(error=str(e))
        
        # 3. Relationship Discovery Operation
        @resolver(resource.operation(
            "find_relationships",
            description="Find relationships between two entities in the knowledge graph"
        ))
        async def find_relationships(source: str, target: str, max_depth: int = 3) -> Response:
            """Find paths between two entities in the graph."""
            try:
                # Find paths between entities
                rel_query = f"""
                MATCH path = (source:Table)-[*1..{max_depth}]-(target:Table)
                WHERE source.name = $source AND target.name = $target
                RETURN path
                LIMIT 5
                """
                
                paths = self.run_query(rel_query, {
                    "source": source, 
                    "target": target
                })
                
                if not paths:
                    return Response(
                        data=[],
                        message=f"No paths found between '{source}' and '{target}'"
                    )
                    
                return Response(data=paths)
            except Exception as e:
                return Response(error=str(e))
        
        # 4. Business Term Search Operation
        @resolver(resource.operation(
            "search_business_terms",
            description="Search for business terms by keyword"
        ))
        async def search_business_terms(keyword: str) -> Response:
            """Search for business terms by keyword."""
            try:
                # First try with BusinessTerm label
                term_query_business_term = """
                MATCH (bt:BusinessTerm)
                WHERE bt.name CONTAINS $keyword OR bt.definition CONTAINS $keyword
                RETURN bt LIMIT 10
                """
                
                try:
                    terms = self.run_query(term_query_business_term, {"keyword": keyword})
                    if terms:
                        return Response(data=terms)
                except Exception:
                    logger.warning("BusinessTerm label not found, trying BusinessMetric")
                
                # Try with BusinessMetric label if BusinessTerm doesn't exist
                term_query_metric = """
                MATCH (bt:BusinessMetric)
                WHERE bt.name CONTAINS $keyword OR bt.definition CONTAINS $keyword
                RETURN bt LIMIT 10
                """
                
                terms = self.run_query(term_query_metric, {"keyword": keyword})
                return Response(data=terms)
            except Exception as e:
                return Response(error=str(e))
        
        # 5. Table Recommendation Operation
        @resolver(resource.operation(
            "recommend_tables_for_query",
            description="Suggest relevant tables based on a natural language query"
        ))
        async def recommend_tables(query: str, limit: int = 5) -> Response:
            """Recommend tables for a query."""
            try:
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
                
                tables = self.run_query(query_text, params)
                
                return Response(data=tables)
            except Exception as e:
                return Response(error=str(e))
    
    def close(self):
        """Close the Neo4j connection."""
        if self.client:
            self.client.close()