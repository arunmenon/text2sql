"""
Knowledge Graph Resource for MCP Server

This module defines the MCP resource for the Knowledge Graph,
providing operations to query and traverse the graph.
"""
from typing import Dict, List, Any, Optional

from mcp_server import Resource, resolver, Response

from src.graph_storage.neo4j_client import Neo4jClient


class KnowledgeGraphResource:
    """
    MCP Resource implementation for Knowledge Graph operations.
    Exposes graph querying and traversal functionality to MCP clients.
    """
    
    def __init__(self, neo4j_client: Neo4jClient):
        """
        Initialize the Knowledge Graph resource.
        
        Args:
            neo4j_client: An initialized Neo4j client
        """
        self.client = neo4j_client
        self.resource = Resource(
            name="knowledge_graph",
            description="Access to the semantic knowledge graph for Text2SQL operations"
        )
        
        # Register operations
        self._register_operations()
    
    def _register_operations(self):
        """Register all operations for this resource"""
        
        @resolver(self.resource.operation(
            "query",
            description="Run a Cypher query against the knowledge graph"
        ))
        async def query_graph(query: str) -> Response:
            """
            Execute a Cypher query and return results.
            
            Args:
                query: Valid Cypher query string
                
            Returns:
                Response with query results
            """
            try:
                results = self.client.query(query)
                return Response(data=results)
            except Exception as e:
                return Response(error=str(e))
        
        @resolver(self.resource.operation(
            "get_entity",
            description="Get detailed information about an entity by name"
        ))
        async def get_entity(entity_name: str) -> Response:
            """
            Get detailed information about an entity.
            
            Args:
                entity_name: Name of the entity (table, concept, etc.)
                
            Returns:
                Response with entity details
            """
            try:
                # Get entity by name (table, business term, or concept)
                result = self.client.run_query(
                    """
                    MATCH (n)
                    WHERE (n:Table OR n:BusinessTerm OR n:Concept) AND n.name = $name
                    RETURN n
                    """,
                    {'name': entity_name}
                )
                
                if not result:
                    return Response(error=f"Entity '{entity_name}' not found")
                    
                return Response(data=result)
            except Exception as e:
                return Response(error=str(e))
        
        @resolver(self.resource.operation(
            "find_relationships",
            description="Find relationships between two entities"
        ))
        async def find_relationships(source: str, target: str, max_depth: int = 3) -> Response:
            """
            Find paths between two entities in the graph.
            
            Args:
                source: Source entity name
                target: Target entity name
                max_depth: Maximum path length to consider
                
            Returns:
                Response with paths between entities
            """
            try:
                # Find paths between entities
                paths = self.client.run_query(
                    """
                    MATCH path = (source)-[*1..{max_depth}]-(target)
                    WHERE (source:Table OR source:BusinessTerm OR source:Concept) 
                      AND source.name = $source
                      AND (target:Table OR target:BusinessTerm OR target:Concept)
                      AND target.name = $target
                    RETURN path
                    LIMIT 5
                    """.format(max_depth=max_depth),
                    {'source': source, 'target': target}
                )
                
                if not paths:
                    return Response(
                        data=[],
                        message=f"No paths found between '{source}' and '{target}'"
                    )
                    
                return Response(data=paths)
            except Exception as e:
                return Response(error=str(e))
        
        @resolver(self.resource.operation(
            "get_table_schema",
            description="Get detailed schema information for a table"
        ))
        async def get_table_schema(table_name: str) -> Response:
            """
            Get detailed schema information for a specific table.
            
            Args:
                table_name: Name of the table
                
            Returns:
                Response with table schema details including columns
            """
            try:
                # Get table and its columns
                schema = self.client.run_query(
                    """
                    MATCH (t:Table {name: $table_name})
                    OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
                    WITH t, collect(c) as columns
                    RETURN {
                        table: t,
                        columns: columns
                    } as result
                    """,
                    {'table_name': table_name}
                )
                
                if not schema or not schema.get('table'):
                    return Response(error=f"Table '{table_name}' not found")
                    
                return Response(data=schema)
            except Exception as e:
                return Response(error=str(e))
        
        @resolver(self.resource.operation(
            "search_business_terms",
            description="Search for business terms by keyword"
        ))
        async def search_business_terms(keyword: str) -> Response:
            """
            Search for business terms by keyword.
            
            Args:
                keyword: Search keyword
                
            Returns:
                Response with matching business terms
            """
            try:
                terms = self.client.run_query(
                    """
                    MATCH (bt:BusinessTerm)
                    WHERE bt.name CONTAINS $keyword OR bt.definition CONTAINS $keyword
                    RETURN bt
                    LIMIT 10
                    """,
                    {'keyword': keyword}
                )
                
                return Response(data=terms)
            except Exception as e:
                return Response(error=str(e))
                
        @resolver(self.resource.operation(
            "get_column_metadata",
            description="Get detailed metadata for a specific column"
        ))
        async def get_column_metadata(table_name: str, column_name: str) -> Response:
            """
            Get detailed metadata for a specific column.
            
            Args:
                table_name: Name of the table
                column_name: Name of the column
                
            Returns:
                Response with column metadata
            """
            try:
                metadata = self.client.run_query(
                    """
                    MATCH (c:Column {table_name: $table_name, name: $column_name})
                    OPTIONAL MATCH (c)-[r]->(n)
                    WITH c, collect({relationship: type(r), node: n}) as relationships
                    RETURN {
                        column: c,
                        relationships: relationships
                    } as result
                    """,
                    {'table_name': table_name, 'column_name': column_name}
                )
                
                if not metadata:
                    return Response(
                        error=f"Column '{column_name}' not found in table '{table_name}'"
                    )
                    
                return Response(data=metadata)
            except Exception as e:
                return Response(error=str(e))
    
    def get_resource(self) -> Resource:
        """Get the MCP resource instance"""
        return self.resource