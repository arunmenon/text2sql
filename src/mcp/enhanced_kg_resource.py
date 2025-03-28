"""
Enhanced Knowledge Graph Resource for MCP Server

This module defines an enhanced MCP resource for the Knowledge Graph,
providing operations to query and traverse the graph with comprehensive
schema documentation for automatic discovery.
"""
from typing import Dict, List, Any, Optional, Union

from mcp_server import Resource, resolver, Response, JSONSchema

from src.graph_storage.neo4j_client import Neo4jClient


class EnhancedKnowledgeGraphResource:
    """
    Enhanced MCP Resource implementation for Knowledge Graph operations.
    Provides comprehensive operation descriptions and schema documentation.
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
            description="Semantic knowledge graph for Text2SQL with business glossary, tables, and relationships"
        )
        
        # Register operations
        self._register_operations()
    
    def _register_operations(self):
        """Register all operations for this resource with detailed schemas"""
        
        # 1. Query operation
        @resolver(self.resource.operation(
            "query",
            description="Run a Cypher query against the knowledge graph",
            schema=JSONSchema({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Valid Cypher query string to execute"
                    },
                    "parameters": {
                        "type": "object",
                        "description": "Optional parameters for the Cypher query"
                    }
                },
                "required": ["query"]
            })
        ))
        async def query_graph(query: str, parameters: Optional[Dict[str, Any]] = None) -> Response:
            """Execute a Cypher query and return results."""
            try:
                params = parameters or {}
                results = self.client.run_query(query, params)
                return Response(data=results)
            except Exception as e:
                return Response(error=str(e))
        
        # 2. Get entity
        @resolver(self.resource.operation(
            "get_entity",
            description="Get detailed information about an entity by name",
            schema=JSONSchema({
                "type": "object",
                "properties": {
                    "entity_name": {
                        "type": "string",
                        "description": "Name of the entity (table, business term, or concept)"
                    },
                    "entity_type": {
                        "type": "string",
                        "description": "Optional entity type to filter by (Table, BusinessTerm, Concept, etc.)",
                        "enum": ["Table", "BusinessTerm", "Column", "Concept", "BusinessMetric"]
                    }
                },
                "required": ["entity_name"]
            })
        ))
        async def get_entity(entity_name: str, entity_type: Optional[str] = None) -> Response:
            """Get detailed information about an entity."""
            try:
                # Build type filter
                type_filter = f"n:{entity_type}" if entity_type else "(n:Table OR n:BusinessTerm OR n:Concept OR n:Column OR n:BusinessMetric)"
                
                # Get entity by name and type
                result = self.client.run_query(
                    f"""
                    MATCH (n)
                    WHERE {type_filter} AND n.name = $name
                    RETURN n
                    """,
                    {'name': entity_name}
                )
                
                if not result:
                    return Response(error=f"Entity '{entity_name}' not found")
                    
                return Response(data=result)
            except Exception as e:
                return Response(error=str(e))
        
        # 3. Find relationships
        @resolver(self.resource.operation(
            "find_relationships",
            description="Find relationships between two entities in the knowledge graph",
            schema=JSONSchema({
                "type": "object",
                "properties": {
                    "source": {
                        "type": "string",
                        "description": "Name of the source entity"
                    },
                    "target": {
                        "type": "string",
                        "description": "Name of the target entity"
                    },
                    "max_depth": {
                        "type": "integer",
                        "description": "Maximum path length to consider (default: 3)",
                        "minimum": 1,
                        "maximum": 5
                    },
                    "relationship_types": {
                        "type": "array",
                        "description": "Optional list of relationship types to filter by",
                        "items": {
                            "type": "string"
                        }
                    }
                },
                "required": ["source", "target"]
            })
        ))
        async def find_relationships(
            source: str, 
            target: str, 
            max_depth: int = 3,
            relationship_types: Optional[List[str]] = None
        ) -> Response:
            """Find paths between two entities in the graph."""
            try:
                # Build relationship filter
                rel_filter = ""
                if relationship_types:
                    rel_types = "|".join([f":{rel_type}" for rel_type in relationship_types])
                    rel_filter = f"[{rel_types}]"
                
                # Find paths between entities
                query = f"""
                MATCH path = (source)-{rel_filter}*1..{max_depth}-(target)
                WHERE (source:Table OR source:BusinessTerm OR source:Concept) 
                  AND source.name = $source
                  AND (target:Table OR target:BusinessTerm OR target:Concept)
                  AND target.name = $target
                RETURN path
                LIMIT 5
                """
                
                paths = self.client.run_query(
                    query,
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
        
        # 4. Get table schema
        @resolver(self.resource.operation(
            "get_table_schema",
            description="Get detailed schema information for a table with column metadata",
            schema=JSONSchema({
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to retrieve schema for"
                    },
                    "include_relationships": {
                        "type": "boolean",
                        "description": "Whether to include relationships to other tables (default: false)"
                    }
                },
                "required": ["table_name"]
            })
        ))
        async def get_table_schema(table_name: str, include_relationships: bool = False) -> Response:
            """Get detailed schema information for a specific table with columns."""
            try:
                # Get table and its columns
                schema_query = """
                MATCH (t:Table {name: $table_name})
                OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
                WITH t, collect(c) as columns
                """
                
                if include_relationships:
                    schema_query += """
                    OPTIONAL MATCH (t)-[r]-(related)
                    WHERE type(r) <> 'HAS_COLUMN' AND (related:Table OR related:BusinessTerm)
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
                
                schema = self.client.run_query(
                    schema_query,
                    {'table_name': table_name}
                )
                
                if not schema or not schema.get('table'):
                    return Response(error=f"Table '{table_name}' not found")
                    
                return Response(data=schema)
            except Exception as e:
                return Response(error=str(e))
        
        # 5. Search business terms
        @resolver(self.resource.operation(
            "search_business_terms",
            description="Search for business terms by keyword in name or definition",
            schema=JSONSchema({
                "type": "object",
                "properties": {
                    "keyword": {
                        "type": "string",
                        "description": "Keyword to search for in business term names and definitions"
                    },
                    "exact_match": {
                        "type": "boolean",
                        "description": "Whether to require exact matching (default: false)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return (default: 10)",
                        "minimum": 1,
                        "maximum": 100
                    }
                },
                "required": ["keyword"]
            })
        ))
        async def search_business_terms(
            keyword: str, 
            exact_match: bool = False,
            limit: int = 10
        ) -> Response:
            """Search for business terms by keyword."""
            try:
                # Build search condition
                if exact_match:
                    condition = "bt.name = $keyword OR bt.definition = $keyword"
                else:
                    condition = "bt.name CONTAINS $keyword OR bt.definition CONTAINS $keyword"
                
                # Search for business terms
                terms = self.client.run_query(
                    f"""
                    MATCH (bt:BusinessTerm)
                    WHERE {condition}
                    RETURN bt
                    LIMIT {limit}
                    """,
                    {'keyword': keyword}
                )
                
                return Response(data=terms)
            except Exception as e:
                return Response(error=str(e))
        
        # 6. Get column metadata
        @resolver(self.resource.operation(
            "get_column_metadata",
            description="Get detailed metadata for a specific column with relationships",
            schema=JSONSchema({
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table containing the column"
                    },
                    "column_name": {
                        "type": "string",
                        "description": "Name of the column to retrieve metadata for"
                    }
                },
                "required": ["table_name", "column_name"]
            })
        ))
        async def get_column_metadata(table_name: str, column_name: str) -> Response:
            """Get detailed metadata for a specific column."""
            try:
                metadata = self.client.run_query(
                    """
                    MATCH (c:Column {table_name: $table_name, name: $column_name})
                    OPTIONAL MATCH (c)-[r]->(n)
                    WITH c, collect({relationship: type(r), node: n}) as outgoing
                    OPTIONAL MATCH (m)-[r2]->(c)
                    WITH c, outgoing, collect({relationship: type(r2), node: m}) as incoming
                    RETURN {
                        column: c,
                        outgoing_relationships: outgoing,
                        incoming_relationships: incoming
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
        
        # 7. Recommend tables for query
        @resolver(self.resource.operation(
            "recommend_tables_for_query",
            description="Suggest relevant tables based on a natural language query",
            schema=JSONSchema({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language query to analyze"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of tables to recommend (default: 5)",
                        "minimum": 1,
                        "maximum": 20
                    }
                },
                "required": ["query"]
            })
        ))
        async def recommend_tables_for_query(query: str, limit: int = 5) -> Response:
            """Suggest relevant tables based on a natural language query."""
            try:
                # Extract keywords from the query
                keywords = [word.lower() for word in query.split() if len(word) > 3]
                
                # Build keyword match conditions
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
                
                tables = self.client.run_query(query_text, params)
                
                return Response(data=tables)
            except Exception as e:
                return Response(error=str(e))
        
        # 8. Find join paths
        @resolver(self.resource.operation(
            "find_join_paths",
            description="Discover possible join paths between multiple tables",
            schema=JSONSchema({
                "type": "object",
                "properties": {
                    "table_names": {
                        "type": "array",
                        "description": "List of table names to find join paths between",
                        "items": {
                            "type": "string"
                        }
                    },
                    "max_hops": {
                        "type": "integer",
                        "description": "Maximum number of hops between tables (default: 3)",
                        "minimum": 1,
                        "maximum": 5
                    }
                },
                "required": ["table_names"]
            })
        ))
        async def find_join_paths(table_names: List[str], max_hops: int = 3) -> Response:
            """Discover possible join paths between multiple tables."""
            try:
                if len(table_names) < 2:
                    return Response(error="At least two table names are required")
                
                # Find all possible paths between each pair of tables
                all_paths = []
                
                for i in range(len(table_names)):
                    for j in range(i + 1, len(table_names)):
                        source = table_names[i]
                        target = table_names[j]
                        
                        paths = self.client.run_query(
                            f"""
                            MATCH path = (source:Table {{name: $source}})-[*1..{max_hops}]-(target:Table {{name: $target}})
                            RETURN path
                            LIMIT 3
                            """,
                            {'source': source, 'target': target}
                        )
                        
                        if paths:
                            all_paths.append({
                                'source': source,
                                'target': target,
                                'paths': paths
                            })
                
                return Response(data=all_paths)
            except Exception as e:
                return Response(error=str(e))
        
        # 9. Get entity neighborhood
        @resolver(self.resource.operation(
            "get_entity_neighborhood",
            description="Return all entities directly connected to a given entity",
            schema=JSONSchema({
                "type": "object",
                "properties": {
                    "entity_name": {
                        "type": "string",
                        "description": "Name of the entity to get neighborhood for"
                    },
                    "entity_type": {
                        "type": "string",
                        "description": "Type of the entity (Table, BusinessTerm, etc.)",
                        "enum": ["Table", "BusinessTerm", "Column", "Concept", "BusinessMetric"]
                    },
                    "relationship_types": {
                        "type": "array",
                        "description": "Optional list of relationship types to filter by",
                        "items": {
                            "type": "string"
                        }
                    },
                    "include_properties": {
                        "type": "boolean",
                        "description": "Whether to include entity properties (default: true)"
                    }
                },
                "required": ["entity_name", "entity_type"]
            })
        ))
        async def get_entity_neighborhood(
            entity_name: str, 
            entity_type: str,
            relationship_types: Optional[List[str]] = None,
            include_properties: bool = True
        ) -> Response:
            """Return all entities directly connected to a given entity."""
            try:
                # Build relationship filter
                rel_filter = ""
                if relationship_types:
                    rel_types = "|".join([f":{rel_type}" for rel_type in relationship_types])
                    rel_filter = f"[r:{rel_types}]"
                else:
                    rel_filter = "[r]"
                
                # Get connected entities
                query = f"""
                MATCH (e:{entity_type} {{name: $entity_name}})-{rel_filter}-(connected)
                RETURN {{
                    entity: e,
                    connected: collect({{
                        node: connected,
                        relationship: type(r),
                        direction: case when startNode(r) = e then 'outgoing' else 'incoming' end
                    }})
                }} as result
                """
                
                neighborhood = self.client.run_query(
                    query,
                    {'entity_name': entity_name}
                )
                
                if not neighborhood:
                    return Response(error=f"Entity '{entity_name}' of type '{entity_type}' not found")
                
                # Filter properties if requested
                if not include_properties:
                    # Code to filter properties would go here
                    pass
                
                return Response(data=neighborhood)
            except Exception as e:
                return Response(error=str(e))
        
        # 10. Translate business term
        @resolver(self.resource.operation(
            "translate_business_term",
            description="Map business terms to technical entities (tables/columns)",
            schema=JSONSchema({
                "type": "object",
                "properties": {
                    "term_name": {
                        "type": "string",
                        "description": "Name of the business term to translate"
                    }
                },
                "required": ["term_name"]
            })
        ))
        async def translate_business_term(term_name: str) -> Response:
            """Map business terms to tables/columns."""
            try:
                translation = self.client.run_query(
                    """
                    MATCH (bt:BusinessTerm {name: $term_name})
                    OPTIONAL MATCH (bt)-[r]-(technical)
                    WHERE technical:Table OR technical:Column
                    RETURN {
                        term: bt,
                        technical_entities: collect({
                            entity: technical,
                            relationship: type(r)
                        })
                    } as result
                    """,
                    {'term_name': term_name}
                )
                
                if not translation:
                    return Response(error=f"Business term '{term_name}' not found")
                
                return Response(data=translation)
            except Exception as e:
                return Response(error=str(e))
    
    def get_resource(self) -> Resource:
        """Get the MCP resource instance"""
        return self.resource