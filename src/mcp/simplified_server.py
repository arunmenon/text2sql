"""
Simplified MCP server for Knowledge Graph

This module implements a simplified MCP server without depending on the
specific MCP package structure.
"""
import argparse
import logging
import os
import json
from typing import Dict, Any, List
import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn
from neo4j import GraphDatabase

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Neo4j connection parameters
NEO4J_URI = os.environ.get('NEO4J_URI', 'neo4j://localhost:7687')
NEO4J_USER = os.environ.get('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD', 'password')

# Create FastAPI app
app = FastAPI(title="Knowledge Graph MCP Server", 
              description="MCP server for exposing the Knowledge Graph")

# Neo4j client class
class Neo4jClient:
    """Client for Neo4j database."""
    
    def __init__(self, uri, username, password):
        """Initialize the Neo4j client."""
        self.uri = uri
        self.username = username
        self.password = password
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        logger.info(f"Connected to Neo4j at {uri}")
    
    def close(self):
        """Close the Neo4j connection."""
        self.driver.close()
    
    def run_query(self, query, params=None):
        """Run a Cypher query."""
        params = params or {}
        try:
            with self.driver.session() as session:
                result = session.run(query, params)
                records = [dict(record) for record in result]
                return records
        except Exception as e:
            logger.error(f"Query failed: {str(e)}")
            raise

# Pydantic models for API
class QueryRequest(BaseModel):
    """Cypher query request."""
    query: str
    parameters: Dict[str, Any] = {}

class TableSchemaRequest(BaseModel):
    """Table schema request."""
    table_name: str
    include_relationships: bool = False

class RelationshipRequest(BaseModel):
    """Relationship request."""
    source: str
    target: str
    max_depth: int = 3

class BusinessTermRequest(BaseModel):
    """Business term search request."""
    keyword: str

class EntityRequest(BaseModel):
    """Entity request."""
    entity_name: str
    entity_type: str = None

class RecommendTablesRequest(BaseModel):
    """Recommend tables request."""
    query: str
    limit: int = 5

class JoinPathRequest(BaseModel):
    """Join path request."""
    table_names: List[str]
    max_hops: int = 3

# Global Neo4j client
neo4j_client = None

@app.on_event("startup")
async def startup_event():
    """Connect to Neo4j on startup."""
    global neo4j_client
    try:
        neo4j_client = Neo4jClient(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        
        # Test connection
        result = neo4j_client.run_query("MATCH (n) RETURN count(n) as count LIMIT 1")
        node_count = result[0]["count"]
        logger.info(f"Connected to Neo4j: Found {node_count} nodes")
    except Exception as e:
        logger.error(f"Failed to connect to Neo4j: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Close Neo4j connection on shutdown."""
    global neo4j_client
    if neo4j_client:
        neo4j_client.close()
        logger.info("Neo4j connection closed")

# MCP compatible endpoints
@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "Knowledge Graph MCP Server"}

@app.get("/mcp")
async def mcp_info():
    """MCP server info."""
    return {
        "name": "Knowledge Graph MCP Server", 
        "version": "1.0.0",
        "resources": [
            {
                "name": "knowledge_graph",
                "description": "Access to the semantic knowledge graph"
            }
        ]
    }

@app.get("/mcp/resources")
async def list_resources():
    """List available resources."""
    return {
        "resources": [
            {
                "name": "knowledge_graph",
                "description": "Access to the semantic knowledge graph"
            }
        ]
    }

@app.get("/mcp/tools")
async def list_tools():
    """List available tools."""
    return {
        "tools": [
            {
                "name": "knowledge_graph.query",
                "description": "Run a Cypher query against the knowledge graph",
                "schema": {
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
                }
            },
            {
                "name": "knowledge_graph.get_table_schema",
                "description": "Get detailed schema information for a table with column metadata",
                "schema": {
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
                }
            },
            {
                "name": "knowledge_graph.find_relationships",
                "description": "Find relationships between two entities in the knowledge graph",
                "schema": {
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
                            "description": "Maximum path length to consider (default: 3)"
                        }
                    },
                    "required": ["source", "target"]
                }
            },
            {
                "name": "knowledge_graph.search_business_terms",
                "description": "Search for business terms by keyword",
                "schema": {
                    "type": "object",
                    "properties": {
                        "keyword": {
                            "type": "string",
                            "description": "Search keyword for business terms"
                        }
                    },
                    "required": ["keyword"]
                }
            },
            {
                "name": "knowledge_graph.recommend_tables_for_query",
                "description": "Suggest relevant tables based on a natural language query",
                "schema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Natural language query to analyze"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of tables to recommend (default: 5)"
                        }
                    },
                    "required": ["query"]
                }
            }
        ]
    }

# Tool implementation endpoints
@app.post("/tools/knowledge_graph.query")
async def kg_query(request: QueryRequest):
    """Run a Cypher query."""
    try:
        result = neo4j_client.run_query(request.query, request.parameters)
        return {"data": result}
    except Exception as e:
        logger.error(f"Query failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tools/knowledge_graph.get_table_schema")
async def kg_get_table_schema(request: TableSchemaRequest):
    """Get table schema."""
    try:
        # Build schema query
        schema_query = """
        MATCH (t:Table {name: $table_name})
        OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
        WITH t, collect(c) as columns
        """
        
        if request.include_relationships:
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
        
        result = neo4j_client.run_query(schema_query, {"table_name": request.table_name})
        
        if not result or not result[0].get("result", {}).get("table"):
            raise HTTPException(status_code=404, detail=f"Table '{request.table_name}' not found")
            
        return {"data": result[0]["result"]}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get table schema failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tools/knowledge_graph.find_relationships")
async def kg_find_relationships(request: RelationshipRequest):
    """Find relationships between entities."""
    try:
        # Find paths between entities
        rel_query = f"""
        MATCH path = (source:Table)-[*1..{request.max_depth}]-(target:Table)
        WHERE source.name = $source AND target.name = $target
        RETURN path
        LIMIT 5
        """
        
        paths = neo4j_client.run_query(rel_query, {
            "source": request.source, 
            "target": request.target
        })
        
        if not paths:
            return {
                "data": [],
                "message": f"No paths found between '{request.source}' and '{request.target}'"
            }
            
        return {"data": paths}
    except Exception as e:
        logger.error(f"Find relationships failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tools/knowledge_graph.search_business_terms")
async def kg_search_business_terms(request: BusinessTermRequest):
    """Search for business terms by keyword."""
    try:
        # First try with BusinessTerm label
        term_query_business_term = """
        MATCH (bt:BusinessTerm)
        WHERE bt.name CONTAINS $keyword OR bt.definition CONTAINS $keyword
        RETURN bt LIMIT 10
        """
        
        try:
            terms = neo4j_client.run_query(term_query_business_term, {"keyword": request.keyword})
            if terms:
                return {"data": terms}
        except Exception:
            logger.warning("BusinessTerm label not found, trying BusinessMetric")
        
        # Try with BusinessMetric label if BusinessTerm doesn't exist
        term_query_metric = """
        MATCH (bt:BusinessMetric)
        WHERE bt.name CONTAINS $keyword OR bt.definition CONTAINS $keyword
        RETURN bt LIMIT 10
        """
        
        terms = neo4j_client.run_query(term_query_metric, {"keyword": request.keyword})
        return {"data": terms}
    except Exception as e:
        logger.error(f"Search business terms failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tools/knowledge_graph.recommend_tables_for_query")
async def kg_recommend_tables(request: RecommendTablesRequest):
    """Recommend tables for a query."""
    try:
        # Extract keywords from the query
        keywords = [word.lower() for word in request.query.split() if len(word) > 3]
        
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
            LIMIT {request.limit}
            """
        else:
            query_text = f"""
            MATCH (t:Table)
            RETURN t
            LIMIT {request.limit}
            """
        
        tables = neo4j_client.run_query(query_text, params)
        
        return {"data": tables}
    except Exception as e:
        logger.error(f"Recommend tables failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

def run_server(host="0.0.0.0", port=8234):
    """Run the server."""
    uvicorn.run(app, host=host, port=port)

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Simplified Knowledge Graph MCP Server")
    parser.add_argument("--host", default="0.0.0.0", help="Server host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8234, help="Server port (default: 8234)")
    parser.add_argument("--neo4j-uri", help="Neo4j connection URI")
    parser.add_argument("--neo4j-user", help="Neo4j username")
    parser.add_argument("--neo4j-password", help="Neo4j password")
    
    args = parser.parse_args()
    
    # Update environment variables if provided
    if args.neo4j_uri:
        os.environ["NEO4J_URI"] = args.neo4j_uri
    if args.neo4j_user:
        os.environ["NEO4J_USER"] = args.neo4j_user
    if args.neo4j_password:
        os.environ["NEO4J_PASSWORD"] = args.neo4j_password
    
    # Run server
    run_server(host=args.host, port=args.port)

if __name__ == "__main__":
    main()