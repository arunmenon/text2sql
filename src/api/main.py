"""
Text2SQL API

FastAPI-based API for Text2SQL platform.
"""
import logging
import os
from typing import Dict, List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel, Field

# Load environment variables
load_dotenv()

# Import components
from src.schema_extraction.bigquery.extractor import BigQuerySchemaExtractor
from src.relationship_inference.statistical.overlap_analyzer import OverlapAnalyzer
from src.relationship_inference.name_pattern.pattern_matcher import PatternMatcher
from src.graph_storage.neo4j_client import Neo4jClient

# Import routes
from src.api.routes.text2sql import router as text2sql_router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Text2SQL API",
    description="API for Text2SQL platform with KB curation",
    version="0.1.0"
)

# Include routers
app.include_router(text2sql_router)

# Model definitions
class TenantCreate(BaseModel):
    tenant_id: str
    name: str
    description: Optional[str] = None

class DatasetExtract(BaseModel):
    tenant_id: str
    project_id: str
    dataset_id: str

class RelationshipInfer(BaseModel):
    tenant_id: str
    project_id: str
    dataset_id: str
    min_confidence: float = Field(0.7, ge=0, le=1)

class QueryPathRequest(BaseModel):
    tenant_id: str
    source_table: str
    target_table: str
    min_confidence: float = Field(0.5, ge=0, le=1)

# Dependencies
def get_neo4j_client():
    """Get Neo4j client from environment variables."""
    uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")
    
    client = Neo4jClient(uri, username, password)
    try:
        yield client
    finally:
        client.close()

# Background tasks
async def extract_schema_task(
    tenant_id: str,
    project_id: str,
    dataset_id: str,
    neo4j_client: Neo4jClient
):
    """
    Background task to extract schema from BigQuery and store in Neo4j.
    
    Args:
        tenant_id: Tenant ID
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        neo4j_client: Neo4j client
    """
    logger.info(f"Starting schema extraction for {tenant_id}/{project_id}/{dataset_id}")
    
    try:
        # Extract schema
        extractor = BigQuerySchemaExtractor(project_id)
        schema = await extractor.extract_full_schema(dataset_id)
        
        # Create dataset in Neo4j
        neo4j_client.create_dataset(tenant_id, dataset_id)
        
        # Create tables and columns
        for table in schema["tables"]:
            table_name = table["table_name"]
            
            # Create table
            neo4j_client.create_table(tenant_id, dataset_id, table)
            
            # Create columns
            for column in table["columns"]:
                neo4j_client.create_column(
                    tenant_id,
                    dataset_id,
                    table_name,
                    column
                )
        
        logger.info(f"Completed schema extraction for {tenant_id}/{project_id}/{dataset_id}")
    except Exception as e:
        logger.error(f"Error in schema extraction: {e}")
        raise

async def infer_relationships_task(
    tenant_id: str,
    project_id: str,
    dataset_id: str,
    min_confidence: float,
    neo4j_client: Neo4jClient
):
    """
    Background task to infer relationships between tables.
    
    Args:
        tenant_id: Tenant ID
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        min_confidence: Minimum confidence threshold
        neo4j_client: Neo4j client
    """
    logger.info(f"Starting relationship inference for {tenant_id}/{project_id}/{dataset_id}")
    
    try:
        # Get tables from Neo4j
        tables = neo4j_client.get_tables_for_tenant(tenant_id)
        
        # Add columns to tables
        for table in tables:
            table_name = table["name"]
            columns = neo4j_client.get_columns_for_table(tenant_id, table_name)
            table["columns"] = columns
        
        # Infer relationships using name patterns
        pattern_matcher = PatternMatcher()
        pattern_relationships = pattern_matcher.infer_relationships(tables)
        
        # Store pattern-based relationships
        for rel in pattern_relationships:
            if rel["confidence"] >= min_confidence:
                neo4j_client.create_relationship(
                    tenant_id,
                    rel["source_table"],
                    rel["source_column"],
                    rel["target_table"],
                    rel["target_column"],
                    rel["confidence"],
                    rel["detection_method"]
                )
        
        # Infer relationships using statistical analysis
        overlap_analyzer = OverlapAnalyzer(project_id)
        stat_relationships = await overlap_analyzer.find_candidate_relationships(
            dataset_id,
            tables,
            min_confidence
        )
        
        # Store statistical relationships
        for rel in stat_relationships:
            neo4j_client.create_relationship(
                tenant_id,
                rel["source_table"],
                rel["source_column"],
                rel["target_table"],
                rel["target_column"],
                rel["confidence"],
                rel["detection_method"]
            )
        
        logger.info(f"Completed relationship inference for {tenant_id}/{project_id}/{dataset_id}")
    except Exception as e:
        logger.error(f"Error in relationship inference: {e}")
        raise

# API endpoints
@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "Text2SQL API is running"}

@app.post("/api/tenants")
async def create_tenant(
    tenant: TenantCreate,
    neo4j_client: Neo4jClient = Depends(get_neo4j_client)
):
    """
    Create a new tenant.
    
    Args:
        tenant: Tenant data
        neo4j_client: Neo4j client
        
    Returns:
        Created tenant data
    """
    try:
        # Initialize Neo4j schema
        neo4j_client.create_schema_constraints()
        
        # Create tenant
        result = neo4j_client.create_tenant(
            tenant.tenant_id,
            tenant.name,
            tenant.description
        )
        
        return result
    except Exception as e:
        logger.error(f"Error creating tenant: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error creating tenant: {str(e)}"
        )

@app.post("/api/schema/extract")
async def extract_schema(
    request: DatasetExtract,
    background_tasks: BackgroundTasks,
    neo4j_client: Neo4jClient = Depends(get_neo4j_client)
):
    """
    Extract schema from BigQuery and store in Neo4j.
    
    Args:
        request: Dataset extraction request
        background_tasks: FastAPI background tasks
        neo4j_client: Neo4j client
        
    Returns:
        Status message
    """
    try:
        # Queue background task
        background_tasks.add_task(
            extract_schema_task,
            request.tenant_id,
            request.project_id,
            request.dataset_id,
            neo4j_client
        )
        
        return {
            "status": "started",
            "message": f"Schema extraction started for {request.tenant_id}/{request.project_id}/{request.dataset_id}"
        }
    except Exception as e:
        logger.error(f"Error starting schema extraction: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error starting schema extraction: {str(e)}"
        )

@app.post("/api/relationships/infer")
async def infer_relationships(
    request: RelationshipInfer,
    background_tasks: BackgroundTasks,
    neo4j_client: Neo4jClient = Depends(get_neo4j_client)
):
    """
    Infer relationships between tables.
    
    Args:
        request: Relationship inference request
        background_tasks: FastAPI background tasks
        neo4j_client: Neo4j client
        
    Returns:
        Status message
    """
    try:
        # Queue background task
        background_tasks.add_task(
            infer_relationships_task,
            request.tenant_id,
            request.project_id,
            request.dataset_id,
            request.min_confidence,
            neo4j_client
        )
        
        return {
            "status": "started",
            "message": f"Relationship inference started for {request.tenant_id}/{request.project_id}/{request.dataset_id}"
        }
    except Exception as e:
        logger.error(f"Error starting relationship inference: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error starting relationship inference: {str(e)}"
        )

@app.get("/api/schema/summary/{tenant_id}")
async def get_schema_summary(
    tenant_id: str,
    neo4j_client: Neo4jClient = Depends(get_neo4j_client)
):
    """
    Get schema summary for a tenant.
    
    Args:
        tenant_id: Tenant ID
        neo4j_client: Neo4j client
        
    Returns:
        Schema summary
    """
    try:
        summary = neo4j_client.get_schema_summary(tenant_id)
        return summary
    except Exception as e:
        logger.error(f"Error getting schema summary: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting schema summary: {str(e)}"
        )

@app.get("/api/schema/tables/{tenant_id}")
async def get_tables(
    tenant_id: str,
    neo4j_client: Neo4jClient = Depends(get_neo4j_client)
):
    """
    Get all tables for a tenant.
    
    Args:
        tenant_id: Tenant ID
        neo4j_client: Neo4j client
        
    Returns:
        List of tables
    """
    try:
        tables = neo4j_client.get_tables_for_tenant(tenant_id)
        return tables
    except Exception as e:
        logger.error(f"Error getting tables: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting tables: {str(e)}"
        )

@app.get("/api/schema/tables/{tenant_id}/{table_name}")
async def get_table_details(
    tenant_id: str,
    table_name: str,
    neo4j_client: Neo4jClient = Depends(get_neo4j_client)
):
    """
    Get table details including columns and relationships.
    
    Args:
        tenant_id: Tenant ID
        table_name: Table name
        neo4j_client: Neo4j client
        
    Returns:
        Table details
    """
    try:
        # Get columns
        columns = neo4j_client.get_columns_for_table(tenant_id, table_name)
        
        # Get relationships
        relationships = neo4j_client.get_relationships_for_table(tenant_id, table_name)
        
        return {
            "table_name": table_name,
            "columns": columns,
            "relationships": relationships
        }
    except Exception as e:
        logger.error(f"Error getting table details: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting table details: {str(e)}"
        )

@app.post("/api/schema/join_path")
async def find_join_path(
    request: QueryPathRequest,
    neo4j_client: Neo4jClient = Depends(get_neo4j_client)
):
    """
    Find join paths between two tables.
    
    Args:
        request: Join path request
        neo4j_client: Neo4j client
        
    Returns:
        Possible join paths
    """
    try:
        paths = neo4j_client.find_join_path(
            request.tenant_id,
            request.source_table,
            request.target_table,
            request.min_confidence
        )
        
        return {
            "source_table": request.source_table,
            "target_table": request.target_table,
            "paths": paths
        }
    except Exception as e:
        logger.error(f"Error finding join path: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error finding join path: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    uvicorn.run("src.api.main:app", host=host, port=port, reload=True)