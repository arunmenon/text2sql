import os
import logging
from typing import Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks

from src.graph_storage.neo4j_client import Neo4jClient
from src.llm.client import LLMClient
from src.text2sql.engine import TextToSQLEngine
from src.text2sql.models import Text2SQLRequest, Text2SQLResponse
from src.text2sql.utils import track_text2sql_query

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/api/text2sql", tags=["text2sql"])

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

def get_llm_client():
    """Get LLM client from environment variables."""
    api_key = os.getenv("LLM_API_KEY")
    model = os.getenv("LLM_MODEL", "claude-3-opus-20240229")
    
    if not api_key:
        raise HTTPException(status_code=500, detail="LLM API key not configured")
    
    client = LLMClient(api_key=api_key, model=model)
    try:
        yield client
    finally:
        client.close()

def get_text2sql_engine(
    neo4j_client: Neo4jClient = Depends(get_neo4j_client),
    llm_client: LLMClient = Depends(get_llm_client)
):
    """Get Text2SQL engine."""
    return TextToSQLEngine(neo4j_client=neo4j_client, llm_client=llm_client)

# Routes
@router.post("/", response_model=Text2SQLResponse)
async def text_to_sql(
    request: Text2SQLRequest,
    background_tasks: BackgroundTasks,
    engine: TextToSQLEngine = Depends(get_text2sql_engine)
):
    """
    Convert natural language to SQL.
    
    Args:
        request: Text2SQL request with natural language query
        background_tasks: FastAPI background tasks
        engine: Text2SQL engine
        
    Returns:
        Generated SQL and interpretation
    """
    try:
        # Process the query
        result = await engine.process_query(
            request.query, request.tenant_id, request.context
        )
        
        # Track query for analytics
        background_tasks.add_task(
            track_text2sql_query,
            request.tenant_id,
            request.query,
            result.dict()
        )
        
        return result
    except Exception as e:
        logger.error(f"Error in text2sql: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error processing text2sql query: {str(e)}"
        )