import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

from src.llm.client import LLMClient
from src.graph_storage.neo4j_client import Neo4jClient
from src.text2sql.components.nl_understanding import NLUnderstandingComponent
from src.text2sql.components.query_resolution import QueryResolutionComponent
from src.text2sql.components.sql_generation import SQLGenerationComponent
from src.text2sql.models import Text2SQLResponse, SQLResult, StructuredQuery, ResolvedQuery

logger = logging.getLogger(__name__)

class TextToSQLEngine:
    """Main engine for converting natural language to SQL"""
    
    def __init__(self, neo4j_client: Neo4jClient, llm_client: LLMClient):
        """
        Initialize Text2SQL engine.
        
        Args:
            neo4j_client: Client for accessing schema information
            llm_client: Client for LLM interactions
        """
        # Initialize components
        self.nl_understanding = NLUnderstandingComponent(llm_client)
        self.query_resolution = QueryResolutionComponent(neo4j_client, llm_client)
        self.sql_generation = SQLGenerationComponent(llm_client)
        
        # Store clients
        self.neo4j_client = neo4j_client
        self.llm_client = llm_client
        
    async def process_query(
        self, natural_language_query: str, tenant_id: str, 
        context: Optional[Dict[str, Any]] = None
    ) -> Text2SQLResponse:
        """
        Process natural language query end-to-end.
        
        Args:
            natural_language_query: Natural language query
            tenant_id: Tenant ID
            context: Optional additional context
            
        Returns:
            Complete response with SQL and metadata
        """
        try:
            logger.info(f"Processing query: {natural_language_query}")
            
            # Step 1: Parse and understand the natural language query
            structured_query = await self.nl_understanding.parse_query(
                natural_language_query, tenant_id
            )
            logger.debug(f"Structured query: {structured_query}")
            
            # Step 2: Resolve and disambiguate against schema
            resolved_query = await self.query_resolution.resolve_query(
                structured_query, tenant_id
            )
            logger.debug(f"Resolved query: {resolved_query}")
            
            # Step 3: Generate SQL
            sql_results = await self.sql_generation.generate_sql(
                resolved_query, structured_query
            )
            logger.debug(f"Generated {len(sql_results)} SQL variants")
            
            # Step 4: Format response
            response = self._format_response(
                sql_results, resolved_query, structured_query, natural_language_query
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Error processing query: {e}", exc_info=True)
            raise
    
    def _format_response(
        self, 
        sql_results: List[SQLResult], 
        resolved_query: ResolvedQuery, 
        structured_query: StructuredQuery, 
        original_query: str
    ) -> Text2SQLResponse:
        """Format the complete response to the user"""
        # Determine primary interpretation if multiple
        primary_sql = next((sql for sql in sql_results if sql.is_primary), sql_results[0] if sql_results else None)
        
        # Get entities resolution information
        entities_resolved = {
            entity_name: entity.table_name 
            for entity_name, entity in resolved_query.resolved_entities.items()
        }
        
        # Create response
        response = Text2SQLResponse(
            original_query=original_query,
            interpreted_as=structured_query.primary_intent,
            ambiguity_level=structured_query.ambiguity_assessment.score,
            sql_results=sql_results,
            primary_interpretation=primary_sql,
            multiple_interpretations=(len(sql_results) > 1),
            entities_resolved=entities_resolved,
            metadata={
                "timestamp": datetime.now().isoformat(),
                "engine_version": "1.0",
                "processing_time_ms": None  # Would be set from actual timing data
            }
        )
        
        return response