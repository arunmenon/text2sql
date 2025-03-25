"""
Transparent Query Engine for Text2SQL.

This module provides a text-to-SQL engine that exposes chain-of-thought
reasoning during query processing for improved interpretability.
"""

import logging
import uuid
from typing import Dict, List, Any, Optional

from src.llm.client import LLMClient
from src.graph_storage.neo4j_client import Neo4jClient
from src.text2sql.utils.graph_context_provider import SemanticGraphContextProvider
from src.text2sql.utils.prompt_loader import PromptLoader
from src.text2sql.reasoning.models import ReasoningStream
from src.text2sql.reasoning.knowledge_boundary import BoundaryRegistry
from src.text2sql.reasoning.agents.intent_agent import IntentAgent
from src.text2sql.reasoning.agents.entity import EntityAgent
from src.text2sql.reasoning.agents.relationship import RelationshipAgent
from src.text2sql.reasoning.agents.sql_agent import SQLAgent
from src.text2sql.models import Text2SQLResponse, SQLResult


class TransparentQueryEngine:
    """
    Transparent query engine that exposes reasoning during the query journey.
    
    This engine uses specialized agents with visible reasoning for converting 
    natural language to SQL, leveraging the semantic graph for improved understanding.
    """
    
    def __init__(self, llm_client: LLMClient, neo4j_client: Neo4jClient):
        """
        Initialize transparent query engine.
        
        Args:
            llm_client: LLM client for text generation
            neo4j_client: Neo4j client for graph access
        """
        # Initialize shared context providers
        self.graph_context = SemanticGraphContextProvider(neo4j_client)
        self.prompt_loader = PromptLoader()
        
        # Initialize specialized agents
        self.intent_agent = IntentAgent(llm_client, self.prompt_loader)
        self.entity_agent = EntityAgent(llm_client, self.graph_context, self.prompt_loader)
        self.relationship_agent = RelationshipAgent(llm_client, self.graph_context, self.prompt_loader)
        self.sql_agent = SQLAgent(llm_client, self.graph_context, self.prompt_loader)
        
        # Will add more agents as we implement them:
        # self.attribute_agent = AttributeAgent(...)
        
        # Store clients
        self.llm_client = llm_client
        self.neo4j_client = neo4j_client
        self.logger = logging.getLogger(__name__)
    
    async def process_query(self, query: str, tenant_id: str, context: Optional[Dict[str, Any]] = None) -> Text2SQLResponse:
        """
        Process a natural language query with transparent reasoning.
        
        Args:
            query: Natural language query
            tenant_id: Tenant ID
            context: Optional additional context
            
        Returns:
            Text2SQL response with SQL and reasoning
        """
        self.logger.info(f"Processing transparent query: {query}")
        
        # Initialize reasoning stream
        reasoning_stream = ReasoningStream(
            query_id=str(uuid.uuid4()),
            query_text=query
        )
        
        # Initialize processing context
        processing_context = {
            "query": query,
            "tenant_id": tenant_id,
            "context": context or {},
            "boundary_registry": BoundaryRegistry()
        }
        
        # Step 1: Intent Analysis
        intent_result = await self.intent_agent.process(processing_context, reasoning_stream)
        processing_context["intent"] = intent_result
        
        # Step 2: Entity Recognition
        entities_result = await self.entity_agent.process(processing_context, reasoning_stream)
        processing_context["entities"] = entities_result
        
        # Step 3: Relationship Discovery
        relationships_result = await self.relationship_agent.process(processing_context, reasoning_stream)
        processing_context["relationships"] = relationships_result
        
        # Step 4: SQL Generation
        sql_result = await self.sql_agent.process(processing_context, reasoning_stream)
        processing_context["sql_result"] = sql_result
        
        # For now, we'll use a simplified flow until we implement all agents
        # In the future, we'll add:
        # - Attribute Recognition
        
        # Use the SQL result from the SQLAgent
        primary_sql = SQLResult(
            sql=sql_result["sql"],
            explanation=sql_result["explanation"],
            assumptions=[
                f"Based on intent: {intent_result['intent_type']}",
                f"Identified entities: {', '.join(entities_result.keys())}",
                f"Confidence: {sql_result['confidence']:.2f}",
                f"Approach: {sql_result['approach']}"
            ],
            approach=sql_result["approach"],
            is_primary=True
        )
        
        # Create alternative SQL results if available
        alternative_sqls = []
        for alt in sql_result.get("alternatives", []):
            if "sql" in alt and alt["sql"]:
                alternative_sqls.append(
                    SQLResult(
                        sql=alt["sql"],
                        explanation=alt.get("explanation", "Alternative interpretation"),
                        assumptions=[
                            f"Alternative approach: {alt.get('approach', 'Unknown')}",
                            f"Confidence: {alt.get('confidence', 0.5):.2f}"
                        ],
                        approach=alt.get("approach", "Alternative generation"),
                        is_primary=False
                    )
                )
        
        # Check if we have knowledge boundaries that require clarification
        boundary_registry = processing_context["boundary_registry"]
        requires_clarification = boundary_registry.requires_clarification()
        
        # Mark reasoning as complete
        reasoning_stream.complete_reasoning()
        
        # Calculate overall ambiguity level
        confidence_values = [
            intent_result["confidence"], 
            max([e["confidence"] for e in entities_result.values()]) if entities_result else 0.5,
            sql_result["confidence"]
        ]
        overall_confidence = min(confidence_values)
        ambiguity_level = 1.0 - overall_confidence
        
        # Combine all SQL results (primary and alternatives)
        all_sql_results = [primary_sql] + alternative_sqls
        
        # Determine if we have multiple interpretations
        has_multiple_interpretations = len(alternative_sqls) > 0
        
        # Prepare response
        response = Text2SQLResponse(
            original_query=query,
            interpreted_as=f"Query with intent: {intent_result['intent_type']}",
            ambiguity_level=ambiguity_level,
            sql_results=all_sql_results,
            primary_interpretation=primary_sql,
            multiple_interpretations=has_multiple_interpretations,
            entities_resolved={
                entity: info["resolved_to"] 
                for entity, info in entities_result.items()
            },
            metadata={
                "reasoning_stream": reasoning_stream.dict(),
                "intent": intent_result,
                "entities": entities_result,
                "relationships": relationships_result,
                "sql_generation": sql_result,
                "knowledge_boundaries": boundary_registry.to_dict(),
                "requires_clarification": requires_clarification,
                "transparent_engine": True,
                "agent_based": True  # Flag that this is using the agent-based approach
            }
        )
        
        return response
"""