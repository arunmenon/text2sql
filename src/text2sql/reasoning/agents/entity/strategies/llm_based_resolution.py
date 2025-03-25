"""
LLM-based resolution strategy for entity resolution.

This module implements a strategy for resolving entities using
a language model when other strategies fail.
"""

from typing import Dict, Any
import logging
import json

from src.text2sql.reasoning.agents.entity.base import EntityResolutionStrategy
from src.text2sql.reasoning.registry import StrategyRegistry
from src.llm.client import LLMClient
from src.text2sql.utils.prompt_loader import PromptLoader


@StrategyRegistry.register("llm_based_resolution")
class LLMBasedResolutionStrategy(EntityResolutionStrategy):
    """Strategy for resolving entities using LLM."""
    
    def __init__(self, graph_context_provider, llm_client, prompt_loader, **kwargs):
        """
        Initialize LLM-based resolution strategy.
        
        Args:
            graph_context_provider: Provider for semantic graph context
            llm_client: LLM client for text generation
            prompt_loader: Loader for prompt templates
            **kwargs: Additional configuration parameters
        """
        super().__init__(graph_context_provider)
        self.llm_client = llm_client
        self.prompt_loader = prompt_loader
        self.prompt_template = kwargs.get("prompt_template", "entity_resolution")
        self.confidence_threshold = kwargs.get("confidence_threshold", 0.4)
    
    @property
    def description(self) -> str:
        """Get strategy description."""
        return "Uses language model to resolve entities based on query context and available schema"
    
    async def resolve(self, entity: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve entity using LLM.
        
        Args:
            entity: Entity to resolve
            context: Resolution context
            
        Returns:
            Resolution result
        """
        tenant_id = context.get("tenant_id", "default")
        query = context.get("query", "")
        intent = context.get("intent", {}).get("intent_type", "selection")
        
        # Get schema context for LLM prompt
        graph_context = self.graph_context.get_graph_enhanced_context(tenant_id)
        tables_info = self._format_tables_info(graph_context)
        
        # Create prompt for LLM resolution
        prompt = self.prompt_loader.format_prompt(
            self.prompt_template,
            query=query,
            entity_name=entity,
            intent_type=intent,
            tables_info=tables_info
        )
        
        # Define schema for structured response
        schema = {
            "type": "object",
            "properties": {
                "table_name": {"type": "string"},
                "confidence": {"type": "number"},
                "reasoning": {"type": "string"}
            },
            "required": ["table_name", "confidence"]
        }
        
        try:
            # Generate structured response
            response = await self.llm_client.generate_structured(prompt, schema)
            
            if response and "table_name" in response:
                table_name = response["table_name"]
                confidence = response.get("confidence", 0.7)
                
                # Only return if confidence meets threshold
                if confidence >= self.confidence_threshold:
                    return {
                        "entity": entity,
                        "resolved_to": table_name,
                        "resolution_type": "llm",
                        "confidence": confidence,
                        "strategy": self.strategy_name,
                        "metadata": {
                            "reasoning": response.get("reasoning", ""),
                            "llm_confidence": confidence
                        }
                    }
        except Exception as e:
            self.logger.error(f"Error in LLM resolution: {e}")
        
        return {
            "entity": entity,
            "resolved_to": None,
            "resolution_type": None,
            "confidence": 0.0,
            "strategy": self.strategy_name,
            "metadata": {}
        }
    
    def _format_tables_info(self, graph_context: Dict[str, Any]) -> str:
        """
        Format tables information for LLM prompts.
        
        Args:
            graph_context: Graph context information
            
        Returns:
            Formatted string with tables information
        """
        tables_info = []
        
        for table_name, table_data in graph_context.get("tables", {}).items():
            description = table_data.get("description", "No description available")
            columns = table_data.get("columns", [])
            
            column_str = ", ".join([
                f"{col.get('name')} ({col.get('data_type', 'unknown')})" 
                for col in columns[:5]  # Limit to first 5 columns
            ])
            
            tables_info.append(f"- Table: {table_name}\n  Description: {description}\n  Columns: {column_str}")
        
        return "\n".join(tables_info)