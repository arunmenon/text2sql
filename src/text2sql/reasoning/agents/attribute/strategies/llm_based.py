"""
LLM-based attribute resolution strategy.

This module contains a strategy for resolving attributes using a language model.
"""

import json
from typing import Dict, Any, List, Optional

from src.text2sql.reasoning.agents.attribute.base import AttributeResolutionStrategy, AttributeType
from src.llm.client import LLMClient


class LLMBasedResolutionStrategy(AttributeResolutionStrategy):
    """Resolve attributes using language model intelligence."""
    
    def __init__(self, graph_context_provider, llm_client: LLMClient, prompt_loader):
        """
        Initialize LLM-based strategy.
        
        Args:
            graph_context_provider: Provider for semantic graph context
            llm_client: Language model client
            prompt_loader: Prompt template loader
        """
        super().__init__(graph_context_provider)
        self.llm_client = llm_client
        self.prompt_loader = prompt_loader
    
    @property
    def description(self) -> str:
        """Get strategy description."""
        return "Resolves attributes using language model intelligence"
    
    async def resolve(self, attribute_type: str, attribute_value: str, 
                   context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve attribute using language model.
        
        Args:
            attribute_type: Type of attribute (filter, aggregation, etc.)
            attribute_value: Attribute value to resolve
            context: Resolution context with entities, schema, etc.
            
        Returns:
            Resolution result
        """
        # Initialize the result
        result = {
            "attribute_type": attribute_type,
            "attribute_value": attribute_value,
            "resolved_to": None,
            "confidence": 0.0,
            "strategy": self.strategy_name,
            "metadata": {}
        }
        
        # Get schema information
        entities = context.get("entities", {})
        schema_context = self._get_schema_context(context)
        
        # Convert attribute value to string if it's a dict
        attribute_text = attribute_value
        attribute_metadata = {}
        
        if isinstance(attribute_value, dict):
            attribute_metadata = attribute_value.copy()
            attribute_text = attribute_value.get("attribute_value", str(attribute_value))
        
        # Prepare schema information for the prompt
        schema_info = []
        for table_name, schema in schema_context.items():
            schema_info.append({
                "table": table_name,
                "columns": [
                    {
                        "name": col.get("name"),
                        "data_type": col.get("data_type"),
                        "description": col.get("description", "")
                    }
                    for col in schema.get("columns", [])
                ]
            })
        
        # Format entity information
        entity_info = [
            {
                "name": name,
                "table": info.get("resolved_to")
            }
            for name, info in entities.items()
            if "resolved_to" in info
        ]
        
        # Prepare prompt for attribute resolution
        prompt = self.prompt_loader.format_prompt(
            f"attribute_resolution_{attribute_type}",
            query=context.get("query", ""),
            attribute_text=attribute_text,
            attribute_type=attribute_type,
            attribute_metadata=json.dumps(attribute_metadata, indent=2),
            schema=json.dumps(schema_info, indent=2),
            entities=json.dumps(entity_info, indent=2)
        )
        
        # Define schema for structured response
        schema = {
            "type": "object",
            "properties": {
                "sql_component": {"type": "string"},
                "explanation": {"type": "string"},
                "confidence": {"type": "number"},
                "table": {"type": "string"},
                "column": {"type": "string"}
            },
            "required": ["sql_component", "confidence"]
        }
        
        try:
            # Generate structured response
            response = await self.llm_client.generate_structured(prompt, schema)
            
            if response and "sql_component" in response:
                result["resolved_to"] = response["sql_component"]
                result["confidence"] = response.get("confidence", 0.7)
                result["metadata"]["explanation"] = response.get("explanation", "")
                result["metadata"]["table"] = response.get("table", "")
                result["metadata"]["column"] = response.get("column", "")
                
                return result
        except Exception as e:
            self.logger.error(f"Error in LLM attribute resolution: {e}")
        
        return result
    
    def _get_schema_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Get schema context for entity tables."""
        schema_context = {}
        
        # Get all entities and their table schemas
        entities = context.get("entities", {})
        
        for entity_name, entity_info in entities.items():
            if "resolved_to" in entity_info and entity_info["resolved_to"]:
                table_name = entity_info["resolved_to"]
                
                # Get table schema using the graph context provider
                table_schema = self.graph_context.get_table_schema(table_name)
                
                if table_schema:
                    schema_context[table_name] = table_schema
        
        return schema_context