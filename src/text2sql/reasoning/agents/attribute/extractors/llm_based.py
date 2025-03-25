"""
LLM-based attribute extractor.

This module contains an extractor that uses a language model to
identify attributes in natural language queries.
"""

from typing import Dict, List, Any

from src.text2sql.reasoning.agents.attribute.base import AttributeType
from src.llm.client import LLMClient


class LLMBasedAttributeExtractor:
    """Extract attributes using language model intelligence."""
    
    def __init__(self, llm_client: LLMClient, prompt_loader):
        """
        Initialize the LLM-based extractor.
        
        Args:
            llm_client: Language model client
            prompt_loader: Prompt template loader
        """
        self.llm_client = llm_client
        self.prompt_loader = prompt_loader
    
    @property
    def name(self) -> str:
        """Get extractor name."""
        return "LLM-Based Attribute Extractor"
    
    async def extract(self, query: str, entity_context: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract attributes using language model.
        
        Args:
            query: Natural language query
            entity_context: Context with entity information
            
        Returns:
            Dictionary of extracted attributes by type
        """
        # Format entity information for the prompt
        entity_info = []
        for entity_name, entity_data in entity_context.get("entities", {}).items():
            if "resolved_to" in entity_data:
                entity_info.append({
                    "name": entity_name,
                    "resolved_to": entity_data["resolved_to"],
                    "columns": entity_context.get("schema", {}).get("tables", {}).get(
                        entity_data["resolved_to"], {}).get("columns", [])
                })
        
        # Prepare prompt for attribute extraction
        prompt = self.prompt_loader.format_prompt(
            "attribute_extraction",
            query=query,
            entities=entity_info
        )
        
        # Define schema for structured response
        schema = {
            "type": "object",
            "properties": {
                "filters": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "attribute_value": {"type": "string"},
                            "operator": {"type": "string"},
                            "value": {"type": "object"},
                            "column_hint": {"type": "string"},
                            "entity_name": {"type": "string"},
                            "confidence": {"type": "number"}
                        }
                    }
                },
                "aggregations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "attribute_value": {"type": "string"},
                            "function": {"type": "string"},
                            "target": {"type": "string"},
                            "confidence": {"type": "number"}
                        }
                    }
                },
                "groupings": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "attribute_value": {"type": "string"},
                            "target": {"type": "string"},
                            "entity_name": {"type": "string"},
                            "confidence": {"type": "number"}
                        }
                    }
                },
                "sortings": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "attribute_value": {"type": "string"},
                            "direction": {"type": "string"},
                            "target": {"type": "string"},
                            "confidence": {"type": "number"}
                        }
                    }
                },
                "limits": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "attribute_value": {"type": "string"},
                            "value": {"type": "number"},
                            "confidence": {"type": "number"}
                        }
                    }
                }
            }
        }
        
        try:
            # Generate structured response
            response = await self.llm_client.generate_structured(prompt, schema)
            
            if response:
                return {
                    AttributeType.FILTER: response.get("filters", []),
                    AttributeType.AGGREGATION: response.get("aggregations", []),
                    AttributeType.GROUPING: response.get("groupings", []),
                    AttributeType.SORTING: response.get("sortings", []),
                    AttributeType.LIMIT: response.get("limits", [])
                }
        except Exception as e:
            print(f"Error in LLM attribute extraction: {e}")
        
        # Return empty results if LLM fails
        return {
            AttributeType.FILTER: [],
            AttributeType.AGGREGATION: [],
            AttributeType.GROUPING: [],
            AttributeType.SORTING: [],
            AttributeType.LIMIT: []
        }