"""
Direct table match strategy for entity resolution.

This module implements a strategy for resolving entities by
directly matching them with database table names.
"""

from typing import Dict, Any
import logging

from src.text2sql.reasoning.agents.entity.base import EntityResolutionStrategy
from src.text2sql.reasoning.registry import StrategyRegistry


@StrategyRegistry.register("direct_table_match")
class DirectTableMatchStrategy(EntityResolutionStrategy):
    """Strategy for direct matching with database tables."""
    
    @property
    def description(self) -> str:
        """Get strategy description."""
        return "Matches entity names directly with database table names"
    
    async def resolve(self, entity: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve entity by direct table matching.
        
        Args:
            entity: Entity to resolve
            context: Resolution context
            
        Returns:
            Resolution result
        """
        tenant_id = context.get("tenant_id", "default")
        entity_context = self.graph_context.get_entity_resolution_context(tenant_id, entity)
        
        # If direct table match found
        if entity_context["type"] == "table" and entity_context["resolution"]:
            table_info = entity_context["resolution"][0]
            table_name = table_info.get("name")
            
            return {
                "entity": entity,
                "resolved_to": table_name,
                "resolution_type": "table",
                "confidence": 0.9,  # High confidence for direct matches
                "strategy": self.strategy_name,
                "metadata": {
                    "table_description": table_info.get("description", ""),
                    "dataset_id": table_info.get("dataset_id", "")
                }
            }
        
        return {
            "entity": entity,
            "resolved_to": None,
            "resolution_type": None,
            "confidence": 0.0,
            "strategy": self.strategy_name,
            "metadata": {}
        }