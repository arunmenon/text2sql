"""
Glossary term match strategy for entity resolution.

This module implements a strategy for resolving entities by
matching them with business glossary terms.
"""

from typing import Dict, Any
import logging

from src.text2sql.reasoning.agents.entity.base import EntityResolutionStrategy
from src.text2sql.reasoning.registry import StrategyRegistry


@StrategyRegistry.register("glossary_term_match")
class GlossaryTermMatchStrategy(EntityResolutionStrategy):
    """Strategy for matching with business glossary terms."""
    
    @property
    def description(self) -> str:
        """Get strategy description."""
        return "Maps entity names to tables via business glossary terms"
    
    async def resolve(self, entity: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve entity via business glossary terms.
        
        Args:
            entity: Entity to resolve
            context: Resolution context
            
        Returns:
            Resolution result
        """
        tenant_id = context.get("tenant_id", "default")
        entity_context = self.graph_context.get_entity_resolution_context(tenant_id, entity)
        
        # If glossary term match found
        if entity_context["type"] == "business_term" and entity_context["resolution"]:
            term_info = entity_context["resolution"]
            
            if "mapped_tables" in term_info and term_info["mapped_tables"]:
                table_name = term_info["mapped_tables"][0]
                
                return {
                    "entity": entity,
                    "resolved_to": table_name,
                    "resolution_type": "business_term",
                    "confidence": 0.85,  # Good confidence for glossary matches
                    "strategy": self.strategy_name,
                    "metadata": {
                        "term_name": term_info.get("name", entity),
                        "term_definition": term_info.get("definition", ""),
                        "term_domain": term_info.get("domain", "")
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