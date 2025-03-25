"""
Semantic concept match strategy for entity resolution.

This module implements a strategy for resolving entities by
matching them with semantic concepts in the knowledge graph.
"""

from typing import Dict, Any
import logging

from src.text2sql.reasoning.agents.entity.base import EntityResolutionStrategy
from src.text2sql.reasoning.registry import StrategyRegistry


@StrategyRegistry.register("semantic_concept_match")
class SemanticConceptMatchStrategy(EntityResolutionStrategy):
    """Strategy for matching with semantic concepts in the graph."""
    
    @property
    def description(self) -> str:
        """Get strategy description."""
        return "Maps entity names to tables via semantic concepts in the knowledge graph"
    
    async def resolve(self, entity: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve entity via semantic concepts.
        
        Args:
            entity: Entity to resolve
            context: Resolution context
            
        Returns:
            Resolution result
        """
        tenant_id = context.get("tenant_id", "default")
        intent = context.get("intent", {}).get("intent_type", "selection")
        entity_context = self.graph_context.get_entity_resolution_context(tenant_id, entity)
        
        # If semantic concept match found
        if entity_context["type"] == "semantic_concept" and entity_context["resolution"]:
            concept_info = entity_context["resolution"]
            
            if "implementation" in concept_info and "tables_involved" in concept_info["implementation"]:
                tables = concept_info["implementation"]["tables_involved"]
                
                if tables:
                    # For aggregation intents, prioritize tables with numeric columns
                    if intent == "aggregation":
                        # In a real implementation, we would check for numeric columns
                        # For now, just use the first table
                        table_name = tables[0]
                    else:
                        table_name = tables[0]
                    
                    concept_type = self._determine_concept_type(concept_info)
                    
                    return {
                        "entity": entity,
                        "resolved_to": table_name,
                        "resolution_type": "semantic_concept",
                        "confidence": 0.8,  # Good confidence for concept matches
                        "strategy": self.strategy_name,
                        "metadata": {
                            "concept_name": concept_info.get("name", entity),
                            "concept_type": concept_type,
                            "concept_description": concept_info.get("description", ""),
                            "all_tables": tables
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
    
    def _determine_concept_type(self, concept_info: Dict[str, Any]) -> str:
        """
        Determine the type of semantic concept.
        
        Args:
            concept_info: Concept information
            
        Returns:
            Concept type as string
        """
        labels = concept_info.get("labels", [])
        
        if isinstance(labels, list):
            if "CompositeConcept" in labels:
                return "composite"
            elif "BusinessProcess" in labels:
                return "business_process"
            elif "RelationshipConcept" in labels:
                return "relationship"
            elif "HierarchicalConcept" in labels:
                return "hierarchical"
        elif isinstance(labels, str):
            if "CompositeConcept" in labels:
                return "composite"
            elif "BusinessProcess" in labels:
                return "business_process"
            elif "RelationshipConcept" in labels:
                return "relationship"
            elif "HierarchicalConcept" in labels:
                return "hierarchical"
        
        return "general"