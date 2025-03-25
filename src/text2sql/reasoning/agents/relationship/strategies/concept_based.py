"""
Concept-based strategy for join path discovery.

This module implements a strategy for discovering join paths through
semantic concepts in the knowledge graph.
"""

from typing import Dict, Any
import logging

from src.text2sql.reasoning.agents.relationship.base import JoinPathStrategy
from src.text2sql.reasoning.registry import StrategyRegistry


@StrategyRegistry.register("concept_based_join")
class ConceptBasedJoinStrategy(JoinPathStrategy):
    """Strategy for finding relationships through semantic concepts."""
    
    @property
    def description(self) -> str:
        """Get strategy description."""
        return "Discovers relationships using semantic concepts in the knowledge graph"
    
    async def resolve(self, source_table: str, target_table: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Find relationships through semantic concepts.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            context: Resolution context
            
        Returns:
            Resolution result with join paths
        """
        tenant_id = context.get("tenant_id", "default")
        
        # Look for concepts that relate these tables
        join_paths = self.graph_context.get_join_paths(
            tenant_id=tenant_id, 
            source_table=source_table,
            target_table=target_table,
            min_confidence=0.4,
            strategy="semantic_concepts"
        )
        
        if join_paths:
            # Sort by confidence and path length
            join_paths.sort(key=lambda p: (p.get("confidence", 0), -len(p.get("path", []))), reverse=True)
            best_path = join_paths[0]
            
            return {
                "source_table": source_table,
                "target_table": target_table,
                "join_path": best_path,
                "confidence": min(0.85, best_path.get("confidence", 0.7)),  # Cap at 0.85
                "strategy": self.strategy_name,
                "alternative_paths": join_paths[1:3] if len(join_paths) > 1 else [],
                "concept": best_path.get("concept_info", {})
            }
        
        return {
            "source_table": source_table,
            "target_table": target_table,
            "join_path": None,
            "confidence": 0.0,
            "strategy": self.strategy_name,
            "alternative_paths": []
        }