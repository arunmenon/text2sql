"""
Direct foreign key strategy for join path discovery.

This module implements a strategy for discovering join paths through
direct foreign key relationships.
"""

from typing import Dict, Any
import logging

from src.text2sql.reasoning.agents.relationship.base import JoinPathStrategy
from src.text2sql.reasoning.registry import StrategyRegistry


@StrategyRegistry.register("direct_foreign_key")
class DirectForeignKeyStrategy(JoinPathStrategy):
    """Strategy for finding direct foreign key relationships."""
    
    @property
    def description(self) -> str:
        """Get strategy description."""
        return "Discovers direct foreign key relationships between tables"
    
    async def resolve(self, source_table: str, target_table: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Find direct foreign key relationships.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            context: Resolution context
            
        Returns:
            Resolution result with join paths
        """
        tenant_id = context.get("tenant_id", "default")
        
        # Get direct foreign key relationships from the graph
        join_paths = self.graph_context.get_join_paths(
            tenant_id=tenant_id, 
            source_table=source_table,
            target_table=target_table,
            min_confidence=0.7,
            strategy="direct_fk"
        )
        
        if join_paths:
            # Select the most direct path (fewest joins)
            join_paths.sort(key=lambda p: len(p.get("path", [])))
            best_path = join_paths[0]
            
            return {
                "source_table": source_table,
                "target_table": target_table,
                "join_path": best_path,
                "confidence": 0.9,  # High confidence for direct FK relationships
                "strategy": self.strategy_name,
                "alternative_paths": join_paths[1:3] if len(join_paths) > 1 else []
            }
        
        return {
            "source_table": source_table,
            "target_table": target_table,
            "join_path": None,
            "confidence": 0.0,
            "strategy": self.strategy_name,
            "alternative_paths": []
        }