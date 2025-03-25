"""
Common column strategy for join path discovery.

This module implements a strategy for discovering join paths through
common column names and patterns.
"""

from typing import Dict, Any
import logging

from src.text2sql.reasoning.agents.relationship.base import JoinPathStrategy
from src.text2sql.reasoning.registry import StrategyRegistry


@StrategyRegistry.register("common_column")
class CommonColumnStrategy(JoinPathStrategy):
    """Strategy for finding relationships through common column names."""
    
    @property
    def description(self) -> str:
        """Get strategy description."""
        return "Discovers relationships through common column names and patterns"
    
    async def resolve(self, source_table: str, target_table: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Find relationships through common column names.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            context: Resolution context
            
        Returns:
            Resolution result with join paths
        """
        tenant_id = context.get("tenant_id", "default")
        
        # Get paths based on common column naming patterns
        join_paths = self.graph_context.get_join_paths(
            tenant_id=tenant_id, 
            source_table=source_table,
            target_table=target_table,
            min_confidence=0.5,
            strategy="common_columns"
        )
        
        if join_paths:
            # Sort by confidence
            join_paths.sort(key=lambda p: p.get("confidence", 0), reverse=True)
            best_path = join_paths[0]
            
            return {
                "source_table": source_table,
                "target_table": target_table,
                "join_path": best_path,
                "confidence": min(0.8, best_path.get("confidence", 0.6)),  # Cap at 0.8
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