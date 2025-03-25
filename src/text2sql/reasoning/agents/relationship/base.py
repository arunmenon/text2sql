"""
Base classes for relationship discovery agent.

This module defines base classes and interfaces for join path
discovery strategies.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Protocol

import logging


class JoinPathStrategy(ABC):
    """Base class for join path resolution strategies."""
    
    def __init__(self, graph_context_provider):
        """
        Initialize join path strategy.
        
        Args:
            graph_context_provider: Provider for semantic graph context
        """
        self.graph_context = graph_context_provider
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @property
    def strategy_name(self) -> str:
        """Get the name of this resolution strategy."""
        return self.__class__.__name__
    
    @property
    @abstractmethod
    def description(self) -> str:
        """Get description of this resolution strategy."""
        pass
    
    @abstractmethod
    async def resolve(self, source_table: str, target_table: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve join path between tables.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            context: Resolution context
            
        Returns:
            Resolution result with keys:
            - source_table: Source table name
            - target_table: Target table name
            - join_path: Join path information or None if not found
            - confidence: Confidence in join path (0.0-1.0)
            - strategy: Strategy used for resolution
            - alternative_paths: Other possible join paths
        """
        pass


class RelationshipExtractor(Protocol):
    """Interface for relationship hint extraction methods."""
    
    @property
    def name(self) -> str:
        """Get the name of this extractor."""
        pass
    
    def extract(self, query: str, entities: List[str]) -> Dict[str, Any]:
        """
        Extract relationship hints from query.
        
        Args:
            query: Natural language query
            entities: List of entity names
            
        Returns:
            Dictionary with relationship hints
        """
        pass