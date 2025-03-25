"""
Base classes for entity recognition agent.

This module defines base classes and interfaces for entity recognition
strategies and extractors.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Protocol


class EntityResolutionStrategy(ABC):
    """Base class for entity resolution strategies."""
    
    def __init__(self, graph_context_provider):
        """
        Initialize resolution strategy.
        
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
    async def resolve(self, entity: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve entity using strategy.
        
        Args:
            entity: Entity to resolve
            context: Resolution context
            
        Returns:
            Resolution result with keys:
            - entity: Original entity name
            - resolved_to: Resolved table name (or None if not resolved)
            - resolution_type: Type of resolution (table, business_term, etc.)
            - confidence: Confidence in resolution (0.0-1.0)
            - strategy: Strategy used for resolution
            - metadata: Additional strategy-specific metadata
        """
        pass


class EntityExtractor(Protocol):
    """Interface for entity extraction methods."""
    
    @property
    def name(self) -> str:
        """Get the name of this extractor."""
        pass
    
    def extract(self, query: str) -> List[str]:
        """
        Extract entities from query.
        
        Args:
            query: Natural language query
            
        Returns:
            List of extracted entities
        """
        pass


import logging