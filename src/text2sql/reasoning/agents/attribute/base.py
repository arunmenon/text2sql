"""
Base classes for attribute extraction and resolution.

This module defines base classes and interfaces for attribute resolution
strategies and extractors.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Protocol, Set, Tuple, Optional


class AttributeType:
    """Enumeration of attribute types."""
    FILTER = "filter"
    AGGREGATION = "aggregation"
    GROUPING = "grouping"
    SORTING = "sorting"
    LIMIT = "limit"


class AttributeResolutionStrategy(ABC):
    """Base class for attribute resolution strategies."""
    
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
    async def resolve(self, attribute_type: str, attribute_value: str, 
                    context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve attribute using strategy.
        
        Args:
            attribute_type: Type of attribute (filter, aggregation, etc.)
            attribute_value: Attribute value to resolve
            context: Resolution context with entities, relationships, etc.
            
        Returns:
            Resolution result with keys:
            - attribute_type: Type of attribute (filter, etc.)
            - attribute_value: Original attribute value
            - resolved_to: Resolved SQL component
            - confidence: Confidence in resolution (0.0-1.0)
            - strategy: Strategy used for resolution
            - metadata: Additional strategy-specific metadata
        """
        pass


class AttributeExtractor(Protocol):
    """Interface for attribute extraction methods."""
    
    @property
    def name(self) -> str:
        """Get the name of this extractor."""
        pass
    
    def extract(self, query: str, entity_context: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract attributes from query.
        
        Args:
            query: Natural language query
            entity_context: Context with entity information
            
        Returns:
            Dictionary with attribute types as keys and lists of attributes as values
        """
        pass