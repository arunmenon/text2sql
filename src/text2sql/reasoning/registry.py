"""
Strategy registry system for transparent text2sql reasoning.

This module provides a registry system for resolution strategies, allowing
for dynamic discovery and loading of strategies.
"""

from typing import Dict, Any, Type, Callable


class StrategyRegistry:
    """Registry for resolution strategies."""
    
    _strategies = {}
    
    @classmethod
    def register(cls, strategy_type: str) -> Callable:
        """
        Decorator to register a strategy class.
        
        Args:
            strategy_type: Unique identifier for the strategy
            
        Returns:
            Decorator function
        """
        def decorator(strategy_class):
            cls._strategies[strategy_type] = strategy_class
            return strategy_class
        return decorator
    
    @classmethod
    def get_strategy(cls, strategy_type: str) -> Type:
        """
        Get strategy class by type.
        
        Args:
            strategy_type: Strategy type identifier
            
        Returns:
            Strategy class
            
        Raises:
            ValueError: If strategy type is not registered
        """
        if strategy_type not in cls._strategies:
            raise ValueError(f"Unknown strategy type: {strategy_type}")
        return cls._strategies[strategy_type]
    
    @classmethod
    def list_strategies(cls) -> Dict[str, Type]:
        """
        Get all registered strategies.
        
        Returns:
            Dictionary of strategy types to strategy classes
        """
        return cls._strategies.copy()