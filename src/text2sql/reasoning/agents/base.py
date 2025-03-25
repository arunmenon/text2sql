"""
Base agent interface for transparent text2sql reasoning.

This module defines the base agent interface and common functionality
for all specialized reasoning agents.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional

from src.text2sql.reasoning.models import ReasoningStream
from src.text2sql.utils.prompt_loader import PromptLoader


class Agent(ABC):
    """Base class for all reasoning agents."""
    
    def __init__(self, prompt_loader: PromptLoader):
        """
        Initialize agent with prompt loader.
        
        Args:
            prompt_loader: Loader for prompt templates
        """
        self.prompt_loader = prompt_loader
        self.agent_type = self._get_agent_type()
    
    @abstractmethod
    def _get_agent_type(self) -> str:
        """
        Return the agent type for prompt naming.
        
        Returns:
            String identifying agent type (e.g., "intent", "entity")
        """
        pass
    
    @abstractmethod
    async def process(self, context: Dict[str, Any], reasoning_stream: ReasoningStream) -> Dict[str, Any]:
        """
        Process input context and update reasoning stream.
        
        Args:
            context: Input context with query and previous agent results
            reasoning_stream: Reasoning stream to update
            
        Returns:
            Processing results
        """
        pass
    
    def _get_prompt_name(self, operation: str) -> str:
        """
        Get prompt name for specific operation.
        
        Args:
            operation: Name of operation
            
        Returns:
            Prompt template name
        """
        return f"{self.agent_type}_{operation}"


class ResolutionStrategy(ABC):
    """Base class for resolution strategies."""
    
    @abstractmethod
    async def resolve(self, component: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve component using strategy.
        
        Args:
            component: Component to resolve (entity, attribute, etc.)
            context: Resolution context
            
        Returns:
            Resolution result
        """
        pass
"""