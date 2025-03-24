"""
Prompt Loader Utility

Provides centralized prompt loading functionality for the semantic graph builder.
"""
import os
import logging
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)

class PromptLoader:
    """
    Loads and caches prompt templates from files.
    
    This utility handles loading prompts from external files,
    applying variable substitutions, and caching for performance.
    """
    
    def __init__(self, prompts_dir: str = None):
        """
        Initialize the prompt loader.
        
        Args:
            prompts_dir: Directory containing prompt templates.
                         Defaults to the semantic-graph-builder prompts directory.
        """
        if prompts_dir is None:
            # Default to semantic-graph-builder prompts directory
            package_dir = Path(__file__).parent.parent
            self.prompts_dir = os.path.join(package_dir, 'prompts')
        else:
            self.prompts_dir = prompts_dir
            
        self._prompt_cache = {}
        
    def load_prompt(self, prompt_name: str) -> str:
        """
        Load a prompt template by name.
        
        Args:
            prompt_name: Name of the prompt template file (without extension)
            
        Returns:
            Prompt template string
            
        Raises:
            FileNotFoundError: If prompt file doesn't exist
        """
        # Check cache first
        if prompt_name in self._prompt_cache:
            return self._prompt_cache[prompt_name]
            
        # Construct file path
        file_path = os.path.join(self.prompts_dir, f"{prompt_name}.txt")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                prompt = f.read()
                
            # Cache the result
            self._prompt_cache[prompt_name] = prompt
            return prompt
        except FileNotFoundError:
            logger.error(f"Prompt file not found: {file_path}")
            raise
            
    def format_prompt(self, prompt_name: str, **kwargs) -> str:
        """
        Load a prompt template and format it with variables.
        
        Args:
            prompt_name: Name of the prompt template
            **kwargs: Variables to substitute in the template
            
        Returns:
            Formatted prompt string
        """
        prompt_template = self.load_prompt(prompt_name)
        
        try:
            # Format the prompt with provided variables
            return prompt_template.format(**kwargs)
        except KeyError as e:
            logger.error(f"Missing required variable in prompt: {e}")
            # Return template with missing variables for debugging
            return prompt_template