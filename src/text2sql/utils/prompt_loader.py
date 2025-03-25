"""Prompt loader utility for text2sql.

Provides functionality for loading and formatting prompt templates.
"""

import os
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class PromptLoader:
    """Utility for loading and formatting prompt templates."""
    
    def __init__(self, base_path: Optional[str] = None):
        """
        Initialize prompt loader.
        
        Args:
            base_path: Base path for prompt files. If None, uses default path.
        """
        if base_path:
            self.base_path = base_path
        else:
            # Default to src/text2sql/prompts directory
            self.base_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "prompts"
            )
        
        logger.debug(f"Initialized PromptLoader with base path: {self.base_path}")
    
    def load_prompt(self, prompt_name: str) -> str:
        """
        Load prompt template from file.
        
        Args:
            prompt_name: Name of the prompt template
            
        Returns:
            Prompt template string
        """
        # First, try the direct path as provided
        prompt_path = os.path.join(self.base_path, f"{prompt_name}.txt")
        
        try:
            with open(prompt_path, "r") as f:
                return f.read()
        except FileNotFoundError:
            # If not found, check alternate directory structure
            # Look in a related directory structure (e.g., reasoning/prompts)
            alternate_paths = [
                os.path.join(self.base_path, "..", "reasoning", "prompts", f"{prompt_name}.txt"),
                os.path.join(self.base_path, "..", "..", "prompts", f"{prompt_name}.txt"),
                os.path.join(os.path.dirname(os.path.dirname(self.base_path)), "prompts", f"{prompt_name}.txt")
            ]
            
            for alt_path in alternate_paths:
                try:
                    with open(alt_path, "r") as f:
                        logger.info(f"Loaded prompt from alternate path: {alt_path}")
                        return f.read()
                except FileNotFoundError:
                    continue
            
            logger.warning(f"Prompt template not found: {prompt_name}")
            return f"ERROR: Prompt '{prompt_name}' not found."
    
    def format_prompt(self, prompt_name: str, **kwargs) -> str:
        """
        Load and format prompt template with variables.
        
        Args:
            prompt_name: Name of the prompt template
            **kwargs: Variables to substitute in the template
            
        Returns:
            Formatted prompt string
        """
        prompt_template = self.load_prompt(prompt_name)
        
        if not prompt_template:
            logger.warning(f"Empty prompt template: {prompt_name}")
            return ""
        
        # Format prompt with provided variables
        try:
            formatted_prompt = prompt_template.format(**kwargs)
            return formatted_prompt
        except KeyError as e:
            logger.error(f"Missing variable in prompt formatting: {e}")
            return prompt_template