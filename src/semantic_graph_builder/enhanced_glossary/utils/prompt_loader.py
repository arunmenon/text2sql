"""
Prompt loader utility for loading prompt templates from files.

This module now imports from the centralized prompt loader in the semantic_graph_builder.
"""

import os
import logging
from src.semantic_graph_builder.utils.prompt_loader import PromptLoader as CentralPromptLoader

logger = logging.getLogger(__name__)

# Re-export the centralized PromptLoader for backward compatibility
PromptLoader = CentralPromptLoader