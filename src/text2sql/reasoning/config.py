"""
Configuration management for transparent text2sql reasoning.

This module provides utilities for loading and managing configuration
for reasoning components.
"""

import os
import json
from typing import Dict, Any, Optional


class ConfigLoader:
    """Utility for loading configuration files."""
    
    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        """
        Load configuration from JSON file.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Configuration dictionary
        """
        if not os.path.exists(config_path):
            return {}
            
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading config from {config_path}: {e}")
            return {}
    
    @staticmethod
    def get_default_config_path(component: str) -> str:
        """
        Get default configuration path for a component.
        
        Args:
            component: Component name (e.g., "entity_agent")
            
        Returns:
            Default configuration path
        """
        # Base directory is two levels up from this file
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        return os.path.join(base_dir, "config", f"{component}.json")