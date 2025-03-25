"""
Capitalization-based entity extraction.

This module extracts potential entity mentions based on capitalization
patterns in the query text.
"""

from typing import List
import logging

from src.text2sql.reasoning.agents.entity.base import EntityExtractor


class CapitalizationExtractor:
    """Extracts entities based on capitalization patterns."""
    
    @property
    def name(self) -> str:
        """Get extractor name."""
        return "capitalization"
    
    def extract(self, query: str) -> List[str]:
        """
        Extract terms that begin with capital letters.
        
        Args:
            query: Natural language query
            
        Returns:
            List of extracted terms
        """
        words = query.split()
        candidates = []
        
        # Extract single capitalized words
        for word in words:
            if word and word[0].isupper() and len(word) > 3:
                clean_word = word.strip(",.;:()\"'")
                if clean_word and len(clean_word) > 3:
                    candidates.append(clean_word)
        
        # Extract adjacent capitalized words
        for i in range(len(words) - 1):
            if words[i] and words[i+1] and words[i][0].isupper() and words[i+1][0].isupper():
                clean_term = f"{words[i]} {words[i+1]}".strip(",.;:()\"'")
                candidates.append(clean_term)
        
        return candidates