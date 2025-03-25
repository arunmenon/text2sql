"""
Keyword-based entity extraction.

This module extracts potential entity mentions based on SQL-like
keywords in the query text.
"""

from typing import List
import logging

from src.text2sql.reasoning.agents.entity.base import EntityExtractor


class KeywordBasedExtractor:
    """Extracts entities based on SQL-like keyword patterns."""
    
    @property
    def name(self) -> str:
        """Get extractor name."""
        return "sql_keywords"
    
    def extract(self, query: str) -> List[str]:
        """
        Extract terms following SQL-like keywords.
        
        Args:
            query: Natural language query
            
        Returns:
            List of extracted terms
        """
        query_lower = query.lower()
        words = query_lower.split()
        candidates = []
        
        # SQL-like keywords that might precede entities
        sql_indicators = ["select", "from", "where", "join", "group", "order", 
                          "show", "find", "get", "list", "display", "about"]
        
        for i, word in enumerate(words):
            if i < len(words) - 1 and word in sql_indicators:
                next_word = words[i + 1].strip(",.;:()\"'")
                if next_word and len(next_word) > 3:
                    # Get the original case version
                    original_case = query.split()[i + 1].strip(",.;:()\"'")
                    candidates.append(original_case)
        
        return candidates