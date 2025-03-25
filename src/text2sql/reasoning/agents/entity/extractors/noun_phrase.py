"""
Noun phrase-based entity extraction.

This module extracts potential entity mentions based on common
noun phrase patterns in the query text.
"""

from typing import List
import logging

from src.text2sql.reasoning.agents.entity.base import EntityExtractor


class NounPhraseExtractor:
    """Extracts entities based on noun phrase patterns."""
    
    @property
    def name(self) -> str:
        """Get extractor name."""
        return "noun_phrases"
    
    def extract(self, query: str) -> List[str]:
        """
        Extract noun phrases as potential entities.
        
        Args:
            query: Natural language query
            
        Returns:
            List of extracted terms
        """
        # This is a simplified version - in a real implementation,
        # we would use a proper NLP library for part-of-speech tagging
        
        words = query.split()
        candidates = []
        
        # Common adjectives that might precede entity nouns
        adjectives = ["active", "new", "all", "recent", "past", "top", "high", "low", 
                      "best", "worst", "available", "current", "previous", "next"]
        
        # Common nouns that might be database entities
        entity_nouns = ["customer", "order", "product", "sale", "transaction", 
                       "user", "account", "item", "invoice", "payment", "location", 
                       "store", "facility", "asset", "report", "proposal"]
        
        # Check for adjective + noun combinations
        for i, word in enumerate(words):
            if i < len(words) - 1:
                if word.lower() in adjectives and words[i+1].lower() in entity_nouns:
                    candidates.append(f"{word} {words[i+1]}")
                elif word.lower() in entity_nouns:
                    candidates.append(word)
        
        return candidates