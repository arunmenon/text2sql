"""
Keyword-based relationship hint extraction.

This module extracts potential relationship hints based on common
relationship keywords in natural language queries.
"""

from typing import Dict, List, Any
import re
import logging

from src.text2sql.reasoning.agents.relationship.base import RelationshipExtractor


class KeywordBasedRelationshipExtractor:
    """Extracts relationship hints based on keyword patterns."""
    
    @property
    def name(self) -> str:
        """Get extractor name."""
        return "keyword_based"
    
    def extract(self, query: str, entities: List[str]) -> Dict[str, Any]:
        """
        Extract relationship hints from query.
        
        Args:
            query: Natural language query
            entities: List of entity names
            
        Returns:
            Dictionary with relationship hints
        """
        # Look for common relationship patterns
        patterns = {
            "joins": ["join", "with", "and", "between", "related to", "linked to"],
            "ownership": ["has", "have", "owns", "own", "belonging to", "possessed by"],
            "membership": ["in", "contains", "member of", "part of", "within"]
        }
        
        relationship_types = {}
        confidence = 0.5  # Default confidence
        query_lower = query.lower()
        
        # Check for each entity pair
        for i, entity1 in enumerate(entities):
            for entity2 in entities[i+1:]:
                rel_type = None
                rel_confidence = 0.0
                
                # Check proximity of entities in query
                if entity1.lower() in query_lower and entity2.lower() in query_lower:
                    words_between = self._count_words_between(query_lower, entity1.lower(), entity2.lower())
                    proximity_score = max(0, 1.0 - (words_between / 10))  # 0 words = 1.0, 10+ words = 0.0
                    
                    # Check for relationship patterns
                    for rel_pattern, keywords in patterns.items():
                        for keyword in keywords:
                            if keyword in query_lower:
                                # Check if keyword is between entities
                                if self._is_keyword_between(query_lower, entity1.lower(), entity2.lower(), keyword):
                                    rel_type = rel_pattern
                                    # Higher confidence for explicit relationship keywords between entities
                                    rel_confidence = 0.7 + (proximity_score * 0.2)  # 0.7-0.9 range
                                    break
                    
                    # If no specific pattern matched but entities are close, infer general relationship
                    if not rel_type and proximity_score > 0.5:
                        rel_type = "general"
                        rel_confidence = 0.6
                
                if rel_type:
                    relationship_types[f"{entity1}_to_{entity2}"] = {
                        "type": rel_type,
                        "confidence": rel_confidence,
                        "entities": [entity1, entity2]
                    }
        
        # Calculate overall confidence
        overall_confidence = max([r["confidence"] for r in relationship_types.values()]) if relationship_types else 0.5
        
        return {
            "relationship_types": relationship_types,
            "confidence": overall_confidence
        }
    
    def _count_words_between(self, query: str, entity1: str, entity2: str) -> int:
        """Count words between two entities in the query."""
        idx1 = query.find(entity1)
        idx2 = query.find(entity2)
        
        if idx1 == -1 or idx2 == -1:
            return 100  # Large number if not found
        
        # Get text between entities
        start_idx = idx1 + len(entity1) if idx1 < idx2 else idx2 + len(entity2)
        end_idx = idx2 if idx1 < idx2 else idx1
        
        if start_idx >= end_idx:
            return 0  # Adjacent entities
        
        between_text = query[start_idx:end_idx].strip()
        return len(between_text.split())
    
    def _is_keyword_between(self, query: str, entity1: str, entity2: str, keyword: str) -> bool:
        """Check if a keyword appears between two entities."""
        idx1 = query.find(entity1)
        idx2 = query.find(entity2)
        keyword_idx = query.find(keyword)
        
        if idx1 == -1 or idx2 == -1 or keyword_idx == -1:
            return False
        
        # Ensure keyword is between entities
        if idx1 < idx2:
            return idx1 + len(entity1) <= keyword_idx < idx2
        else:
            return idx2 + len(entity2) <= keyword_idx < idx1