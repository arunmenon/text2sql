"""
Knowledge boundary handling for transparent text2sql.

This module provides a systematic approach for handling knowledge boundaries
and making them explicit in the reasoning process.
"""

from enum import Enum
from typing import Dict, Any, List, Optional


class BoundaryType(Enum):
    """Types of knowledge boundaries that can occur in reasoning."""
    UNKNOWN_ENTITY = "unknown_entity"
    UNMAPPABLE_CONCEPT = "unmappable_concept"
    AMBIGUOUS_INTENT = "ambiguous_intent"
    MISSING_RELATIONSHIP = "missing_relationship"
    UNCERTAIN_ATTRIBUTE = "uncertain_attribute"
    COMPLEX_IMPLEMENTATION = "complex_implementation"


class KnowledgeBoundary:
    """Represents a knowledge boundary in reasoning."""
    
    def __init__(
        self,
        boundary_type: BoundaryType,
        component: str,
        confidence: float,
        explanation: str,
        suggestions: Optional[List[str]] = None,
        alternatives: Optional[List[Dict[str, Any]]] = None
    ):
        """
        Initialize knowledge boundary.
        
        Args:
            boundary_type: Type of knowledge boundary
            component: Component that hit the boundary
            confidence: Confidence in current handling (0.0-1.0)
            explanation: Explanation of the boundary
            suggestions: Optional suggestions for handling
            alternatives: Optional alternative interpretations
        """
        self.boundary_type = boundary_type
        self.component = component
        self.confidence = confidence
        self.explanation = explanation
        self.suggestions = suggestions or []
        self.alternatives = alternatives or []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "boundary_type": self.boundary_type.value,
            "component": self.component,
            "confidence": self.confidence,
            "explanation": self.explanation,
            "suggestions": self.suggestions,
            "alternatives": self.alternatives,
            "requires_clarification": self.confidence < 0.4
        }


class BoundaryRegistry:
    """Registry for knowledge boundaries in processing flow."""
    
    def __init__(self):
        """Initialize boundary registry."""
        self.boundaries = []
    
    def add_boundary(self, boundary: KnowledgeBoundary) -> None:
        """
        Add knowledge boundary to registry.
        
        Args:
            boundary: Knowledge boundary to add
        """
        self.boundaries.append(boundary)
    
    def get_boundaries_by_type(self, boundary_type: BoundaryType) -> List[KnowledgeBoundary]:
        """
        Get boundaries of specific type.
        
        Args:
            boundary_type: Type of boundaries to retrieve
            
        Returns:
            List of matching knowledge boundaries
        """
        return [b for b in self.boundaries if b.boundary_type == boundary_type]
    
    def get_all_boundaries(self) -> List[KnowledgeBoundary]:
        """
        Get all registered boundaries.
        
        Returns:
            List of all knowledge boundaries
        """
        return self.boundaries
    
    def requires_clarification(self) -> bool:
        """
        Check if any boundary requires user clarification.
        
        Returns:
            True if clarification is required, False otherwise
        """
        return any(b.confidence < 0.4 for b in self.boundaries)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert registry to dictionary representation.
        
        Returns:
            Dictionary representation of boundary registry
        """
        return {
            "boundaries": [b.to_dict() for b in self.boundaries],
            "requires_clarification": self.requires_clarification(),
            "boundary_count": len(self.boundaries),
            "boundary_types": list(set(b.boundary_type.value for b in self.boundaries))
        }
"""