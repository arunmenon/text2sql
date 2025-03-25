"""
NLP-based attribute extractor.

This module contains an extractor that uses natural language processing
techniques to identify attributes in the query.
"""

import re
from typing import Dict, List, Any, Optional
try:
    import spacy
    from spacy.matcher import DependencyMatcher
    SPACY_AVAILABLE = True
except ImportError:
    SPACY_AVAILABLE = False

from src.text2sql.reasoning.agents.attribute.base import AttributeType


class NLPBasedAttributeExtractor:
    """Extract attributes using NLP techniques."""
    
    def __init__(self):
        """Initialize the NLP-based extractor."""
        self.nlp = None
        if SPACY_AVAILABLE:
            try:
                self.nlp = spacy.load("en_core_web_sm")
            except OSError:
                # If the model isn't available, set to None
                pass
    
    @property
    def name(self) -> str:
        """Get extractor name."""
        return "NLP-Based Attribute Extractor"
    
    def extract(self, query: str, entity_context: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract attributes using NLP techniques.
        
        Args:
            query: Natural language query
            entity_context: Context with entity information
            
        Returns:
            Dictionary of extracted attributes by type
        """
        if not SPACY_AVAILABLE or not self.nlp:
            # Return empty results if spaCy is not available
            return {
                AttributeType.FILTER: [],
                AttributeType.AGGREGATION: [],
                AttributeType.GROUPING: [],
                AttributeType.SORTING: [],
                AttributeType.LIMIT: []
            }
        
        # Process the query with spaCy
        doc = self.nlp(query)
        
        # Extract all attribute types using NLP
        filters = self._extract_filters(doc, entity_context)
        aggregations = self._extract_aggregations(doc)
        groupings = self._extract_groupings(doc, entity_context)
        sortings = self._extract_sortings(doc)
        limits = self._extract_limits(doc)
        
        return {
            AttributeType.FILTER: filters,
            AttributeType.AGGREGATION: aggregations,
            AttributeType.GROUPING: groupings,
            AttributeType.SORTING: sortings,
            AttributeType.LIMIT: limits
        }
    
    def _extract_filters(self, doc, entity_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract filter attributes using NLP dependency parsing."""
        filters = []
        
        # Look for comparison-based dependencies
        comparison_tokens = [token for token in doc if token.dep_ in ["acomp", "ccomp", "ncomp", "amod"]]
        
        for token in comparison_tokens:
            # Look for comparison words
            if token.lemma_ in ["greater", "more", "higher", "larger", "bigger", "less", 
                            "lower", "smaller", "fewer", "equal", "same", "like", "between"]:
                # Identify the numeric value being compared
                numeric_child = None
                for child in token.children:
                    if child.like_num or child.ent_type_ in ["CARDINAL", "MONEY", "PERCENT"]:
                        numeric_child = child
                        break
                
                if numeric_child:
                    # Determine the operator
                    operator = None
                    if token.lemma_ in ["greater", "more", "higher", "larger", "bigger"]:
                        operator = "greater_than"
                    elif token.lemma_ in ["less", "lower", "smaller", "fewer"]:
                        operator = "less_than"
                    elif token.lemma_ in ["equal", "same"]:
                        operator = "equal_to"
                    elif token.lemma_ == "between":
                        operator = "between"
                    
                    # Determine the column/entity being compared
                    head = token.head
                    entity_name = None
                    for entity in entity_context.get("entities", {}):
                        # Check if entity appears in the sentence
                        if entity.lower() in doc.text.lower():
                            entity_name = entity
                            break
                    
                    # Look for a noun phrase that might indicate the column
                    column_hint = None
                    for chunk in doc.noun_chunks:
                        # If this chunk contains or is close to the comparison
                        if chunk.root == head or chunk.start < token.i < chunk.end + 3:
                            column_hint = chunk.text
                            break
                    
                    if operator and (column_hint or entity_name):
                        filters.append({
                            "attribute_value": doc[max(0, token.i-3):min(len(doc), numeric_child.i+3)].text,
                            "operator": operator,
                            "value": {"value": numeric_child.text},
                            "column_hint": column_hint,
                            "entity_name": entity_name,
                            "confidence": 0.7
                        })
        
        # Look for date-related filters
        date_entities = [ent for ent in doc.ents if ent.label_ in ["DATE", "TIME"]]
        
        for date_ent in date_entities:
            # Look for temporal prepositions like "before", "after", "on"
            date_prep = None
            for token in doc:
                if token.dep_ == "prep" and token.head.i >= date_ent.start and token.head.i <= date_ent.end:
                    if token.lemma_ in ["before", "after", "on", "during", "in", "since"]:
                        date_prep = token.lemma_
                        break
            
            # If no direct preposition found, look for words before the date
            if not date_prep:
                context_before = doc[max(0, date_ent.start-2):date_ent.start].text.lower()
                if any(word in context_before for word in ["before", "prior", "earlier"]):
                    date_prep = "before"
                elif any(word in context_before for word in ["after", "since", "later"]):
                    date_prep = "after"
                elif any(word in context_before for word in ["on", "during", "in"]):
                    date_prep = "on"
            
            if date_prep:
                # Map preposition to operator
                operator = None
                if date_prep in ["before", "prior", "earlier"]:
                    operator = "before_date"
                elif date_prep in ["after", "since", "later"]:
                    operator = "after_date"
                elif date_prep in ["on", "during", "in"]:
                    operator = "on_date"
                
                # Determine the column/entity for this date filter
                entity_name = None
                for entity in entity_context.get("entities", {}):
                    # Check if entity appears in the context
                    if entity.lower() in doc[:date_ent.start].text.lower():
                        entity_name = entity
                        break
                
                if operator:
                    filters.append({
                        "attribute_value": doc[max(0, date_ent.start-2):date_ent.end].text,
                        "operator": operator,
                        "value": {"date": date_ent.text},
                        "entity_name": entity_name,
                        "confidence": 0.7
                    })
        
        return filters
    
    def _extract_aggregations(self, doc) -> List[Dict[str, Any]]:
        """Extract aggregation attributes using NLP techniques."""
        aggregations = []
        
        # Define aggregation function words
        aggregation_verbs = {
            "count": ["count", "tally", "enumerate"],
            "sum": ["sum", "total", "add"],
            "avg": ["average", "mean"],
            "max": ["maximize", "maximum", "highest"],
            "min": ["minimize", "minimum", "lowest"]
        }
        
        # Look for aggregation verbs
        for token in doc:
            agg_func = None
            for func, verbs in aggregation_verbs.items():
                if token.lemma_ in verbs:
                    agg_func = func
                    break
            
            if agg_func:
                # Find the target noun phrase
                target = None
                
                # First check direct object
                for child in token.children:
                    if child.dep_ in ["dobj", "pobj"]:
                        # Get the entire span including children
                        span_start = child.i
                        span_end = child.i + 1
                        
                        for descendant in child.subtree:
                            span_start = min(span_start, descendant.i)
                            span_end = max(span_end, descendant.i + 1)
                        
                        target = doc[span_start:span_end].text
                        break
                
                # If no direct object, look for prepositional phrases
                if not target:
                    for prep in token.children:
                        if prep.dep_ == "prep" and prep.lemma_ in ["of", "for", "in"]:
                            for pobj in prep.children:
                                if pobj.dep_ == "pobj":
                                    # Get the entire span including children
                                    span_start = pobj.i
                                    span_end = pobj.i + 1
                                    
                                    for descendant in pobj.subtree:
                                        span_start = min(span_start, descendant.i)
                                        span_end = max(span_end, descendant.i + 1)
                                    
                                    target = doc[span_start:span_end].text
                                    break
                
                # Also handle "how many" pattern
                if not target and agg_func == "count" and token.lemma_ == "how" and token.i + 1 < len(doc) and doc[token.i + 1].lemma_ == "many":
                    for i in range(token.i + 2, len(doc)):
                        if doc[i].pos_ in ["NOUN", "PROPN"]:
                            # Get the noun phrase
                            span_start = i
                            span_end = i + 1
                            
                            for j in range(i, len(doc)):
                                if doc[j].pos_ in ["NOUN", "PROPN", "ADJ"]:
                                    span_end = j + 1
                                else:
                                    break
                            
                            target = doc[span_start:span_end].text
                            break
                
                if target:
                    aggregations.append({
                        "attribute_value": f"{token.text} {target}",
                        "function": agg_func,
                        "target": target,
                        "confidence": 0.7
                    })
        
        return aggregations
    
    def _extract_groupings(self, doc, entity_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract grouping attributes using NLP techniques."""
        groupings = []
        
        # Look for grouping indicators
        group_terms = ["group", "grouped", "groups", "grouping"]
        by_preps = ["by", "per", "for each", "each"]
        
        for token in doc:
            # Check for "group by" pattern
            if token.lemma_ in group_terms:
                # Look for "by" preposition
                by_token = None
                for child in token.children:
                    if child.dep_ == "prep" and child.lemma_ in by_preps:
                        by_token = child
                        break
                
                if by_token:
                    # Find the target of "by"
                    target = None
                    for child in by_token.children:
                        if child.dep_ in ["pobj", "dobj"]:
                            # Get the entire span including children
                            span_start = child.i
                            span_end = child.i + 1
                            
                            for descendant in child.subtree:
                                span_start = min(span_start, descendant.i)
                                span_end = max(span_end, descendant.i + 1)
                            
                            target = doc[span_start:span_end].text
                            break
                    
                    if target:
                        # Check if target mentions an entity
                        entity_name = None
                        for entity in entity_context.get("entities", {}):
                            if entity.lower() in target.lower():
                                entity_name = entity
                                break
                        
                        groupings.append({
                            "attribute_value": f"{token.text} {by_token.text} {target}",
                            "target": target,
                            "entity_name": entity_name,
                            "confidence": 0.7
                        })
            
            # Check for "per" or "for each" patterns
            elif token.lemma_ in by_preps:
                # Find the target
                target = None
                for child in token.children:
                    if child.dep_ == "pobj":
                        # Get the entire span including children
                        span_start = child.i
                        span_end = child.i + 1
                        
                        for descendant in child.subtree:
                            span_start = min(span_start, descendant.i)
                            span_end = max(span_end, descendant.i + 1)
                        
                        target = doc[span_start:span_end].text
                        break
                
                if target:
                    # Check if target mentions an entity
                    entity_name = None
                    for entity in entity_context.get("entities", {}):
                        if entity.lower() in target.lower():
                            entity_name = entity
                            break
                    
                    groupings.append({
                        "attribute_value": f"{token.text} {target}",
                        "target": target,
                        "entity_name": entity_name,
                        "confidence": 0.7
                    })
        
        return groupings
    
    def _extract_sortings(self, doc) -> List[Dict[str, Any]]:
        """Extract sorting attributes using NLP techniques."""
        sortings = []
        
        # Look for sorting verbs and their objects
        sort_verbs = ["sort", "order", "arrange", "rank"]
        
        for token in doc:
            if token.lemma_ in sort_verbs:
                # Look for "by" preposition
                by_token = None
                for child in token.children:
                    if child.dep_ == "prep" and child.lemma_ == "by":
                        by_token = child
                        break
                
                if by_token:
                    # Find the target of "by"
                    target = None
                    for child in by_token.children:
                        if child.dep_ == "pobj":
                            # Get the entire span including children
                            span_start = child.i
                            span_end = child.i + 1
                            
                            for descendant in child.subtree:
                                span_start = min(span_start, descendant.i)
                                span_end = max(span_end, descendant.i + 1)
                            
                            target = doc[span_start:span_end].text
                            break
                    
                    if target:
                        # Determine direction (ascending or descending)
                        direction = "asc"  # Default to ascending
                        
                        # Look for direction indicators in the sentence
                        for i in range(len(doc)):
                            if doc[i].lemma_ in ["ascending", "asc", "increasing", "descending", "desc", "decreasing"]:
                                direction = "asc" if doc[i].lemma_ in ["ascending", "asc", "increasing"] else "desc"
                                break
                        
                        sortings.append({
                            "attribute_value": f"{token.text} {by_token.text} {target}",
                            "direction": direction,
                            "target": target,
                            "confidence": 0.7
                        })
        
        return sortings
    
    def _extract_limits(self, doc) -> List[Dict[str, Any]]:
        """Extract limit attributes using NLP techniques."""
        limits = []
        
        # Look for limit indicators with numbers
        limit_terms = ["top", "first", "limit", "only", "just", "show"]
        
        for token in doc:
            if token.lemma_ in limit_terms:
                # Look for numeric values
                for i in range(max(0, token.i - 2), min(len(doc), token.i + 3)):
                    if doc[i].like_num and doc[i].text.isdigit():
                        limits.append({
                            "attribute_value": doc[max(0, token.i - 1):min(len(doc), doc[i].i + 1)].text,
                            "value": int(doc[i].text),
                            "confidence": 0.7
                        })
                        break
        
        return limits