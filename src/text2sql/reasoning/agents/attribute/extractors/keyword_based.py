"""
Keyword-based attribute extractor.

This module contains an extractor that identifies attributes based on
common keywords and patterns in the query.
"""

import re
from typing import Dict, List, Any

from src.text2sql.reasoning.agents.attribute.base import AttributeType


class KeywordBasedAttributeExtractor:
    """Extract attributes based on keywords and patterns."""
    
    @property
    def name(self) -> str:
        """Get extractor name."""
        return "Keyword-Based Attribute Extractor"
    
    def extract(self, query: str, entity_context: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract attributes using keyword patterns.
        
        Args:
            query: Natural language query
            entity_context: Context with entity information
            
        Returns:
            Dictionary of extracted attributes by type
        """
        normalized_query = query.lower()
        
        # Extract all attribute types
        filters = self._extract_filters(normalized_query, entity_context)
        aggregations = self._extract_aggregations(normalized_query)
        groupings = self._extract_groupings(normalized_query, entity_context)
        sortings = self._extract_sortings(normalized_query)
        limits = self._extract_limits(normalized_query)
        
        return {
            AttributeType.FILTER: filters,
            AttributeType.AGGREGATION: aggregations,
            AttributeType.GROUPING: groupings,
            AttributeType.SORTING: sortings,
            AttributeType.LIMIT: limits
        }
    
    def _extract_filters(self, query: str, entity_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract filter attributes from query."""
        filters = []
        
        # Pattern for value comparisons (greater than, less than, equal to, etc.)
        comparison_patterns = [
            (r"(greater|more|higher|larger|bigger) than (\d+)", "greater_than", r"\2"),
            (r"(less|lower|smaller|fewer) than (\d+)", "less_than", r"\2"),
            (r"equal to (\d+)", "equal_to", r"\1"),
            (r"not equal to (\d+)", "not_equal_to", r"\1"),
            (r"at least (\d+)", "greater_equal", r"\1"),
            (r"at most (\d+)", "less_equal", r"\1"),
            (r"between (\d+) and (\d+)", "between", r"\1 AND \2"),
        ]
        
        for pattern, operator, value_pattern in comparison_patterns:
            for match in re.finditer(pattern, query):
                # Extract the value from the match
                if "between" in pattern:
                    value = re.sub(pattern, value_pattern, match.group(0))
                    values = value.split(" AND ")
                    value_obj = {"start": values[0], "end": values[1]}
                else:
                    value = re.sub(pattern, value_pattern, match.group(0))
                    value_obj = {"value": value}
                
                # Determine which column this applies to
                # Look at context before the comparison to find column reference
                query_before = query[:match.start()].lower()
                
                # Context window size (number of words to look back)
                context_window = 5
                words_before = query_before.split()[-context_window:] if len(query_before.split()) > context_window else query_before.split()
                
                column_hint = " ".join(words_before)
                
                # Extract entity from column hint if possible
                entity_name = None
                for word in words_before:
                    if word in entity_context.get("entities", {}):
                        entity_name = word
                        break
                
                filters.append({
                    "attribute_value": match.group(0),
                    "operator": operator,
                    "value": value_obj,
                    "column_hint": column_hint,
                    "entity_name": entity_name,
                    "confidence": 0.8
                })
        
        # Pattern for text containment (includes, contains, etc.)
        containment_patterns = [
            (r"(contains|has|with|including) ['\"](.*?)['\"]", "contains", r"\2"),
            (r"(starts with|begins with) ['\"](.*?)['\"]", "starts_with", r"\2"),
            (r"(ends with) ['\"](.*?)['\"]", "ends_with", r"\2"),
        ]
        
        for pattern, operator, value_pattern in containment_patterns:
            for match in re.finditer(pattern, query):
                value = re.sub(pattern, value_pattern, match.group(0))
                
                # Determine which column this applies to
                query_before = query[:match.start()].lower()
                context_window = 5
                words_before = query_before.split()[-context_window:] if len(query_before.split()) > context_window else query_before.split()
                
                column_hint = " ".join(words_before)
                
                # Extract entity from column hint if possible
                entity_name = None
                for word in words_before:
                    if word in entity_context.get("entities", {}):
                        entity_name = word
                        break
                
                filters.append({
                    "attribute_value": match.group(0),
                    "operator": operator,
                    "value": {"text": value},
                    "column_hint": column_hint,
                    "entity_name": entity_name,
                    "confidence": 0.8
                })
        
        # Look for date-related filters
        date_patterns = [
            (r"(before|prior to|earlier than) ([\w\s,]+\d{4})", "before_date", r"\2"),
            (r"(after|since|later than) ([\w\s,]+\d{4})", "after_date", r"\2"),
            (r"(on|during|in) ([\w\s,]+\d{4})", "on_date", r"\2"),
        ]
        
        for pattern, operator, value_pattern in date_patterns:
            for match in re.finditer(pattern, query):
                date_text = re.sub(pattern, value_pattern, match.group(0))
                
                query_before = query[:match.start()].lower()
                context_window = 5
                words_before = query_before.split()[-context_window:] if len(query_before.split()) > context_window else query_before.split()
                
                column_hint = " ".join(words_before)
                
                # Extract entity from column hint if possible
                entity_name = None
                for word in words_before:
                    if word in entity_context.get("entities", {}):
                        entity_name = word
                        break
                
                filters.append({
                    "attribute_value": match.group(0),
                    "operator": operator,
                    "value": {"date": date_text},
                    "column_hint": column_hint,
                    "entity_name": entity_name,
                    "confidence": 0.7
                })
        
        return filters
    
    def _extract_aggregations(self, query: str) -> List[Dict[str, Any]]:
        """Extract aggregation attributes from query."""
        aggregations = []
        
        # Pattern for common aggregation functions
        aggregation_patterns = [
            (r"(count|number of|total number of|how many) ([\w\s]+)", "count", r"\2"),
            (r"(sum|total|sum of) ([\w\s]+)", "sum", r"\2"),
            (r"(average|avg|mean) ([\w\s]+)", "avg", r"\2"),
            (r"(maximum|max|highest) ([\w\s]+)", "max", r"\2"),
            (r"(minimum|min|lowest) ([\w\s]+)", "min", r"\2"),
        ]
        
        for pattern, func, target_pattern in aggregation_patterns:
            for match in re.finditer(pattern, query):
                target = re.sub(pattern, target_pattern, match.group(0))
                
                aggregations.append({
                    "attribute_value": match.group(0),
                    "function": func,
                    "target": target.strip(),
                    "confidence": 0.8
                })
        
        return aggregations
    
    def _extract_groupings(self, query: str, entity_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract grouping attributes from query."""
        groupings = []
        
        # Pattern for grouping operations
        grouping_patterns = [
            r"(group by|grouped by|for each) ([\w\s]+)",
            r"(per|by) ([\w\s]+?) (show|calculate|display|list)",
        ]
        
        for pattern in grouping_patterns:
            for match in re.finditer(pattern, query):
                # Extract the target from the second capture group
                if len(match.groups()) >= 2:
                    target = match.group(2).strip()
                    
                    # Check if target mentions an entity
                    entity_name = None
                    for entity in entity_context.get("entities", {}):
                        if entity.lower() in target.lower():
                            entity_name = entity
                            break
                    
                    groupings.append({
                        "attribute_value": match.group(0),
                        "target": target,
                        "entity_name": entity_name,
                        "confidence": 0.7
                    })
        
        return groupings
    
    def _extract_sortings(self, query: str) -> List[Dict[str, Any]]:
        """Extract sorting attributes from query."""
        sortings = []
        
        # Pattern for sorting operations
        sorting_patterns = [
            (r"(order by|sort by|ordered by|sorted by) ([\w\s]+) (ascending|asc|increasing)", "asc", r"\2"),
            (r"(order by|sort by|ordered by|sorted by) ([\w\s]+) (descending|desc|decreasing)", "desc", r"\2"),
            (r"(order by|sort by|ordered by|sorted by) ([\w\s]+)", "asc", r"\2"),  # Default to ascending
        ]
        
        for pattern, direction, target_pattern in sorting_patterns:
            for match in re.finditer(pattern, query):
                target = re.sub(pattern, target_pattern, match.group(0))
                
                sortings.append({
                    "attribute_value": match.group(0),
                    "direction": direction,
                    "target": target.strip(),
                    "confidence": 0.7
                })
        
        return sortings
    
    def _extract_limits(self, query: str) -> List[Dict[str, Any]]:
        """Extract limit attributes from query."""
        limits = []
        
        # Pattern for limit operations
        limit_patterns = [
            r"(top|first) (\d+)",
            r"(limit to|limited to|at most) (\d+)",
            r"(only|just|show) (\d+)",
        ]
        
        for pattern in limit_patterns:
            for match in re.finditer(pattern, query):
                if len(match.groups()) >= 2:
                    limit_value = match.group(2)
                    
                    limits.append({
                        "attribute_value": match.group(0),
                        "value": int(limit_value),
                        "confidence": 0.8
                    })
        
        return limits