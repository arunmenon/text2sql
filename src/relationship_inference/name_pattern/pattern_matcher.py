"""
Name Pattern Matcher

Analyzes column and table names to infer potential relationships.
"""
import logging
import re
from typing import Dict, List, Tuple, Optional

import Levenshtein

logger = logging.getLogger(__name__)

class PatternMatcher:
    """Matches naming patterns to infer relationships between tables."""
    
    def __init__(self):
        """Initialize the pattern matcher."""
        # Common patterns for foreign key naming
        self.fk_patterns = [
            # FK to PK pattern (customer_id -> customers.id)
            (r'(.+)_id$', r'\1', 'id'),
            
            # Reference patterns
            (r'(.+)_ref$', r'\1', 'id'),
            (r'(.+)_key$', r'\1', 'id'),
            (r'(.+)_fk$', r'\1', 'id'),
            
            # Alternative forms
            (r'(.+)_code$', r'\1', 'code'),
            (r'(.+)_num$', r'\1', 'num'),
            (r'(.+)_number$', r'\1', 'number'),
        ]
        
    def infer_relationships(self, tables: List[Dict]) -> List[Dict]:
        """
        Infer relationships based on naming patterns.
        
        Args:
            tables: List of table metadata dictionaries
            
        Returns:
            List of inferred relationships
        """
        relationships = []
        
        # Generate table name to columns mapping
        table_columns = {}
        for table in tables:
            table_name = table["table_name"]
            table_columns[table_name] = table.get("columns", [])
        
        # Check each table's columns for foreign key patterns
        for source_table_name, source_columns in table_columns.items():
            for source_column in source_columns:
                source_column_name = source_column.get("column_name") or source_column.get("name")
                
                # Skip if column name is None or empty
                if not source_column_name:
                    continue
                
                # Find potential relationships
                matches = self._find_matches(source_table_name, source_column_name, table_columns)
                
                # Add relationships with confidence scores
                for target_table, target_column, confidence, match_type in matches:
                    if confidence > 0:
                        relationships.append({
                            "source_table": source_table_name,
                            "source_column": source_column_name,
                            "target_table": target_table,
                            "target_column": target_column,
                            "confidence": confidence,
                            "detection_method": f"name_pattern_{match_type}"
                        })
        
        return relationships
    
    def _find_matches(
        self, 
        source_table: str, 
        source_column: str, 
        table_columns: Dict[str, List[Dict]]
    ) -> List[Tuple[str, str, float, str]]:
        """
        Find potential matches for a column based on naming patterns.
        
        Args:
            source_table: Source table name
            source_column: Source column name
            table_columns: Mapping of table names to column lists
            
        Returns:
            List of (target_table, target_column, confidence, match_type) tuples
        """
        matches = []
        
        # Skip system columns
        if source_column.startswith('_'):
            return matches
        
        # Check pattern-based matches first
        pattern_matches = self._find_pattern_matches(source_column, table_columns)
        matches.extend(pattern_matches)
        
        # Check for direct name matches
        direct_matches = self._find_direct_matches(source_table, source_column, table_columns)
        matches.extend(direct_matches)
        
        # Check for fuzzy matches with lower confidence
        if not matches:
            fuzzy_matches = self._find_fuzzy_matches(source_column, table_columns)
            matches.extend(fuzzy_matches)
        
        return matches
    
    def _find_pattern_matches(
        self, 
        source_column: str, 
        table_columns: Dict[str, List[Dict]]
    ) -> List[Tuple[str, str, float, str]]:
        """
        Find matches based on foreign key naming patterns.
        
        Args:
            source_column: Source column name
            table_columns: Mapping of table names to column lists
            
        Returns:
            List of (target_table, target_column, confidence, match_type) tuples
        """
        matches = []
        
        for pattern, table_transform, target_column in self.fk_patterns:
            # Check if column matches the pattern
            match = re.match(pattern, source_column, re.IGNORECASE)
            if not match:
                continue
                
            # Extract the entity name
            entity_name = match.group(1).lower()
            
            # Look for matching tables
            for table_name, columns in table_columns.items():
                # Skip if entity doesn't match table name
                table_base = table_name.lower()
                
                # Handle plurals
                singular = table_base[:-1] if table_base.endswith('s') else table_base
                
                if entity_name != table_base and entity_name != singular:
                    continue
                
                # Check if target column exists
                target_col_name = target_column
                for column in columns:
                    col_name = column.get("column_name") or column.get("name")
                    if col_name and col_name.lower() == target_col_name:
                        # Strong match - entity and column pattern match
                        matches.append((table_name, col_name, 0.9, "entity_pattern"))
                        break
        
        return matches
    
    def _find_direct_matches(
        self, 
        source_table: str, 
        source_column: str, 
        table_columns: Dict[str, List[Dict]]
    ) -> List[Tuple[str, str, float, str]]:
        """
        Find matches based on direct column name comparison.
        
        Args:
            source_table: Source table name
            source_column: Source column name
            table_columns: Mapping of table names to column lists
            
        Returns:
            List of (target_table, target_column, confidence, match_type) tuples
        """
        matches = []
        
        # Ignore common non-FK columns
        common_non_fk = ['created_at', 'updated_at', 'created_by', 'updated_by', 'is_active', 'is_deleted']
        if source_column.lower() in common_non_fk:
            return matches
        
        # Check exact column name matches
        for table_name, columns in table_columns.items():
            # Skip self-reference
            if table_name == source_table:
                continue
                
            for column in columns:
                col_name = column.get("column_name") or column.get("name")
                if not col_name:
                    continue
                    
                # Exact name match (e.g., id -> id)
                if col_name.lower() == source_column.lower():
                    # ID column matching is very common but not always a relationship
                    if col_name.lower() == 'id':
                        matches.append((table_name, col_name, 0.6, "id_match"))
                    else:
                        matches.append((table_name, col_name, 0.7, "exact_match"))
        
        return matches
    
    def _find_fuzzy_matches(
        self, 
        source_column: str, 
        table_columns: Dict[str, List[Dict]]
    ) -> List[Tuple[str, str, float, str]]:
        """
        Find matches based on fuzzy column name comparison.
        
        Args:
            source_column: Source column name
            table_columns: Mapping of table names to column lists
            
        Returns:
            List of (target_table, target_column, confidence, match_type) tuples
        """
        matches = []
        threshold = 0.8  # Similarity threshold
        
        # Skip if not likely a foreign key
        if not any(suffix in source_column.lower() for suffix in ['id', 'key', 'ref', 'code']):
            return matches
        
        # Process the source column name
        source_base = re.sub(r'_id$|_key$|_ref$|_code$', '', source_column.lower())
        
        for table_name, columns in table_columns.items():
            table_base = table_name.lower()
            
            # Handle plurals
            singular = table_base[:-1] if table_base.endswith('s') else table_base
            
            # Check if table name is similar to column base
            table_similarity = self._calculate_similarity(source_base, table_base)
            table_similarity = max(table_similarity, self._calculate_similarity(source_base, singular))
            
            # If table name is similar, look for primary key columns
            if table_similarity >= threshold:
                for column in columns:
                    col_name = column.get("column_name") or column.get("name")
                    if not col_name:
                        continue
                        
                    # Common primary key columns
                    if col_name.lower() in ('id', 'key', 'code'):
                        confidence = table_similarity * 0.8  # Adjust confidence
                        matches.append((table_name, col_name, confidence, "fuzzy_match"))
        
        return matches
    
    def _calculate_similarity(self, str1: str, str2: str) -> float:
        """
        Calculate string similarity using Levenshtein distance.
        
        Args:
            str1: First string
            str2: Second string
            
        Returns:
            Similarity score between 0.0 and 1.0
        """
        if not str1 or not str2:
            return 0.0
            
        max_len = max(len(str1), len(str2))
        if max_len == 0:
            return 1.0
            
        distance = Levenshtein.distance(str1, str2)
        return 1.0 - (distance / max_len)