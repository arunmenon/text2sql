from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from pydantic import BaseModel, Field

class AmbiguityAssessment(BaseModel):
    """Assessment of query ambiguity"""
    score: float = Field(..., description="Ambiguity score (0.0-1.0)")
    factors: List[str] = Field(default_factory=list, description="Factors contributing to ambiguity")

class StructuredQueryMeta(BaseModel):
    """Metadata for a structured query"""
    raw_query: str = Field(..., description="Original raw query")
    parsing_timestamp: str = Field(default_factory=lambda: datetime.now().isoformat(), description="When parsing occurred")
    parser_version: str = Field(default="1.0", description="Parser version")

class StructuredQuery(BaseModel):
    """Structured representation of a natural language query"""
    primary_intent: str = Field(..., description="Primary query intent")
    main_entities: List[str] = Field(default_factory=list, description="Main entities (tables) mentioned")
    attributes: List[str] = Field(default_factory=list, description="Attributes (columns) of interest")
    filters: List[Dict[str, Any]] = Field(default_factory=list, description="Filter conditions")
    grouping_dimensions: List[str] = Field(default_factory=list, description="Grouping dimensions")
    sorting_criteria: List[Dict[str, Any]] = Field(default_factory=list, description="Sorting criteria")
    time_references: List[Dict[str, Any]] = Field(default_factory=list, description="Time references")
    aggregation_functions: List[Dict[str, Any]] = Field(default_factory=list, description="Aggregation functions")
    limit: Optional[int] = Field(default=None, description="Result limit")
    identified_ambiguities: List[str] = Field(default_factory=list, description="Identified ambiguous terms")
    ambiguity_assessment: AmbiguityAssessment = Field(...)
    _meta: StructuredQueryMeta = Field(...)

class ResolvedEntity(BaseModel):
    """Resolution of an entity mention to an actual table"""
    table_name: str = Field(..., description="Resolved table name")
    confidence: float = Field(..., description="Confidence in resolution (0.0-1.0)")
    resolution_method: str = Field(..., description="Method used for resolution")

class ResolvedAttribute(BaseModel):
    """Resolution of an attribute mention to an actual column"""
    table_name: str = Field(..., description="Table containing the attribute")
    column_name: str = Field(..., description="Resolved column name")
    confidence: float = Field(..., description="Confidence in resolution (0.0-1.0)")
    resolution_method: str = Field(..., description="Method used for resolution")

class ResolvedConcept(BaseModel):
    """Resolution of a semantic concept to database constructs"""
    concept: str = Field(..., description="Original concept")
    interpretation: str = Field(..., description="Interpretation of concept")
    implementation: Dict[str, Any] = Field(..., description="Implementation details")
    confidence: float = Field(..., description="Confidence in resolution (0.0-1.0)")

class QueryInterpretation(BaseModel):
    """One possible interpretation of an ambiguous query"""
    entities: Dict[str, ResolvedEntity] = Field(..., description="Resolved entities")
    attributes: Dict[str, ResolvedAttribute] = Field(default_factory=dict, description="Resolved attributes")
    concepts: Dict[str, ResolvedConcept] = Field(default_factory=dict, description="Resolved concepts")
    join_paths: Dict[str, Any] = Field(default_factory=dict, description="Join paths")
    confidence: float = Field(..., description="Confidence in interpretation (0.0-1.0)")
    is_primary: bool = Field(default=False, description="Whether this is the primary interpretation")
    rationale: str = Field(default="", description="Rationale for this interpretation")

class ResolvedQuery(BaseModel):
    """Query with all components resolved against schema"""
    resolved_entities: Dict[str, ResolvedEntity] = Field(..., description="Resolved entities")
    resolved_attributes: Dict[str, ResolvedAttribute] = Field(default_factory=dict, description="Resolved attributes")
    resolved_concepts: Dict[str, ResolvedConcept] = Field(default_factory=dict, description="Resolved concepts")
    join_paths: Dict[str, Any] = Field(..., description="Join paths between entities")
    interpretations: List[QueryInterpretation] = Field(default_factory=list, description="Possible interpretations")
    schema_context: Dict[str, Any] = Field(..., description="Schema context used for resolution")

class SQLResult(BaseModel):
    """Generated SQL with metadata"""
    sql: str = Field(..., description="Generated SQL query")
    explanation: str = Field(default="", description="Explanation of the SQL approach")
    assumptions: List[str] = Field(default_factory=list, description="Assumptions made during generation")
    approach: str = Field(default="", description="Approach taken for generation")
    interpretation: Optional[QueryInterpretation] = Field(default=None, description="Interpretation this SQL is based on")
    is_primary: bool = Field(default=True, description="Whether this is the primary SQL result")

class Text2SQLResponse(BaseModel):
    """Complete response to a text2sql request"""
    original_query: str = Field(..., description="Original natural language query")
    interpreted_as: str = Field(..., description="How the query was interpreted")
    ambiguity_level: float = Field(..., description="Assessed level of ambiguity (0.0-1.0)")
    sql_results: List[SQLResult] = Field(..., description="Generated SQL results")
    primary_interpretation: Optional[SQLResult] = Field(default=None, description="Primary interpretation")
    multiple_interpretations: bool = Field(default=False, description="Whether multiple interpretations were generated")
    entities_resolved: Dict[str, str] = Field(default_factory=dict, description="How entities were resolved")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

class Text2SQLRequest(BaseModel):
    """Request for text2sql conversion"""
    query: str = Field(..., description="Natural language query")
    tenant_id: str = Field(..., description="Tenant ID")
    context: Optional[Dict[str, Any]] = Field(default=None, description="Additional context")