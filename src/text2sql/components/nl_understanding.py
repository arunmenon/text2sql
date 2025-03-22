import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

from src.llm.client import LLMClient
from src.text2sql.models import StructuredQuery, StructuredQueryMeta, AmbiguityAssessment

logger = logging.getLogger(__name__)

class NLUnderstandingComponent:
    """Component for understanding natural language queries"""
    
    def __init__(self, llm_client: LLMClient):
        """
        Initialize NL understanding component.
        
        Args:
            llm_client: LLM client for text generation
        """
        self.llm_client = llm_client
        
    async def parse_query(self, natural_language_query: str, tenant_id: str) -> StructuredQuery:
        """
        Parse natural language query and extract key components.
        
        Args:
            natural_language_query: The query to parse
            tenant_id: Tenant ID
            
        Returns:
            Structured representation of the query
        """
        # Extract query intent and components using LLM
        parsing_prompt = self._build_parsing_prompt(natural_language_query)
        parsing_schema = self._get_parsing_schema()
        
        parsing_result = await self.llm_client.generate_structured(
            parsing_prompt, parsing_schema
        )
        
        # Analyze ambiguity level
        ambiguity_assessment = self._assess_ambiguity(parsing_result)
        
        # Construct structured query object
        structured_query = StructuredQuery(
            primary_intent=parsing_result.get("primary_intent", "selection"),
            main_entities=parsing_result.get("main_entities", []),
            attributes=parsing_result.get("attributes", []),
            filters=parsing_result.get("filters", []),
            grouping_dimensions=parsing_result.get("grouping_dimensions", []),
            sorting_criteria=parsing_result.get("sorting_criteria", []),
            time_references=parsing_result.get("time_references", []),
            aggregation_functions=parsing_result.get("aggregation_functions", []),
            limit=parsing_result.get("limit"),
            identified_ambiguities=parsing_result.get("identified_ambiguities", []),
            ambiguity_assessment=ambiguity_assessment,
            _meta=StructuredQueryMeta(
                raw_query=natural_language_query,
                parsing_timestamp=datetime.now().isoformat(),
                parser_version="1.0"
            )
        )
        
        return structured_query
    
    def _build_parsing_prompt(self, query: str) -> str:
        """Build prompt for LLM to parse the query"""
        return f"""
        Parse the following database query and extract key components:
        
        QUERY: {query}
        
        Extract and structure the following elements:
        1. Primary intent (e.g., selection, aggregation, comparison, trending)
        2. Main entities (tables likely involved)
        3. Attributes of interest (columns likely needed)
        4. Filters or conditions (predicates to apply)
        5. Grouping dimensions (what to group by)
        6. Sorting criteria (what to order by)
        7. Time references (temporal context)
        8. Aggregation functions needed (COUNT, SUM, AVG, etc.)
        9. Limit or sampling indications
        10. Identified ambiguities or vague terms
        
        Format as structured JSON according to the provided schema.
        """
    
    def _get_parsing_schema(self) -> Dict:
        """Get JSON schema for parsing result"""
        return {
            "type": "object",
            "properties": {
                "primary_intent": {"type": "string"},
                "main_entities": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "attributes": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "filters": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "field": {"type": "string"},
                            "operator": {"type": "string"},
                            "value": {"type": ["string", "number", "boolean", "null"]}
                        }
                    }
                },
                "grouping_dimensions": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "sorting_criteria": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "field": {"type": "string"},
                            "direction": {"type": "string", "enum": ["asc", "desc"]}
                        }
                    }
                },
                "time_references": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string"},
                            "value": {"type": "string"}
                        }
                    }
                },
                "aggregation_functions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "function": {"type": "string"},
                            "field": {"type": "string"}
                        }
                    }
                },
                "limit": {"type": ["integer", "null"]},
                "identified_ambiguities": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            }
        }
    
    def _assess_ambiguity(self, parsing_result: Dict[str, Any]) -> AmbiguityAssessment:
        """Analyze how ambiguous the query is"""
        ambiguity_score = 0.0
        ambiguity_factors = []
        
        # Check for vague or missing components
        if not parsing_result.get("main_entities") or len(parsing_result.get("main_entities", [])) == 0:
            ambiguity_score += 0.3
            ambiguity_factors.append("missing_entities")
            
        if parsing_result.get("identified_ambiguities") and len(parsing_result.get("identified_ambiguities", [])) > 0:
            ambiguity_score += 0.1 * min(len(parsing_result["identified_ambiguities"]), 5)
            ambiguity_factors.append("explicit_ambiguities")
        
        # Check for time reference ambiguity
        time_refs = parsing_result.get("time_references", [])
        if any(ref.get("type") == "relative" for ref in time_refs):
            ambiguity_score += 0.1
            ambiguity_factors.append("relative_time")
        
        # Check for missing attributes but present aggregations
        if (not parsing_result.get("attributes") or len(parsing_result.get("attributes", [])) == 0) and \
           parsing_result.get("aggregation_functions") and len(parsing_result.get("aggregation_functions", [])) > 0:
            ambiguity_score += 0.2
            ambiguity_factors.append("implicit_attributes")
        
        # Additional ambiguity checks could be added here
        
        return AmbiguityAssessment(
            score=min(ambiguity_score, 1.0),  # 0.0-1.0 scale
            factors=ambiguity_factors
        )