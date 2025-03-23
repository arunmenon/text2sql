"""
Term Validator agent for validating business term mappings.
"""

import logging
import json
from typing import Dict, Any

from src.llm.client import LLMClient
from ..utils.prompt_loader import PromptLoader
from ..utils.schema_loader import SchemaLoader
from ..utils.formatters import format_schema, format_terms

logger = logging.getLogger(__name__)

class TermValidatorAgent:
    """
    Term Validator Agent.
    
    Responsible for validating technical mappings and
    adding confidence scores and explanations.
    """
    
    def __init__(self, llm_client: LLMClient):
        """
        Initialize the Term Validator agent.
        
        Args:
            llm_client: LLM client for text generation
        """
        self.llm_client = llm_client
        self.prompt_loader = PromptLoader()
        self.schema_loader = SchemaLoader()
        
    async def validate(self, refined_terms: Dict[str, Any], schema_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate business terms and their technical mappings.
        
        Args:
            refined_terms: Refined business terms
            schema_data: Database schema information
            
        Returns:
            Validated business terms with confidence scores
        """
        try:
            # Format inputs for prompt
            formatted_terms = format_terms(refined_terms)
            formatted_schema = format_schema(schema_data)
            
            # Load prompt and format it
            prompt = self.prompt_loader.format_prompt(
                "term_validator",
                formatted_terms=formatted_terms,
                formatted_schema=formatted_schema
            )
            
            # Load schema definition
            validation_schema = self.schema_loader.load_schema("validation_schema")
            
            # Validate terms using LLM
            logger.debug("Sending request to LLM for term validation")
            response = await self.llm_client.generate_structured(prompt, validation_schema)
            
            # Ensure we have the expected structure
            if isinstance(response, str):
                logger.warning("LLM returned string instead of structured data, attempting to parse")
                try:
                    # Try to extract JSON from string
                    json_start = response.find('{')
                    json_end = response.rfind('}') + 1
                    
                    if json_start >= 0 and json_end > json_start:
                        json_str = response[json_start:json_end]
                        response = json.loads(json_str)
                    else:
                        # If we can't extract JSON, return the refined terms
                        logger.error("Could not extract JSON from response, using refined terms")
                        return refined_terms
                except Exception as e:
                    logger.error(f"Error parsing LLM response: {e}")
                    return refined_terms
            
            # If the validation results don't include business_terms, copy them from the refined terms
            if "business_terms" not in response and "validation_results" in response:
                # We might have validation results that need to be merged back into the refined terms
                logger.info("Merging validation results into refined terms")
                
                # Create a map of term names to validation results
                validation_map = {}
                for validation in response.get("validation_results", []):
                    term_name = validation.get("term_name")
                    if term_name:
                        validation_map[term_name] = validation
                
                # Copy refined terms and add validation data
                result = {"business_terms": []}
                for term in refined_terms.get("business_terms", []):
                    term_name = term.get("name")
                    if term_name in validation_map:
                        # Add validation data to the term
                        validation = validation_map[term_name]
                        term["confidence_score"] = validation.get("confidence_score", 0.7)
                        term["validation_notes"] = validation.get("notes", "")
                    
                    result["business_terms"].append(term)
                    
                return result
            elif "business_terms" not in response:
                logger.warning("Response missing both 'business_terms' and 'validation_results', using refined terms")
                return refined_terms
                
            logger.debug(f"Validated {len(response.get('business_terms', []))} business terms")
            return response
            
        except Exception as e:
            logger.error(f"Error in term validation: {e}")
            # Return refined terms as fallback
            return refined_terms