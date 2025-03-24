"""
Term Refiner agent for enhancing business glossary terms.
"""

import logging
import json
from typing import Dict, Any

from src.llm.client import LLMClient
from ..utils.prompt_loader import PromptLoader
from ..utils.schema_loader import SchemaLoader
from ..utils.formatters import format_schema, format_terms

logger = logging.getLogger(__name__)

class TermRefinerAgent:
    """
    Term Refiner Agent.
    
    Responsible for improving term definitions, adding synonyms,
    and ensuring glossary completeness.
    """
    
    def __init__(self, llm_client: LLMClient):
        """
        Initialize the Term Refiner agent.
        
        Args:
            llm_client: LLM client for text generation
        """
        self.llm_client = llm_client
        self.prompt_loader = PromptLoader()
        self.schema_loader = SchemaLoader()
        
    async def refine(self, initial_terms: Dict[str, Any], schema_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Refine business terms by improving definitions and adding synonyms.
        
        Args:
            initial_terms: Initial business terms
            schema_data: Database schema information
            
        Returns:
            Refined business terms dictionary
        """
        try:
            # Format inputs for prompt
            formatted_terms = format_terms(initial_terms)
            formatted_schema = format_schema(schema_data)
            
            # Load prompt and format it
            prompt = self.prompt_loader.format_prompt(
                "term_refiner",
                formatted_terms=formatted_terms,
                formatted_schema=formatted_schema
            )
            
            # Load schema definition
            term_schema = self.schema_loader.load_schema("term_schema")
            
            # Refine terms using LLM
            logger.debug("Sending request to LLM for term refinement")
            response = await self.llm_client.generate_structured(prompt, term_schema)
            
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
                        # If we can't extract JSON, return the initial terms
                        logger.error("Could not extract JSON from response, using initial terms")
                        return initial_terms
                except Exception as e:
                    logger.error(f"Error parsing LLM response: {e}")
                    return initial_terms
            
            # Ensure business_terms is present
            if "business_terms" not in response:
                logger.warning("Response missing 'business_terms' key, using initial terms")
                return initial_terms
                
            logger.debug(f"Refined {len(response.get('business_terms', []))} business terms")
            return response
            
        except Exception as e:
            logger.error(f"Error in term refinement: {e}")
            # Return initial terms as fallback
            return initial_terms