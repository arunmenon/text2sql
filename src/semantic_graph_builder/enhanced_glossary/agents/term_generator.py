"""
Term Generator agent for initial business glossary term generation.
"""

import logging
import json
from typing import Dict, Any

from src.llm.client import LLMClient
from ..utils.prompt_loader import PromptLoader
from ..utils.schema_loader import SchemaLoader
from ..utils.formatters import format_schema

logger = logging.getLogger(__name__)

class TermGeneratorAgent:
    """
    Term Generator Agent.
    
    Responsible for identifying potential business terms from schema
    and creating initial definitions and technical mappings.
    """
    
    def __init__(self, llm_client: LLMClient):
        """
        Initialize the Term Generator agent.
        
        Args:
            llm_client: LLM client for text generation
        """
        self.llm_client = llm_client
        self.prompt_loader = PromptLoader()
        self.schema_loader = SchemaLoader()
        
    async def generate(self, schema_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate initial business terms from schema data.
        
        Args:
            schema_data: Database schema information
            
        Returns:
            Initial business terms dictionary
        """
        try:
            # Format schema for prompt
            formatted_schema = format_schema(schema_data)
            logger.debug(f"Formatted schema: {formatted_schema[:200]}...")
            
            # Load prompt and format it
            prompt = self.prompt_loader.format_prompt(
                "term_generator",
                formatted_schema=formatted_schema
            )
            
            # Load schema definition
            term_schema = self.schema_loader.load_schema("term_schema")
            
            # Generate terms using LLM
            logger.debug("Sending request to LLM for term generation")
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
                        # Create default structure
                        logger.error("Could not extract JSON from response, using default structure")
                        response = {"business_terms": []}
                except Exception as e:
                    logger.error(f"Error parsing LLM response: {e}")
                    response = {"business_terms": []}
            
            # Ensure business_terms is present
            if "business_terms" not in response:
                logger.warning("Response missing 'business_terms' key, adding empty array")
                response["business_terms"] = []
                
            logger.debug(f"Generated {len(response.get('business_terms', []))} business terms")
            return response
            
        except Exception as e:
            logger.error(f"Error in term generation: {e}")
            # Return a valid empty response as fallback
            return {"business_terms": []}