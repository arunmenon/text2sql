"""
Enhanced Business Glossary Generator.

Orchestrates the multi-agent approach to business glossary generation.
"""

import logging
from typing import Dict, Any

from src.llm.client import LLMClient
from .agents.term_generator import TermGeneratorAgent
from .agents.term_refiner import TermRefinerAgent
from .agents.term_validator import TermValidatorAgent

logger = logging.getLogger(__name__)

class EnhancedBusinessGlossaryGenerator:
    """
    Enhanced Business Glossary Generator using a multi-agent approach.
    
    Orchestrates the three specialized agents:
    1. Term Generator - Creates initial business terms from schema
    2. Term Refiner - Enhances definitions, adds synonyms, ensures completeness
    3. Term Validator - Validates technical mappings and adds confidence scores
    """
    
    def __init__(self, llm_client: LLMClient, data_context_provider=None):
        """
        Initialize the enhanced business glossary generator.
        
        Args:
            llm_client: LLM client for text generation
            data_context_provider: Optional provider for sample data context
        """
        self.llm_client = llm_client
        self.data_context_provider = data_context_provider
        
        # Initialize the specialized agents
        self.term_generator = TermGeneratorAgent(llm_client, data_context_provider)
        self.term_refiner = TermRefinerAgent(llm_client)
        self.term_validator = TermValidatorAgent(llm_client)
    
    async def generate_enhanced_glossary(self, schema_data: Dict[str, Any], tenant_id: str) -> Dict[str, Any]:
        """
        Generate an enhanced business glossary using the multi-agent approach.
        
        This orchestrates the three-step process:
        1. Generate initial terms
        2. Refine those terms
        3. Validate and score the mappings
        
        Args:
            schema_data: Database schema information
            tenant_id: Tenant ID
            
        Returns:
            Enhanced business glossary with validated terms
        """
        try:
            # Step 1: Generate initial terms
            logger.info(f"[{tenant_id}] Generating initial business terms")
            initial_terms = await self.term_generator.generate(schema_data)
            
            # Step 2: Refine the terms
            logger.info(f"[{tenant_id}] Refining business terms")
            refined_terms = await self.term_refiner.refine(initial_terms, schema_data)
            
            # Step 3: Validate the terms and add confidence scores
            logger.info(f"[{tenant_id}] Validating business terms and technical mappings")
            validated_terms = await self.term_validator.validate(refined_terms, schema_data)
            
            return validated_terms
        except Exception as e:
            logger.error(f"Error generating enhanced glossary: {e}")
            # Fallback to initial terms if available, otherwise raise
            if 'initial_terms' in locals():
                logger.warning("Using initial terms as fallback due to error")
                return initial_terms
            raise