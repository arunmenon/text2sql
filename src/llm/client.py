"""
LLM Client

Provides interface to LLM APIs (OpenAI, Anthropic)
"""
import logging
import json
import os
from typing import Dict, Any, Optional, List, Union
import httpx
import asyncio

logger = logging.getLogger(__name__)

class LLMClient:
    """Client for interacting with LLM APIs"""
    
    def __init__(self, api_key: str, provider: str = "openai", model: str = "gpt-4o", timeout_ms: int = 120000):
        """
        Initialize LLM client.
        
        Args:
            api_key: API key for the LLM service
            provider: Provider name ("openai" or "anthropic")
            model: Model identifier
            timeout_ms: Timeout in milliseconds (default: 120000ms = 2 minutes)
        """
        self.api_key = api_key
        self.provider = provider.lower()
        self.model = model
        # Convert to seconds for httpx
        timeout_seconds = timeout_ms / 1000.0
        self.client = httpx.AsyncClient(timeout=timeout_seconds)
    
    async def generate(self, prompt: str, temperature: float = 0.0) -> str:
        """
        Generate text using LLM.
        
        Args:
            prompt: The prompt to send to the LLM
            temperature: Sampling temperature (0.0-1.0)
            
        Returns:
            Generated text
        """
        try:
            if self.provider == "anthropic":
                return await self._generate_anthropic(prompt, temperature)
            else:  # Default to OpenAI
                return await self._generate_openai(prompt, temperature)
                
        except Exception as e:
            logger.error(f"Error in LLM generation: {e}")
            # Provide a fallback response
            return f"Error generating response: {str(e)}"
    
    async def _generate_anthropic(self, prompt: str, temperature: float) -> str:
        """Generate text using Anthropic Claude API"""
        url = "https://api.anthropic.com/v1/messages"
        
        # Request headers
        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }
        
        # Request payload
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
            "max_tokens": 4000
        }
        
        # Make API request
        response = await self.client.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        # Parse response
        result = response.json()
        
        # Extract content
        content = result["content"][0]["text"]
        
        return content
    
    async def _generate_openai(self, prompt: str, temperature: float) -> str:
        """Generate text using OpenAI API"""
        url = "https://api.openai.com/v1/chat/completions"
        
        # Request headers
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # Request payload
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt}
            ],
            "temperature": temperature,
            "max_tokens": 4000
        }
        
        # Make API request
        response = await self.client.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        # Parse response
        result = response.json()
        
        # Extract content
        content = result["choices"][0]["message"]["content"]
        
        return content
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()

    async def generate_structured_output(self, prompt: str, schema: Dict, temperature: float = 0.0) -> Dict:
        """
        Generate structured output using LLM.
        
        Args:
            prompt: The prompt to send to the LLM
            schema: JSON schema for the expected output
            temperature: Sampling temperature (0.0-1.0)
            
        Returns:
            Structured data
        """
        # Add schema instructions to prompt
        structured_prompt = f"""
        {prompt}
        
        Your response should be valid JSON following this schema:
        {json.dumps(schema, indent=2)}
        
        Ensure your response is properly formatted as JSON. Do not include any explanatory text outside the JSON structure.
        """
        
        # Generate response
        response = await self.generate(structured_prompt, temperature)
        
        # Extract JSON from response
        try:
            # Find JSON in response
            json_start = response.find('{')
            json_end = response.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_str = response[json_start:json_end]
                result = json.loads(json_str)
                return result
            else:
                # Fallback if no JSON found
                logger.warning("No valid JSON found in LLM response")
                return {"error": "No valid JSON found in response", "raw_response": response}
                
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from LLM response: {e}")
            return {"error": f"Invalid JSON: {str(e)}", "raw_response": response}
    
    # Alias for backward compatibility with existing code
    async def generate_structured(self, prompt: str, schema: Dict, temperature: float = 0.0) -> Dict:
        """
        Alias for generate_structured_output for backward compatibility.
        
        Args:
            prompt: The prompt to send to the LLM
            schema: JSON schema for the expected output
            temperature: Sampling temperature (0.0-1.0)
            
        Returns:
            Structured data
        """
        return await self.generate_structured_output(prompt, schema, temperature)