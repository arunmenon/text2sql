"""
Metadata Enhancement Workflow

Enhances schema metadata with LLM-generated insights and annotations.
"""
import logging
import os
import asyncio
from datetime import datetime
from typing import Dict, List, Any, Optional

from src.llm.client import LLMClient
from src.graph_storage.neo4j_client import Neo4jClient

logger = logging.getLogger(__name__)

class SchemaEnhancementWorkflow:
    """Orchestrates the end-to-end schema enhancement workflow"""
    
    def __init__(self, neo4j_client: Neo4jClient, llm_client: LLMClient):
        """
        Initialize schema enhancement workflow.
        
        Args:
            neo4j_client: Neo4j client for schema access
            llm_client: LLM client for metadata enhancement
        """
        self.neo4j_client = neo4j_client
        self.llm_client = llm_client
        
    async def run(self, tenant_id: str, dataset_id: str):
        """
        Execute the full schema enhancement workflow.
        
        Args:
            tenant_id: Tenant ID
            dataset_id: Dataset ID
            
        Returns:
            True if successful, False otherwise
        """
        logging.info(f"Starting schema enhancement workflow for {tenant_id}/{dataset_id}")
        
        try:
            # 1. Get basic schema from Neo4j
            tables = self.neo4j_client.get_tables_for_tenant(tenant_id)
            if not tables:
                logging.error(f"No tables found for tenant {tenant_id}")
                return False
                
            logging.info(f"Found {len(tables)} tables to enhance")
            
            # 2. Enhance table descriptions
            await self._enhance_table_descriptions(tenant_id, tables)
            
            # 3. Identify business domain
            await self._identify_business_domain(tenant_id, dataset_id, tables)
            
            # 4. Generate sample queries
            await self._generate_sample_queries(tenant_id, tables)
            
            # 5. Record completion
            self._record_workflow_completion(tenant_id, dataset_id)
            
            logging.info(f"Schema enhancement workflow completed for {tenant_id}/{dataset_id}")
            return True
            
        except Exception as e:
            logging.error(f"Error in schema enhancement workflow: {e}")
            self._record_workflow_failure(tenant_id, dataset_id, str(e))
            return False
    
    async def _enhance_table_descriptions(self, tenant_id: str, tables: List[Dict[str, Any]]):
        """Enhance table descriptions with LLM"""
        for table in tables:
            table_name = table.get("name")
            if not table_name:
                continue
                
            original_description = table.get("description", "")
            if original_description and len(original_description) > 100:
                # Skip tables with already detailed descriptions
                continue
                
            # Get columns for context
            columns = self.neo4j_client.get_columns_for_table(tenant_id, table_name)
            
            # Create prompt for enhancement
            prompt = self._build_description_enhancement_prompt(table_name, columns, original_description)
            
            # Generate enhanced description
            enhanced_description = await self.llm_client.generate(prompt)
            
            # Store enhanced description in Neo4j
            # This would require adding a method to Neo4jClient to update table metadata
            logger.info(f"Enhanced description for table {table_name}")
    
    def _build_description_enhancement_prompt(
        self, table_name: str, columns: List[Dict[str, Any]], original_description: str
    ) -> str:
        """Build prompt for description enhancement"""
        columns_text = "\n".join([
            f"- {col.get('name') or col.get('column_name')}: {col.get('data_type')} - {col.get('description', 'No description')}"
            for col in columns if col.get('name') or col.get('column_name')
        ])
        
        return f"""
        I need a comprehensive business description for a database table.
        
        Table name: {table_name}
        
        Current description: {original_description if original_description else "No description available"}
        
        Columns:
        {columns_text}
        
        Please provide a detailed business-oriented description of this table that explains:
        1. What kind of data it likely contains
        2. Its business purpose in the organization
        3. How it might relate to other tables (based on the column names)
        4. Typical use cases for this data
        
        Keep the description business-focused and around 2-3 sentences long.
        """
    
    async def _identify_business_domain(self, tenant_id: str, dataset_id: str, tables: List[Dict[str, Any]]):
        """Identify business domain for the entire dataset"""
        # Create context about tables and columns
        table_info = []
        for table in tables:
            table_name = table.get("name")
            if not table_name:
                continue
                
            description = table.get("description", "No description")
            columns = self.neo4j_client.get_columns_for_table(tenant_id, table_name)
            column_names = [col.get("name") or col.get("column_name") for col in columns if col.get("name") or col.get("column_name")]
            
            table_info.append(f"- Table: {table_name}\n  Description: {description}\n  Columns: {', '.join(column_names)}")
        
        # Create prompt
        prompt = f"""
        Analyze the following database schema and determine the business domain it represents:
        
        {"\n".join(table_info)}
        
        Please provide:
        1. The primary business domain (e.g., retail, healthcare, finance, etc.)
        2. Key business entities represented in this data
        3. Potential business questions this data could answer
        
        Format your response as a concise domain analysis.
        """
        
        # Generate domain analysis
        domain_analysis = await self.llm_client.generate(prompt)
        
        # Store domain analysis in Neo4j
        # This would require adding a method to Neo4jClient to store dataset metadata
        logger.info(f"Generated business domain analysis for dataset {dataset_id}")
    
    async def _generate_sample_queries(self, tenant_id: str, tables: List[Dict[str, Any]]):
        """Generate sample SQL queries based on the schema"""
        if len(tables) < 2:
            logger.info("Not enough tables to generate meaningful sample queries")
            return
            
        # Get relationships for context
        all_relationships = []
        for table in tables:
            table_name = table.get("name")
            if not table_name:
                continue
                
            relationships = self.neo4j_client.get_relationships_for_table(tenant_id, table_name)
            all_relationships.extend(relationships)
        
        # Create prompt
        table_names = [table.get("name") for table in tables if table.get("name")]
        prompt = f"""
        Generate 3 useful sample SQL queries for the following database schema:
        
        Tables: {", ".join(table_names)}
        
        For each query:
        1. Start with a clear comment explaining what business question the query answers
        2. Include joins between tables where appropriate
        3. Demonstrate filtering, aggregation, and sorting
        4. Make the queries realistic for business users
        
        Format as SQL only, with comments explaining each query.
        """
        
        # Generate sample queries
        sample_queries = await self.llm_client.generate(prompt)
        
        # Store sample queries in Neo4j
        # This would require adding a method to Neo4jClient to store sample queries
        logger.info(f"Generated sample queries for tenant {tenant_id}")
    
    def _record_workflow_completion(self, tenant_id: str, dataset_id: str):
        """Record successful workflow completion"""
        logger.info(f"Enhancement completed for {tenant_id}/{dataset_id}")
        # In a real implementation, this would store a record in Neo4j or another storage
    
    def _record_workflow_failure(self, tenant_id: str, dataset_id: str, error: str):
        """Record workflow failure"""
        logger.error(f"Enhancement failed for {tenant_id}/{dataset_id}: {error}")
        # In a real implementation, this would store a record in Neo4j or another storage


async def run_enhancement(tenant_id: str, dataset_id: str = None):
    """Run schema enhancement workflow as a standalone process"""
    try:
        # Get Neo4j connection details
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Get LLM API details
        llm_api_key = os.getenv("LLM_API_KEY")
        llm_model = os.getenv("LLM_MODEL", "claude-3-opus-20240229")
        
        if not llm_api_key:
            logger.error("LLM API key not configured")
            return False
        
        # Initialize clients
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        llm_client = LLMClient(api_key=llm_api_key, model=llm_model)
        
        # Initialize and run workflow
        workflow = SchemaEnhancementWorkflow(neo4j_client, llm_client)
        
        if dataset_id:
            # Enhance specific dataset
            logger.info(f"Enhancing schema for tenant {tenant_id}, dataset {dataset_id}")
            success = await workflow.run(tenant_id, dataset_id)
        else:
            # Enhance all datasets for tenant
            datasets = neo4j_client.get_datasets_for_tenant(tenant_id)
            logger.info(f"Enhancing schema for {len(datasets)} datasets")
            
            success = True
            for dataset in datasets:
                dataset_id = dataset["name"]
                dataset_success = await workflow.run(tenant_id, dataset_id)
                if not dataset_success:
                    success = False
        
        # Close Neo4j client
        neo4j_client.close()
        # Close LLM client
        await llm_client.close()
        
        return success
        
    except Exception as e:
        logger.error(f"Error in schema enhancement: {e}", exc_info=True)
        return False


# Function to call after schema extraction completes
def post_extraction_hook(tenant_id: str, project_id: str, dataset_id: str):
    """Hook to run after schema extraction completes"""
    logger.info(f"Schema extraction completed for {tenant_id}/{dataset_id}, triggering enhancement workflow")
    
    # Run the enhancement workflow in a background task
    asyncio.create_task(run_enhancement(tenant_id, dataset_id))