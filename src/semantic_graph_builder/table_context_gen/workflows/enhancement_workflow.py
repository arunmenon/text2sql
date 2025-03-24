"""
Schema Enhancement Workflow

Enhances schema metadata with LLM-generated insights and annotations for improved text-to-SQL.
Previously located in src/text2sql/metadata_enhancement.py
"""
import logging
import os
import asyncio
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Set

from src.llm.client import LLMClient
from src.graph_storage.neo4j_client import Neo4jClient
from src.semantic_graph_builder.utils.prompt_loader import PromptLoader

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
        self.prompt_loader = PromptLoader()
        
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
            
            # 3. Enhance column descriptions
            await self._enhance_column_descriptions(tenant_id, tables)
            
            # 4. Generate business glossary
            await self._generate_business_glossary(tenant_id, dataset_id, tables)
            
            # 5. Analyze semantic relationships
            await self._analyze_semantic_relationships(tenant_id, tables)
            
            # 6. Identify business domain
            await self._identify_business_domain(tenant_id, dataset_id, tables)
            
            # 7. Generate concept tags
            await self._generate_concept_tags(tenant_id, tables)
            
            # 8. Generate sample queries
            await self._generate_sample_queries(tenant_id, tables)
            
            # 9. Record completion
            self._record_workflow_completion(tenant_id, dataset_id)
            
            logging.info(f"Schema enhancement workflow completed for {tenant_id}/{dataset_id}")
            return True
            
        except Exception as e:
            logging.error(f"Error in schema enhancement workflow: {e}")
            self._record_workflow_failure(tenant_id, dataset_id, str(e))
            return False
    
    async def _enhance_table_descriptions(self, tenant_id: str, tables: List[Dict[str, Any]]):
        """Enhance table descriptions with LLM"""
        enhanced_count = 0
        
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
            
            # Update table metadata in Neo4j
            if hasattr(self.neo4j_client, 'update_table_metadata'):
                self.neo4j_client.update_table_metadata(
                    tenant_id=tenant_id,
                    table_name=table_name,
                    metadata={
                        "description": enhanced_description,
                        "description_enhanced": True,
                        "description_enhanced_at": datetime.now().isoformat()
                    }
                )
                enhanced_count += 1
            else:
                logger.warning("Neo4jClient doesn't have update_table_metadata method")
            
            logger.info(f"Enhanced description for table {table_name}")
            
        logger.info(f"Enhanced descriptions for {enhanced_count} tables")
    
    async def _enhance_column_descriptions(self, tenant_id: str, tables: List[Dict[str, Any]]):
        """Enhance column descriptions with LLM"""
        enhanced_count = 0
        
        for table in tables:
            table_name = table.get("name")
            if not table_name:
                continue
                
            # Get columns for the table
            columns = self.neo4j_client.get_columns_for_table(tenant_id, table_name)
            if not columns:
                continue
                
            # Group columns without good descriptions (batch processing)
            columns_to_enhance = []
            for col in columns:
                col_name = col.get("name") or col.get("column_name")
                col_description = col.get("description", "")
                
                if not col_name:
                    continue
                    
                if not col_description or len(col_description) < 30:
                    columns_to_enhance.append(col)
            
            if not columns_to_enhance:
                continue
                
            # Process columns in batches of 5 to avoid overloading the LLM
            batch_size = 5
            column_batches = [columns_to_enhance[i:i + batch_size] for i in range(0, len(columns_to_enhance), batch_size)]
            
            for batch in column_batches:
                # Create prompt for column enhancement
                prompt = self._build_column_enhancement_prompt(table_name, batch, columns)
                
                # Generate enhanced column descriptions
                try:
                    response = await self.llm_client.generate_structured_output(prompt, {
                        "column_descriptions": [
                            {
                                "column_name": "string",
                                "description": "string",
                                "business_purpose": "string",
                                "data_constraints": "string",
                                "potential_joins": "string"
                            }
                        ]
                    })
                    
                    # Update column metadata in Neo4j
                    for col_desc in response.get("column_descriptions", []):
                        col_name = col_desc.get("column_name")
                        description = col_desc.get("description", "")
                        
                        if col_name and description:
                            # Combine all fields into a rich description
                            rich_description = f"{description}\n\nBusiness purpose: {col_desc.get('business_purpose', '')}\n\nData constraints: {col_desc.get('data_constraints', '')}\n\nPotential joins: {col_desc.get('potential_joins', '')}"
                            
                            if hasattr(self.neo4j_client, 'update_column_metadata'):
                                self.neo4j_client.update_column_metadata(
                                    tenant_id=tenant_id,
                                    table_name=table_name,
                                    column_name=col_name,
                                    metadata={
                                        "description": rich_description,
                                        "business_purpose": col_desc.get("business_purpose", ""),
                                        "data_constraints": col_desc.get("data_constraints", ""),
                                        "potential_joins": col_desc.get("potential_joins", ""),
                                        "description_enhanced": True,
                                        "description_enhanced_at": datetime.now().isoformat()
                                    }
                                )
                                enhanced_count += 1
                            else:
                                logger.warning("Neo4jClient doesn't have update_column_metadata method")
                            
                except Exception as e:
                    logger.error(f"Error enhancing columns for {table_name}: {e}")
            
        logger.info(f"Enhanced descriptions for {enhanced_count} columns")
    
    async def _generate_business_glossary(self, tenant_id: str, dataset_id: str, tables: List[Dict[str, Any]]):
        """Generate business glossary for the database"""
        # Extract all table and column names for context
        table_info = []
        for table in tables:
            table_name = table.get("name")
            if not table_name:
                continue
                
            description = table.get("description", "No description")
            columns = self.neo4j_client.get_columns_for_table(tenant_id, table_name)
            column_info = [f"{col.get('name') or col.get('column_name')} ({col.get('data_type', 'unknown')})" 
                          for col in columns if col.get('name') or col.get('column_name')]
            
            table_info.append({
                "table": table_name,
                "description": description,
                "columns": column_info
            })
        
        # Use the prompt template for glossary generation
        prompt = self.prompt_loader.format_prompt(
            "business_glossary_generation",
            schema_json=json.dumps(table_info, indent=2)
        )
        
        # Generate business glossary
        glossary = await self.llm_client.generate(prompt)
        
        # Store glossary in Neo4j
        if hasattr(self.neo4j_client, 'store_business_glossary'):
            self.neo4j_client.store_business_glossary(
                tenant_id=tenant_id,
                dataset_id=dataset_id,
                glossary=glossary,
                metadata={
                    "generated_at": datetime.now().isoformat(),
                    "schema_version": datetime.now().strftime("%Y%m%d")
                }
            )
        else:
            logger.warning("Neo4jClient doesn't have store_business_glossary method")
            
        logger.info(f"Generated business glossary for dataset {dataset_id}")
    
    async def _analyze_semantic_relationships(self, tenant_id: str, tables: List[Dict[str, Any]]):
        """Analyze semantic relationships between tables"""
        # Get existing relationships to avoid duplication
        existing_relationships = set()
        
        for table in tables:
            table_name = table.get("name")
            if not table_name:
                continue
                
            rels = self.neo4j_client.get_relationships_for_table(tenant_id, table_name)
            for rel in rels:
                # Extract source and target column info
                if 'source' in rel and 'target' in rel:
                    source = rel['source']
                    target = rel['target']
                    
                    source_table = source.get('table_name')
                    source_col = source.get('name')
                    target_table = target.get('table_name')
                    target_col = target.get('name')
                    
                    if source_table and source_col and target_table and target_col:
                        rel_key = f"{source_table}.{source_col}|{target_table}.{target_col}"
                        existing_relationships.add(rel_key)
        
        # Analyze tables in pairs
        total_relationships = 0
        processed_pairs = set()
        
        for i, table1 in enumerate(tables):
            for j, table2 in enumerate(tables):
                # Skip self-comparison and already processed pairs
                if i == j or f"{i}|{j}" in processed_pairs or f"{j}|{i}" in processed_pairs:
                    continue
                    
                table1_name = table1.get("name")
                table2_name = table2.get("name")
                
                if not table1_name or not table2_name:
                    continue
                    
                # Skip pairs with no semantic relationship
                if not self._might_have_semantic_relationship(table1, table2):
                    continue
                
                # Get columns for context
                table1_columns = self.neo4j_client.get_columns_for_table(tenant_id, table1_name)
                table2_columns = self.neo4j_client.get_columns_for_table(tenant_id, table2_name)
                
                # Create prompt for relationship analysis
                prompt = self._build_semantic_relationship_prompt(
                    table1_name, table1_columns, 
                    table2_name, table2_columns
                )
                
                # Generate relationship analysis
                try:
                    response = await self.llm_client.generate_structured_output(prompt, {
                        "semantic_relationships": [
                            {
                                "source_table": "string",
                                "source_column": "string",
                                "target_table": "string",
                                "target_column": "string",
                                "relationship_type": "string",  # one-to-many, many-to-many, etc.
                                "confidence": "number",
                                "explanation": "string"
                            }
                        ]
                    })
                    
                    # Store relationships in Neo4j
                    for rel in response.get("semantic_relationships", []):
                        source_table = rel.get("source_table")
                        source_column = rel.get("source_column")
                        target_table = rel.get("target_table")
                        target_column = rel.get("target_column")
                        confidence = rel.get("confidence", 0.7)
                        explanation = rel.get("explanation", "")
                        relationship_type = rel.get("relationship_type", "")
                        
                        # Skip relationships below threshold
                        if confidence < 0.6:
                            continue
                            
                        # Skip existing relationships
                        rel_key = f"{source_table}.{source_column}|{target_table}.{target_column}"
                        if rel_key in existing_relationships:
                            continue
                        
                        if source_table and source_column and target_table and target_column:
                            self.neo4j_client.create_relationship(
                                tenant_id=tenant_id,
                                source_table=source_table,
                                source_column=source_column,
                                target_table=target_table,
                                target_column=target_column,
                                confidence=confidence,
                                detection_method="llm_semantic",
                                relationship_type=relationship_type,
                                metadata={"explanation": explanation}
                            )
                            total_relationships += 1
                            existing_relationships.add(rel_key)
                    
                except Exception as e:
                    logger.error(f"Error analyzing relationship between {table1_name} and {table2_name}: {e}")
                
                # Mark this pair as processed
                processed_pairs.add(f"{i}|{j}")
        
        logger.info(f"Added {total_relationships} semantic relationships")
    
    def _might_have_semantic_relationship(self, table1: Dict, table2: Dict) -> bool:
        """Determine if two tables might have a semantic relationship worth analyzing"""
        table1_name = table1.get("name", "").lower()
        table2_name = table2.get("name", "").lower()
        
        # Check for name-based relationships
        # e.g., "orders" and "order_items" or "customers" and "customer_addresses"
        if table1_name in table2_name or table2_name in table1_name:
            return True
            
        # Check for common business entity pairs
        common_pairs = [
            ("customers", "orders"),
            ("products", "categories"),
            ("users", "roles"),
            ("invoices", "payments"),
            ("employees", "departments"),
            ("students", "courses"),
            ("vendors", "purchases")
        ]
        
        for entity1, entity2 in common_pairs:
            if (entity1 in table1_name and entity2 in table2_name) or \
               (entity1 in table2_name and entity2 in table1_name):
                return True
        
        # Default to true to err on the side of analyzing more relationships
        # In a production system, this could be more selective
        return True
    
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
        
        # Use the prompt template for domain analysis
        prompt = self.prompt_loader.format_prompt(
            "domain_analysis",
            schema_text="\n".join(table_info)
        )
        
        # Generate domain analysis
        domain_analysis = await self.llm_client.generate(prompt)
        
        # Store domain analysis in Neo4j
        if hasattr(self.neo4j_client, 'update_dataset_metadata'):
            self.neo4j_client.update_dataset_metadata(
                tenant_id=tenant_id,
                dataset_id=dataset_id,
                metadata={
                    "domain_analysis": domain_analysis,
                    "analysis_generated_at": datetime.now().isoformat()
                }
            )
        else:
            logger.warning("Neo4jClient doesn't have update_dataset_metadata method")
            
        logger.info(f"Generated business domain analysis for dataset {dataset_id}")
    
    async def _generate_concept_tags(self, tenant_id: str, tables: List[Dict[str, Any]]):
        """Generate concept tags for tables and columns"""
        for table in tables:
            table_name = table.get("name")
            if not table_name:
                continue
                
            # Get columns for context
            columns = self.neo4j_client.get_columns_for_table(tenant_id, table_name)
            
            # Format columns for the prompt
            columns_json = json.dumps([{
                "name": col.get('name') or col.get('column_name', ''),
                "data_type": col.get('data_type', 'unknown'),
                "description": col.get('description', 'No description')
            } for col in columns if col.get('name') or col.get('column_name')], indent=2)
            
            # Use the prompt template for concept tagging
            prompt = self.prompt_loader.format_prompt(
                "concept_tagging",
                table_name=table_name,
                table_description=table.get('description', 'No description'),
                columns_json=columns_json
            )
            
            # Generate tags
            try:
                response = await self.llm_client.generate_structured_output(prompt, {
                    "table_tags": ["string"],
                    "column_tags": [
                        {
                            "column_name": "string",
                            "tags": ["string"]
                        }
                    ]
                })
                
                # Store table tags in Neo4j
                table_tags = response.get("table_tags", [])
                if table_tags and hasattr(self.neo4j_client, 'update_table_metadata'):
                    self.neo4j_client.update_table_metadata(
                        tenant_id=tenant_id,
                        table_name=table_name,
                        metadata={"concept_tags": table_tags}
                    )
                
                # Store column tags in Neo4j
                for col_tag in response.get("column_tags", []):
                    col_name = col_tag.get("column_name")
                    tags = col_tag.get("tags", [])
                    
                    if col_name and tags and hasattr(self.neo4j_client, 'update_column_metadata'):
                        self.neo4j_client.update_column_metadata(
                            tenant_id=tenant_id,
                            table_name=table_name,
                            column_name=col_name,
                            metadata={"concept_tags": tags}
                        )
                        
            except Exception as e:
                logger.error(f"Error generating concept tags for {table_name}: {e}")
            
        logger.info(f"Generated concept tags for tables and columns")
    
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
        
        # Format relationships for prompt
        relationship_info = []
        for rel in all_relationships:
            if 'source' in rel and 'target' in rel and 'r' in rel:
                source = rel['source']
                target = rel['target']
                r = rel['r']
                
                source_table = source.get('table_name')
                source_col = source.get('name')
                target_table = target.get('table_name')
                target_col = target.get('name')
                confidence = r.get('confidence', 0)
                
                if source_table and source_col and target_table and target_col:
                    relationship_info.append(
                        f"{source_table}.{source_col} → {target_table}.{target_col} (confidence: {confidence:.2f})"
                    )
        
        # Format data for the prompt
        table_names = ", ".join([table.get("name") for table in tables if table.get("name")])
        relationships_text = chr(10).join(relationship_info[:10])
        
        # Use the prompt template for sample query generation
        prompt = self.prompt_loader.format_prompt(
            "sample_query_generation",
            table_names=table_names,
            relationships_text=relationships_text
        )
        
        # Generate sample queries
        sample_queries = await self.llm_client.generate(prompt)
        
        # Store sample queries in Neo4j
        if hasattr(self.neo4j_client, 'store_sample_queries'):
            self.neo4j_client.store_sample_queries(
                tenant_id=tenant_id,
                queries=sample_queries,
                metadata={
                    "generated_at": datetime.now().isoformat(),
                    "table_count": len(tables),
                    "schema_version": datetime.now().strftime("%Y%m%d")
                }
            )
        else:
            logger.warning("Neo4jClient doesn't have store_sample_queries method")
            
        logger.info(f"Generated sample queries for tenant {tenant_id}")
    
    def _build_description_enhancement_prompt(
        self, table_name: str, columns: List[Dict[str, Any]], original_description: str
    ) -> str:
        """Build prompt for description enhancement"""
        columns_text = "\n".join([
            f"- {col.get('name') or col.get('column_name')}: {col.get('data_type')} - {col.get('description', 'No description')}"
            for col in columns if col.get('name') or col.get('column_name')
        ])
        
        # Use the prompt template from the centralized location
        return self.prompt_loader.format_prompt(
            "table_description_enhancement",
            table_name=table_name,
            original_description=original_description if original_description else "No description available",
            columns_text=columns_text
        )
    
    def _build_column_enhancement_prompt(
        self, table_name: str, columns_to_enhance: List[Dict[str, Any]], all_columns: List[Dict[str, Any]]
    ) -> str:
        """Build prompt for column description enhancement"""
        # Format the columns that need enhancement
        columns_to_enhance_text = "\n".join([
            f"- {col.get('name') or col.get('column_name')}: {col.get('data_type')} - {col.get('description', 'No description')}"
            for col in columns_to_enhance if col.get('name') or col.get('column_name')
        ])
        
        # Provide context of all columns in the table
        all_columns_text = "\n".join([
            f"- {col.get('name') or col.get('column_name')}: {col.get('data_type')}"
            for col in all_columns if col.get('name') or col.get('column_name')
        ])
        
        # Use the prompt template
        return self.prompt_loader.format_prompt(
            "column_description_enhancement",
            table_name=table_name,
            columns_to_enhance_text=columns_to_enhance_text,
            all_columns_text=all_columns_text
        )
    
    def _build_semantic_relationship_prompt(
        self, table1_name: str, table1_columns: List[Dict[str, Any]],
        table2_name: str, table2_columns: List[Dict[str, Any]]
    ) -> str:
        """Build prompt for semantic relationship analysis"""
        # Format table 1 columns
        table1_columns_text = "\n".join([
            f"- {col.get('name') or col.get('column_name')}: {col.get('data_type')} - {col.get('description', 'No description')}"
            for col in table1_columns if col.get('name') or col.get('column_name')
        ])
        
        # Format table 2 columns
        table2_columns_text = "\n".join([
            f"- {col.get('name') or col.get('column_name')}: {col.get('data_type')} - {col.get('description', 'No description')}"
            for col in table2_columns if col.get('name') or col.get('column_name')
        ])
        
        # Use the prompt template
        return self.prompt_loader.format_prompt(
            "semantic_relationship_analysis",
            table1_name=table1_name,
            table1_columns_text=table1_columns_text,
            table2_name=table2_name,
            table2_columns_text=table2_columns_text
        )
    
    def _record_workflow_completion(self, tenant_id: str, dataset_id: str):
        """Record successful workflow completion"""
        logger.info(f"Enhancement completed for {tenant_id}/{dataset_id}")
        
        if hasattr(self.neo4j_client, 'record_workflow_status'):
            self.neo4j_client.record_workflow_status(
                tenant_id=tenant_id,
                dataset_id=dataset_id,
                workflow_name="schema_enhancement",
                status="completed",
                metadata={
                    "completed_at": datetime.now().isoformat(),
                    "version": "1.0"
                }
            )
    
    def _record_workflow_failure(self, tenant_id: str, dataset_id: str, error: str):
        """Record workflow failure"""
        logger.error(f"Enhancement failed for {tenant_id}/{dataset_id}: {error}")
        
        if hasattr(self.neo4j_client, 'record_workflow_status'):
            self.neo4j_client.record_workflow_status(
                tenant_id=tenant_id,
                dataset_id=dataset_id,
                workflow_name="schema_enhancement",
                status="failed",
                metadata={
                    "error": error,
                    "failed_at": datetime.now().isoformat(),
                    "version": "1.0"
                }
            )


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