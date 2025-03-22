"""
Direct Normalized Metadata Enhancement

This module contains a modified version of the SchemaEnhancementWorkflow
that directly creates a normalized graph structure for business glossary
and other enhanced metadata.
"""
import logging
import os
import asyncio
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Set

from src.llm.client import LLMClient
from src.graph_storage.neo4j_client import Neo4jClient

logger = logging.getLogger(__name__)

class DirectEnhancementWorkflow:
    """
    Orchestrates the end-to-end schema enhancement workflow 
    with direct normalized graph structure creation
    """
    
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
        logging.info(f"Starting direct enhancement workflow for {tenant_id}/{dataset_id}")
        
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
            
            # 4. Generate normalized business glossary
            await self._generate_normalized_business_glossary(tenant_id, dataset_id, tables)
            
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
            
            logging.info(f"Direct enhancement workflow completed for {tenant_id}/{dataset_id}")
            return True
            
        except Exception as e:
            logging.error(f"Error in direct enhancement workflow: {e}")
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
            query = """
            MATCH (t:Table {tenant_id: $tenant_id, name: $table_name})
            SET t.description = $description,
                t.description_enhanced = true,
                t.description_enhanced_at = datetime()
            RETURN t
            """
            
            params = {
                "tenant_id": tenant_id,
                "table_name": table_name,
                "description": enhanced_description
            }
            
            result = self.neo4j_client._execute_query(query, params)
            if result:
                enhanced_count += 1
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
                col_name = col.get('name') or col.get('column_name')
                col_description = col.get('description', '')
                
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
                            
                            query = """
                            MATCH (c:Column {tenant_id: $tenant_id, table_name: $table_name, name: $column_name})
                            SET c.description = $description,
                                c.business_purpose = $business_purpose,
                                c.data_constraints = $data_constraints,
                                c.potential_joins = $potential_joins,
                                c.description_enhanced = true,
                                c.description_enhanced_at = datetime()
                            RETURN c
                            """
                            
                            params = {
                                "tenant_id": tenant_id,
                                "table_name": table_name,
                                "column_name": col_name,
                                "description": rich_description,
                                "business_purpose": col_desc.get("business_purpose", ""),
                                "data_constraints": col_desc.get("data_constraints", ""),
                                "potential_joins": col_desc.get("potential_joins", "")
                            }
                            
                            result = self.neo4j_client._execute_query(query, params)
                            if result:
                                enhanced_count += 1
                    
                except Exception as e:
                    logger.error(f"Error enhancing columns for {table_name}: {e}")
            
        logger.info(f"Enhanced descriptions for {enhanced_count} columns")
    
    async def _generate_normalized_business_glossary(self, tenant_id: str, dataset_id: str, tables: List[Dict[str, Any]]):
        """Generate normalized business glossary for the database directly in the graph"""
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
        
        # Create prompt for generating business glossary input
        prompt = f"""
        Analyze the following database schema and generate a comprehensive business glossary.
        
        Database schema:
        {json.dumps(table_info, indent=2)}
        
        For this database, create a business glossary that includes important business terms.
        For each term, provide:
        1. Term name
        2. Term definition in plain business language
        3. Technical mapping to tables/columns where relevant
        4. Related terms (other business terms that are related to this one)
        5. Synonyms for the term

        Additionally, include common business metrics and KPIs that can be derived from this data.
        For each metric, provide:
        1. Metric name
        2. Definition
        3. Which tables it can be derived from

        Format your response as a structured JSON with the following schema:
        {
          "business_terms": [
            {
              "name": "string",
              "definition": "string",
              "technical_mapping": {
                "tables": ["string"],
                "columns": [{"table": "string", "column": "string"}]
              },
              "related_terms": ["string"],
              "synonyms": ["string"]
            }
          ],
          "business_metrics": [
            {
              "name": "string",
              "definition": "string",
              "derived_from": ["string"]
            }
          ]
        }
        """
        
        # Generate structured glossary data
        logger.info("Generating business glossary...")
        response = await self.llm_client.generate_structured_output(prompt, {
            "business_terms": [
                {
                    "name": "string",
                    "definition": "string",
                    "technical_mapping": {
                        "tables": ["string"],
                        "columns": [{"table": "string", "column": "string"}]
                    },
                    "related_terms": ["string"],
                    "synonyms": ["string"]
                }
            ],
            "business_metrics": [
                {
                    "name": "string",
                    "definition": "string",
                    "derived_from": ["string"]
                }
            ]
        })
        
        # Create normalized glossary structure in Neo4j
        logger.info("Creating normalized business glossary structure...")
        
        # Clear any existing normalized glossary
        clear_query = """
        MATCH (g:BusinessGlossary {tenant_id: $tenant_id})
        DETACH DELETE g
        
        WITH 1 as dummy
        MATCH (g:NormalizedGlossary {tenant_id: $tenant_id})
        OPTIONAL MATCH (g)-[:HAS_TERM]->(t:GlossaryTerm)
        OPTIONAL MATCH (g)-[:HAS_METRIC]->(m:BusinessMetric)
        DETACH DELETE t, m, g
        """
        
        self.neo4j_client._execute_query(clear_query, {"tenant_id": tenant_id})
        
        # Create root glossary node
        root_query = """
        MATCH (d:Dataset {tenant_id: $tenant_id, name: $dataset_id})
        CREATE (g:NormalizedGlossary {
            tenant_id: $tenant_id,
            dataset_id: $dataset_id,
            created_at: datetime()
        })
        CREATE (d)-[:HAS_GLOSSARY]->(g)
        RETURN g
        """
        
        self.neo4j_client._execute_query(root_query, {
            "tenant_id": tenant_id,
            "dataset_id": dataset_id
        })
        
        # Create term nodes
        business_terms = response.get("business_terms", [])
        for term in business_terms:
            term_query = """
            MATCH (g:NormalizedGlossary {tenant_id: $tenant_id})
            
            // Create term node
            CREATE (t:GlossaryTerm {
                tenant_id: $tenant_id,
                name: $name,
                definition: $definition,
                related_terms: $related_terms,
                synonyms: $synonyms
            })
            
            // Connect to glossary
            CREATE (g)-[:HAS_TERM]->(t)
            
            // Connect to mapped tables
            WITH t
            UNWIND $table_mappings AS table_name
            MATCH (table:Table {tenant_id: $tenant_id, name: table_name})
            CREATE (t)-[:MAPS_TO]->(table)
            
            // Connect to mapped columns
            WITH t
            UNWIND $column_mappings AS mapping
            MATCH (column:Column {tenant_id: $tenant_id, table_name: mapping.table, name: mapping.column})
            CREATE (t)-[:MAPS_TO]->(column)
            
            RETURN t
            """
            
            # Extract mappings
            table_mappings = term.get("technical_mapping", {}).get("tables", [])
            column_mappings = term.get("technical_mapping", {}).get("columns", [])
            
            self.neo4j_client._execute_query(term_query, {
                "tenant_id": tenant_id,
                "name": term.get("name"),
                "definition": term.get("definition"),
                "related_terms": term.get("related_terms", []),
                "synonyms": term.get("synonyms", []),
                "table_mappings": table_mappings,
                "column_mappings": column_mappings
            })
            
            logger.info(f"Created term node: {term.get('name')}")
        
        # Create metric nodes
        business_metrics = response.get("business_metrics", [])
        for metric in business_metrics:
            metric_query = """
            MATCH (g:NormalizedGlossary {tenant_id: $tenant_id})
            
            // Create metric node
            CREATE (m:BusinessMetric {
                tenant_id: $tenant_id,
                name: $name,
                definition: $definition
            })
            
            // Connect to glossary
            CREATE (g)-[:HAS_METRIC]->(m)
            
            // Connect to derived tables
            WITH m
            UNWIND $derived_tables AS table_name
            MATCH (table:Table {tenant_id: $tenant_id, name: table_name})
            CREATE (m)-[:DERIVED_FROM]->(table)
            
            RETURN m
            """
            
            self.neo4j_client._execute_query(metric_query, {
                "tenant_id": tenant_id,
                "name": metric.get("name"),
                "definition": metric.get("definition"),
                "derived_tables": metric.get("derived_from", [])
            })
            
            logger.info(f"Created metric node: {metric.get('name')}")
        
        # Create term relationships
        self._create_term_relationships(tenant_id, business_terms)
        
        logger.info(f"Generated normalized business glossary for dataset {dataset_id}")
    
    def _create_term_relationships(self, tenant_id: str, terms: List[Dict[str, Any]]):
        """Create relationships between term nodes."""
        # Create relationships in Neo4j
        for term in terms:
            term_name = term.get("name")
            
            # Create related_term relationships
            for related_term in term.get("related_terms", []):
                related_query = """
                MATCH (t1:GlossaryTerm {tenant_id: $tenant_id, name: $term_name})
                MATCH (t2:GlossaryTerm {tenant_id: $tenant_id, name: $related_term})
                CREATE (t1)-[:RELATED_TO {type: "related_term"}]->(t2)
                """
                
                try:
                    self.neo4j_client._execute_query(related_query, {
                        "tenant_id": tenant_id,
                        "term_name": term_name,
                        "related_term": related_term
                    })
                except Exception as e:
                    logger.warning(f"Could not create related_term relationship: {term_name} -> {related_term}: {e}")
            
            # Create synonym relationships
            for synonym in term.get("synonyms", []):
                synonym_query = """
                MATCH (t:GlossaryTerm {tenant_id: $tenant_id, name: $term_name})
                MERGE (s:GlossaryTerm {tenant_id: $tenant_id, name: $synonym})
                ON CREATE SET s.definition = 'Synonym of ' + $term_name, s.tenant_id = $tenant_id
                CREATE (t)-[:RELATED_TO {type: "synonym"}]->(s)
                """
                
                try:
                    self.neo4j_client._execute_query(synonym_query, {
                        "tenant_id": tenant_id,
                        "term_name": term_name
                    })
                except Exception as e:
                    logger.warning(f"Could not create synonym relationship: {term_name} -> {synonym}: {e}")
        
        logger.info("Created term relationships")
    
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
        
        # Create prompt for domain analysis
        prompt = f"""
        Analyze the following database schema and determine the business domain it represents:
        
        {"\n".join(table_info)}
        
        Please provide:
        1. The primary business domain (e.g., retail, healthcare, finance, etc.)
        2. Key business entities represented in this data
        3. Potential business questions this data could answer
        4. Industry-specific terminology relevant to this domain
        5. Common business metrics and KPIs for this domain
        
        Format your response as a structured domain analysis.
        """
        
        # Generate domain analysis
        domain_analysis = await self.llm_client.generate(prompt)
        
        # Store domain analysis in Neo4j
        query = """
        MATCH (d:Dataset {tenant_id: $tenant_id, name: $dataset_id})
        SET d.domain_analysis = $domain_analysis,
            d.analysis_generated_at = datetime()
        RETURN d
        """
        
        params = {
            "tenant_id": tenant_id,
            "dataset_id": dataset_id,
            "domain_analysis": domain_analysis
        }
        
        self.neo4j_client._execute_query(query, params)
        logger.info(f"Generated business domain analysis for dataset {dataset_id}")
    
    async def _generate_concept_tags(self, tenant_id: str, tables: List[Dict[str, Any]]):
        """Generate concept tags for tables and columns"""
        for table in tables:
            table_name = table.get("name")
            if not table_name:
                continue
                
            # Get columns for context
            columns = self.neo4j_client.get_columns_for_table(tenant_id, table_name)
            
            # Create prompt for concept tagging
            prompt = f"""
            Analyze this database table and its columns to generate concept tags:
            
            Table: {table_name}
            Description: {table.get('description', 'No description')}
            
            Columns:
            {json.dumps([{
                "name": col.get('name') or col.get('column_name', ''),
                "data_type": col.get('data_type', 'unknown'),
                "description": col.get('description', 'No description')
            } for col in columns if col.get('name') or col.get('column_name')], indent=2)}
            
            For this table and its columns, provide:
            1. A list of business concept tags for the table itself (e.g., Transaction, Customer Profile, Product Catalog)
            2. For each column, assign concept tags that represent the semantic meaning (e.g., Personal Identifier, Timestamp, Amount, Status)
            
            Format your response as a structured JSON with table_tags and column_tags.
            """
            
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
                if table_tags:
                    query = """
                    MATCH (t:Table {tenant_id: $tenant_id, name: $table_name})
                    SET t.concept_tags = $concept_tags
                    RETURN t
                    """
                    
                    params = {
                        "tenant_id": tenant_id,
                        "table_name": table_name,
                        "concept_tags": table_tags
                    }
                    
                    self.neo4j_client._execute_query(query, params)
                
                # Store column tags in Neo4j
                for col_tag in response.get("column_tags", []):
                    col_name = col_tag.get("column_name")
                    tags = col_tag.get("tags", [])
                    
                    if col_name and tags:
                        query = """
                        MATCH (c:Column {tenant_id: $tenant_id, table_name: $table_name, name: $column_name})
                        SET c.concept_tags = $concept_tags
                        RETURN c
                        """
                        
                        params = {
                            "tenant_id": tenant_id,
                            "table_name": table_name,
                            "column_name": col_name,
                            "concept_tags": tags
                        }
                        
                        self.neo4j_client._execute_query(query, params)
                        
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
        
        # Create prompt
        table_names = [table.get("name") for table in tables if table.get("name")]
        prompt = f"""
        Generate 5 useful sample SQL queries for the following database schema:
        
        Tables: {", ".join(table_names)}
        
        Key relationships:
        {chr(10).join(relationship_info[:10])}
        
        For each query:
        1. Start with a clear comment explaining what business question the query answers
        2. Include joins between tables where appropriate
        3. Demonstrate filtering, aggregation, and sorting
        4. Make the queries progressively more complex to showcase different SQL features
        5. Include examples of common business analyses
        
        Format as SQL only, with comments explaining each query.
        """
        
        # Generate sample queries
        sample_queries = await self.llm_client.generate(prompt)
        
        # Store sample queries in Neo4j
        query = """
        MATCH (t:Tenant {id: $tenant_id})
        MERGE (sq:SampleQueries {tenant_id: $tenant_id})
        ON CREATE SET
            sq.content = $queries,
            sq.created_at = datetime()
        ON MATCH SET
            sq.content = $queries,
            sq.updated_at = datetime()
        
        WITH sq
        
        MERGE (t)-[:HAS_SAMPLE_QUERIES]->(sq)
        RETURN sq
        """
        
        params = {
            "tenant_id": tenant_id,
            "queries": sample_queries
        }
        
        self.neo4j_client._execute_query(query, params)
        logger.info(f"Generated sample queries for tenant {tenant_id}")
    
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
        5. The entity lifecycle represented by this table
        
        Keep the description business-focused, detailed yet concise (3-5 sentences long).
        Focus on explaining the business purpose rather than just listing the columns.
        """
    
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
        
        return f"""
        I need detailed descriptions for these columns in the table '{table_name}'.
        
        Columns needing descriptions:
        {columns_to_enhance_text}
        
        All columns in this table (for context):
        {all_columns_text}
        
        For each column that needs a description, provide:
        1. A clear description of what the column represents
        2. Its business purpose and how it's used
        3. Any data constraints or rules that might apply
        4. Potential relationships or joins to other tables
        
        Respond with structured data for each column, focusing on business meaning rather than technical details.
        """
    
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
        
        return f"""
        Analyze these two database tables and identify potential semantic relationships between them:
        
        TABLE 1: {table1_name}
        Columns:
        {table1_columns_text}
        
        TABLE 2: {table2_name}
        Columns:
        {table2_columns_text}
        
        Identify potential foreign key relationships between these tables based on:
        1. Column names and patterns
        2. Data types
        3. Semantic relationships implied by descriptions
        4. Business domain knowledge
        
        For each potential relationship, specify:
        - Source table and column
        - Target table and column
        - Relationship type (one-to-many, many-to-many, etc.)
        - Confidence level (0.0-1.0)
        - A short explanation for the relationship

        Only include relationships that make business sense with a confidence of at least 0.6.
        Focus on semantic relationships rather than just naming patterns.
        """
    
    def _record_workflow_completion(self, tenant_id: str, dataset_id: str):
        """Record successful workflow completion"""
        logger.info(f"Enhancement completed for {tenant_id}/{dataset_id}")
        
        query = """
        MATCH (d:Dataset {tenant_id: $tenant_id, name: $dataset_id})
        CREATE (ws:WorkflowStatus {
            tenant_id: $tenant_id,
            dataset_id: $dataset_id,
            workflow_name: "direct_schema_enhancement",
            status: "completed",
            created_at: datetime(),
            version: "1.0"
        })
        CREATE (d)-[:HAS_WORKFLOW_STATUS]->(ws)
        RETURN ws
        """
        
        params = {
            "tenant_id": tenant_id,
            "dataset_id": dataset_id
        }
        
        self.neo4j_client._execute_query(query, params)
    
    def _record_workflow_failure(self, tenant_id: str, dataset_id: str, error: str):
        """Record workflow failure"""
        logger.error(f"Enhancement failed for {tenant_id}/{dataset_id}: {error}")
        
        query = """
        MATCH (d:Dataset {tenant_id: $tenant_id, name: $dataset_id})
        CREATE (ws:WorkflowStatus {
            tenant_id: $tenant_id,
            dataset_id: $dataset_id,
            workflow_name: "direct_schema_enhancement",
            status: "failed",
            error: $error,
            created_at: datetime(),
            version: "1.0"
        })
        CREATE (d)-[:HAS_WORKFLOW_STATUS]->(ws)
        RETURN ws
        """
        
        params = {
            "tenant_id": tenant_id,
            "dataset_id": dataset_id,
            "error": error
        }
        
        self.neo4j_client._execute_query(query, params)


async def run_direct_enhancement(tenant_id: str, dataset_id: str = None):
    """Run enhanced schema workflow as a standalone process"""
    try:
        # Get Neo4j connection details
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Get LLM API details
        llm_api_key = os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY")
        llm_model = os.getenv("LLM_MODEL", "gpt-4o")
        
        if not llm_api_key:
            logger.error("LLM API key not configured")
            return False
        
        # Initialize clients
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        llm_client = LLMClient(api_key=llm_api_key, model=llm_model)
        
        # Initialize and run workflow
        workflow = DirectEnhancementWorkflow(neo4j_client, llm_client)
        
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
        logger.error(f"Error in direct schema enhancement: {e}", exc_info=True)
        return False