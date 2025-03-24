#!/usr/bin/env python3
"""
Semantic Graph Builder - Main Orchestration Script

This script provides a single entry point for building a complete semantic graph:
1. Clears the Neo4j database
2. Loads a schema from SQL file or simulation
3. Infers relationships between tables (pattern-based, statistical, and optionally LLM)
4. Generates enhanced table and column descriptions
5. Creates a business glossary with domain-specific terminology
6. Verifies the semantic graph structure

Usage:
    python build_semantic_graph.py --tenant-id YOUR_TENANT_ID --schema-file schemas/walmart_facilities_complete.sql --csv-dir data/datasets

Author: Semantic Graph Builder Team
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

# Import core components from the semantic_graph_builder module
from src.graph_storage.neo4j_client import Neo4jClient
from src.llm.client import LLMClient

# Import schema_extraction components
from src.semantic_graph_builder.schema_extraction.simulation.schema_loader import SQLSchemaLoader

# Import relationship_discovery components
from src.semantic_graph_builder.relationship_discovery.name_pattern.pattern_matcher import PatternMatcher
from src.semantic_graph_builder.relationship_discovery.statistical.csv_overlap_analyzer import CSVOverlapAnalyzer
from src.semantic_graph_builder.relationship_discovery.llm_inference.relationship_analyzer import LLMRelationshipAnalyzer

# Import table_context_gen components
from src.semantic_graph_builder.table_context_gen.workflows.direct_enhancement import DirectEnhancementWorkflow

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class SemanticGraphBuilder:
    """Main orchestrator for building a semantic graph."""
    
    def __init__(
        self,
        tenant_id: str,
        schema_file: Optional[str] = None,
        schema_type: Optional[str] = None,
        csv_dir: Optional[str] = None,
        use_llm: bool = True,
        min_confidence: float = 0.6,
        use_enhanced_glossary: bool = True,
        skip_schema_load: bool = False,
        skip_relationship_inference: bool = False,
        only_enhance_metadata: bool = False,
        only_generate_glossary: bool = False
    ):
        """
        Initialize the semantic graph builder.
        
        Args:
            tenant_id: Unique identifier for the tenant/project
            schema_file: Path to SQL schema file (if not using simulation)
            schema_type: Type of simulated schema if not using schema_file ('walmart', 'supply-chain', 'merchandising')
            csv_dir: Directory containing CSV files for relationship inference
            use_llm: Whether to use LLM for relationship inference and metadata enhancement
            min_confidence: Minimum confidence threshold for relationship inference (0.0-1.0)
            use_enhanced_glossary: Whether to use the enhanced multi-agent business glossary generator
            skip_schema_load: Skip loading the schema (use existing data)
            skip_relationship_inference: Skip relationship inference
            only_enhance_metadata: Run only the metadata enhancement step
            only_generate_glossary: Run only the business glossary generation step
        """
        # Store configuration
        self.tenant_id = tenant_id
        self.schema_file = schema_file
        self.schema_type = schema_type
        self.csv_dir = csv_dir
        self.use_llm = use_llm
        self.min_confidence = min_confidence
        self.use_enhanced_glossary = use_enhanced_glossary
        
        # Step control flags
        self.skip_schema_load = skip_schema_load
        self.skip_relationship_inference = skip_relationship_inference
        self.only_enhance_metadata = only_enhance_metadata
        self.only_generate_glossary = only_generate_glossary
        
        # Get Neo4j connection details
        self.neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        self.neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        self.neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Get LLM API details
        self.llm_api_key = os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY")
        self.llm_model = os.getenv("LLM_MODEL", "gpt-4o")
        
        # Initialize clients
        self.neo4j_client = None
        self.llm_client = None
        
        # Validate configuration
        self._validate_config()
        
    def _validate_config(self):
        """Validate the configuration settings."""
        # For metadata enhancement or glossary generation modes, we don't need schema sources
        if self.only_enhance_metadata or self.only_generate_glossary or self.skip_schema_load:
            pass  # Skip schema validation
        else:
            # Check that at least one schema source is specified
            if not self.schema_file and not self.schema_type:
                raise ValueError("Either schema_file or schema_type must be specified unless using --only-enhance-metadata, --only-generate-glossary, or --skip-schema-load")
                
            # Check that schema_file exists if specified
            if self.schema_file and not os.path.exists(self.schema_file):
                raise FileNotFoundError(f"Schema file not found: {self.schema_file}")
                
            # Check that schema_type is valid if specified
            if self.schema_type and self.schema_type not in ["walmart", "supply-chain", "merchandising"]:
                raise ValueError(f"Invalid schema_type: {self.schema_type}. Must be one of: walmart, supply-chain, merchandising")
        
        # Check that csv_dir exists if specified and not skipping relationship inference
        if self.csv_dir and not os.path.exists(self.csv_dir) and not self.skip_relationship_inference and not self.only_enhance_metadata and not self.only_generate_glossary:
            raise FileNotFoundError(f"CSV directory not found: {self.csv_dir}")
            
        # Check for LLM API key if LLM is enabled
        if self.use_llm and not self.llm_api_key:
            raise ValueError("LLM API key not found in environment variables")
            
        # The special modes require LLM
        if (self.only_enhance_metadata or self.only_generate_glossary) and not self.use_llm:
            raise ValueError("--use-llm must be specified when using --only-enhance-metadata or --only-generate-glossary")
            
    async def initialize_clients(self, timeout_seconds=300):
        """Initialize Neo4j and LLM clients."""
        logger.info("Initializing clients...")
        
        # Initialize Neo4j client
        self.neo4j_client = Neo4jClient(self.neo4j_uri, self.neo4j_user, self.neo4j_password)
        
        # Initialize LLM client if needed
        if self.use_llm:
            if not self.llm_api_key:
                logger.error("LLM API key not found but --use-llm was specified")
                raise ValueError("LLM API key is required when using --use-llm flag")
                
            # Convert seconds to milliseconds for httpx timeout
            timeout_ms = timeout_seconds * 1000
            
            logger.info(f"Initializing LLM client with model {self.llm_model} and {timeout_seconds}s timeout")
            self.llm_client = LLMClient(
                api_key=self.llm_api_key, 
                model=self.llm_model,
                timeout_ms=timeout_ms
            )
        else:
            # Explicitly set to None when not using LLM
            self.llm_client = None
            
        logger.info("Clients initialized successfully")
            
    async def close_clients(self):
        """Close clients to clean up resources."""
        if self.neo4j_client:
            self.neo4j_client.close()
            
        # Only try to close LLM client if it was initialized
        if self.use_llm and self.llm_client:
            await self.llm_client.close()
    
    async def clear_database(self):
        """Clear all data from Neo4j database."""
        logger.info("Clearing Neo4j database...")
        
        try:
            # Create schema constraints
            self.neo4j_client.create_schema_constraints()
            
            # Delete all nodes and relationships
            self.neo4j_client._execute_query("MATCH (n) DETACH DELETE n")
            
            logger.info("Database cleared successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing database: {e}")
            return False
    
    async def load_schema(self):
        """Load schema into Neo4j."""
        logger.info(f"Loading schema for tenant {self.tenant_id}...")
        
        try:
            # Create tenant if not exists
            description = f"{self.schema_type or 'Custom'} schema tenant"
            self.neo4j_client.create_tenant(self.tenant_id, self.tenant_id, description)
            
            # Load schema
            if self.schema_file:
                # Use schema file
                logger.info(f"Loading schema from file: {self.schema_file}")
                loader = SQLSchemaLoader(self.schema_file)
                tables = loader.load_schema()
                
                # Create dataset
                dataset_id = Path(self.schema_file).stem
                self.neo4j_client.create_dataset(self.tenant_id, dataset_id, f"Dataset from {self.schema_file}")
                
            else:
                # Use simulated schema
                logger.info(f"Loading simulated {self.schema_type} schema")
                
                # Create dataset
                dataset_id = f"{self.schema_type}_dataset"
                self.neo4j_client.create_dataset(self.tenant_id, dataset_id, f"{self.schema_type} dataset")
                
                # Get tables from simulation module
                from src.semantic_graph_builder.schema_extraction.simulation.schema_loader import SchemaSimulator
                simulator = SchemaSimulator(self.tenant_id, None, self.schema_type)
                schema = simulator.generate_schema()
                tables = schema.get("tables", [])
                
            # Store tables and columns in Neo4j
            logger.info(f"Storing {len(tables)} tables in Neo4j")
            
            # Get dataset
            datasets = self.neo4j_client.get_datasets_for_tenant(self.tenant_id)
            if not datasets:
                raise ValueError(f"No datasets found for tenant {self.tenant_id}")
                
            dataset_id = datasets[0]["name"]
            
            # Create tables and columns
            table_count = 0
            column_count = 0
            
            for table in tables:
                table_name = table.get("table_name")
                if not table_name:
                    continue
                    
                # Create table
                self.neo4j_client.create_table(self.tenant_id, dataset_id, table)
                table_count += 1
                
                # Create columns
                for column in table.get("columns", []):
                    self.neo4j_client.create_column(
                        self.tenant_id,
                        dataset_id,
                        table_name,
                        column
                    )
                    column_count += 1
            
            logger.info(f"Schema loaded successfully. Added {table_count} tables and {column_count} columns.")
            return True
            
        except Exception as e:
            logger.error(f"Error loading schema: {e}")
            return False
    
    async def infer_relationships(self):
        """Infer relationships between tables."""
        logger.info(f"Inferring relationships for tenant {self.tenant_id}...")
        
        try:
            # Get tables from Neo4j
            tables = self.neo4j_client.get_tables_for_tenant(self.tenant_id)
            
            # Prepare table data with columns
            table_data = []
            for table in tables:
                table_name = table["name"]
                columns = self.neo4j_client.get_columns_for_table(self.tenant_id, table_name)
                
                table_data.append({
                    "table_name": table_name,
                    "description": table.get("description", ""),
                    "columns": columns
                })
            
            # Infer relationships using name patterns
            logger.info("Inferring relationships using name patterns")
            pattern_matcher = PatternMatcher()
            pattern_relationships = pattern_matcher.infer_relationships(table_data)
            
            # Store pattern-based relationships
            pattern_count = 0
            for rel in pattern_relationships:
                if rel["confidence"] >= self.min_confidence:
                    self.neo4j_client.create_relationship(
                        self.tenant_id,
                        rel["source_table"],
                        rel["source_column"],
                        rel["target_table"],
                        rel["target_column"],
                        rel["confidence"],
                        rel["detection_method"]
                    )
                    pattern_count += 1
            
            logger.info(f"Created {pattern_count} pattern-based relationships")
            
            # Infer relationships using statistical analysis on CSV files
            if self.csv_dir:
                logger.info(f"Inferring relationships using statistical analysis on CSV files in {self.csv_dir}")
                csv_analyzer = CSVOverlapAnalyzer(self.csv_dir)
                csv_relationships = await csv_analyzer.find_candidate_relationships(
                    table_data,
                    self.min_confidence
                )
                
                # Store statistical relationships
                csv_count = 0
                for rel in csv_relationships:
                    self.neo4j_client.create_relationship(
                        self.tenant_id,
                        rel["source_table"],
                        rel["source_column"],
                        rel["target_table"],
                        rel["target_column"],
                        rel["confidence"],
                        rel["detection_method"]
                    )
                    csv_count += 1
                
                logger.info(f"Created {csv_count} statistical relationships from CSV data")
            
            # LLM-based relationship inference (if enabled)
            llm_count = 0
            if self.use_llm:
                logger.info("Inferring relationships using LLM")
                
                # Initialize LLM relationship analyzer
                llm_analyzer = LLMRelationshipAnalyzer(self.llm_client, self.csv_dir)
                
                # Infer relationships
                llm_relationships = await llm_analyzer.infer_relationships(table_data, self.min_confidence)
                
                # Store LLM-based relationships
                for rel in llm_relationships:
                    self.neo4j_client.create_relationship(
                        self.tenant_id,
                        rel["source_table"],
                        rel["source_column"],
                        rel["target_table"],
                        rel["target_column"],
                        rel["confidence"],
                        rel["detection_method"],
                        relationship_type=rel.get("relationship_type"),
                        metadata={"explanation": rel.get("explanation", "")}
                    )
                    llm_count += 1
                
                logger.info(f"Added {llm_count} LLM-inferred relationships")
            
            # Get summary
            summary = self.neo4j_client.get_schema_summary(self.tenant_id)
            logger.info(f"Schema summary: {summary}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error inferring relationships: {e}")
            return False
    
    async def enhance_metadata(self):
        """Enhance metadata with descriptions and business glossary."""
        logger.info(f"Enhancing metadata for tenant {self.tenant_id}...")
        
        try:
            # Check if LLM is enabled - if not, we can't enhance metadata
            if not self.use_llm or not self.llm_client:
                logger.warning("Skipping metadata enhancement because --use-llm flag was not specified")
                return True  # Return true to allow the script to continue
            
            # Get dataset_id from Neo4j
            datasets = self.neo4j_client.get_datasets_for_tenant(self.tenant_id)
            if not datasets:
                logger.error(f"No datasets found for tenant {self.tenant_id}")
                return False
            
            dataset_id = datasets[0]["name"]
            logger.info(f"Found dataset: {dataset_id}")
            
            # Create and run the DirectEnhancementWorkflow
            workflow = DirectEnhancementWorkflow(
                self.neo4j_client, 
                self.llm_client, 
                use_enhanced_glossary=self.use_enhanced_glossary
            )
            success = await workflow.run(self.tenant_id, dataset_id)
            
            if success:
                logger.info("Metadata enhancement completed successfully")
            else:
                logger.error("Metadata enhancement failed")
            
            return success
            
        except Exception as e:
            logger.error(f"Error enhancing metadata: {e}")
            return False
    
    async def verify_graph(self):
        """Verify the semantic graph structure."""
        logger.info(f"Verifying graph structure for tenant {self.tenant_id}...")
        
        try:
            # Get table count directly from Neo4j
            tables_query = """
            MATCH (t:Table {tenant_id: $tenant_id})
            RETURN count(t) as tables_count
            """
            tables_result = self.neo4j_client._execute_query(tables_query, {"tenant_id": self.tenant_id})
            tables_count = tables_result[0]["tables_count"] if tables_result else 0
            
            if tables_count == 0:
                logger.warning("No tables found in graph")
                return False
            
            logger.info(f"Found {tables_count} tables")
            
            # Get column count
            columns_query = """
            MATCH (c:Column {tenant_id: $tenant_id})
            RETURN count(c) as columns_count
            """
            columns_result = self.neo4j_client._execute_query(columns_query, {"tenant_id": self.tenant_id})
            columns_count = columns_result[0]["columns_count"] if columns_result else 0
            
            if columns_count == 0:
                logger.warning("No columns found in graph")
                return False
            
            logger.info(f"Found {columns_count} columns")
            
            # Get relationship count
            rels_query = """
            MATCH (c1:Column {tenant_id: $tenant_id})-[r:LIKELY_REFERENCES]->(c2:Column {tenant_id: $tenant_id})
            RETURN count(r) as rels_count
            """
            rels_result = self.neo4j_client._execute_query(rels_query, {"tenant_id": self.tenant_id})
            relationships_count = rels_result[0]["rels_count"] if rels_result else 0
            
            logger.info(f"Found {relationships_count} relationships")
            
            # Check enhanced descriptions
            query = """
            MATCH (t:Table {tenant_id: $tenant_id})
            WHERE t.description_enhanced = true
            RETURN count(t) as enhanced_tables
            """
            
            result = self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
            enhanced_tables = result[0]["enhanced_tables"] if result else 0
            
            logger.info(f"Found {enhanced_tables} tables with enhanced descriptions")
            
            # Check business glossary
            query = """
            MATCH (g:BusinessGlossary {tenant_id: $tenant_id})
            OPTIONAL MATCH (g)-[:HAS_TERM]->(t:GlossaryTerm)
            RETURN count(t) as term_count
            """
            
            result = self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
            term_count = result[0]["term_count"] if result else 0
            
            logger.info(f"Found {term_count} business glossary terms")
            
            return True
            
        except Exception as e:
            logger.error(f"Error verifying graph: {e}")
            return False
    
    async def build(self, timeout_seconds=300):
        """Build the complete semantic graph based on selected mode."""
        try:
            # Always initialize clients
            await self.initialize_clients(timeout_seconds)
            
            # Handle specific modes
            if self.only_enhance_metadata:
                logger.info("Running metadata enhancement only")
                # Jump directly to enhance metadata
                if not await self.enhance_metadata():
                    logger.error("Failed to enhance metadata.")
                    return False
                
            elif self.only_generate_glossary:
                logger.info("Running business glossary generation only")
                # Generate glossary - this is included in enhance_metadata
                # Get dataset
                datasets = self.neo4j_client.get_datasets_for_tenant(self.tenant_id)
                if not datasets:
                    logger.error(f"No datasets found for tenant {self.tenant_id}")
                    return False
                
                dataset_id = datasets[0]["name"]
                tables = self.neo4j_client.get_tables_for_tenant(self.tenant_id)
                
                # Create workflow and run glossary generation
                from src.semantic_graph_builder.table_context_gen.workflows.direct_enhancement import DirectEnhancementWorkflow
                workflow = DirectEnhancementWorkflow(self.neo4j_client, self.llm_client, self.use_enhanced_glossary)
                await workflow._generate_normalized_business_glossary(self.tenant_id, dataset_id, tables)
                
            else:
                # Run normal flow with potential step skipping
                
                # Step 2: Clear database (unless we're just enhancing existing data)
                if not self.skip_schema_load and not self.skip_relationship_inference:
                    if not await self.clear_database():
                        logger.error("Failed to clear database. Aborting.")
                        return False
                
                # Step 3: Load schema
                if not self.skip_schema_load:
                    if not await self.load_schema():
                        logger.error("Failed to load schema. Aborting.")
                        return False
                
                # Step 4: Infer relationships
                if not self.skip_relationship_inference:
                    if not await self.infer_relationships():
                        logger.error("Failed to infer relationships. Aborting.")
                        return False
                
                # Step 5: Enhance metadata (only if using LLM)
                if self.use_llm:
                    if not await self.enhance_metadata():
                        logger.error("Failed to enhance metadata. Continuing anyway.")
            
            # Always verify the graph at the end
            if not await self.verify_graph():
                logger.warning("Graph verification found issues. Review the logs.")
            
            logger.info("Semantic graph operation completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Error building semantic graph: {e}")
            return False
            
        finally:
            # Always close clients
            await self.close_clients()


# Command-line interface
async def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Build a semantic graph from schema and data")
    
    # Required arguments
    parser.add_argument("--tenant-id", required=True, help="Tenant ID for the semantic graph")
    
    # Schema source (required for full flow, schema load, or relationship mode)
    schema_group = parser.add_mutually_exclusive_group()
    schema_group.add_argument("--schema-file", help="SQL schema file path")
    schema_group.add_argument("--schema-type", choices=["walmart", "supply-chain", "merchandising"],
                             help="Simulated schema type")
    
    # Optional arguments
    parser.add_argument("--csv-dir", help="Directory containing CSV files for relationship inference")
    parser.add_argument("--use-llm", action="store_true", help="Enable LLM-based relationship inference and enhancement")
    parser.add_argument("--min-confidence", type=float, default=0.6, 
                       help="Minimum confidence threshold for relationships (0.0-1.0)")
    parser.add_argument("--use-enhanced-glossary", action="store_true", 
                       help="Use enhanced multi-agent business glossary generator")
    parser.add_argument("--timeout", type=int, default=300,
                       help="Timeout in seconds for LLM operations (default: 300 seconds)")
    
    # Step control arguments (for selective execution)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--skip-schema-load", action="store_true", 
                      help="Skip loading the schema (use existing data)")
    group.add_argument("--skip-relationship-inference", action="store_true", 
                      help="Skip relationship inference")
    group.add_argument("--only-enhance-metadata", action="store_true", 
                      help="Run only the metadata enhancement step (requires --use-llm)")
    group.add_argument("--only-generate-glossary", action="store_true", 
                      help="Run only the business glossary generation step (requires --use-llm)")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Validate arguments for selective modes
    if (args.only_enhance_metadata or args.only_generate_glossary) and not args.use_llm:
        parser.error("--use-llm must be specified when using --only-enhance-metadata or --only-generate-glossary")
        
    if args.only_enhance_metadata or args.only_generate_glossary or args.skip_schema_load:
        # Schema file not required for these modes
        pass
    elif not args.schema_file and not args.schema_type:
        parser.error("Either --schema-file or --schema-type must be specified unless using selective modes")
    
    # Create and run builder
    builder = SemanticGraphBuilder(
        tenant_id=args.tenant_id,
        schema_file=args.schema_file,
        schema_type=args.schema_type,
        csv_dir=args.csv_dir,
        use_llm=args.use_llm,
        min_confidence=args.min_confidence,
        use_enhanced_glossary=args.use_enhanced_glossary,
        skip_schema_load=args.skip_schema_load,
        skip_relationship_inference=args.skip_relationship_inference,
        only_enhance_metadata=args.only_enhance_metadata,
        only_generate_glossary=args.only_generate_glossary
    )
    
    success = await builder.build(args.timeout)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())