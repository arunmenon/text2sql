#!/usr/bin/env python3
"""
Text2SQL CLI - Main entry point for text2sql commands

Usage:
    ./cli.py schema load --tenant-id <tenant_id> [--schema-type <type>]
    ./cli.py schema relationships --tenant-id <tenant_id> [--use-llm]
    
    ./cli.py enhance run --tenant-id <tenant_id> [--direct]
    ./cli.py enhance glossary --tenant-id <tenant_id>
    ./cli.py enhance normalize --tenant-id <tenant_id>
    ./cli.py enhance concepts --tenant-id <tenant_id>
    
    ./cli.py check database --tenant-id <tenant_id>
    ./cli.py check graph --tenant-id <tenant_id>
    
    ./cli.py utils clear --tenant-id <tenant_id>
"""
import os
import sys
import argparse
import asyncio
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def schema_load(args):
    """Load schema into Neo4j database"""
    cmd = f"python3 scripts/schema/load_schema_demo.py --tenant-id {args.tenant_id}"
    if args.schema_type:
        cmd += f" --schema-type {args.schema_type}"
    os.system(cmd)

def schema_relationships(args):
    """Detect relationships between tables"""
    cmd = f"python3 scripts/relationships/simulate_relationships.py --tenant-id {args.tenant_id}"
    if args.use_llm:
        cmd += " --use-llm"
    os.system(cmd)

def enhance_run(args):
    """Run the metadata enhancement workflow"""
    command = f"python3 scripts/enhancement/run_direct_enhancement.py --tenant-id {args.tenant_id}"
    
    if args.direct:
        if args.use_enhanced_glossary:
            command += " --use-enhanced-glossary"
        elif args.use_legacy_glossary:
            command += " --use-legacy-glossary"
        os.system(command)
    else:
        os.system(f"python3 scripts/enhancement/run_enhancement.py --tenant-id {args.tenant_id}")

def enhance_glossary(args):
    """Generate business glossary"""
    os.system(f"python3 scripts/glossary/generate_glossary.py --tenant-id {args.tenant_id}")

def enhance_normalize(args):
    """Normalize business glossary into graph structure"""
    os.system(f"python3 scripts/glossary/normalize_glossary.py --tenant-id {args.tenant_id}")

def enhance_concepts(args):
    """Generate concept tags for tables and columns"""
    os.system(f"python3 scripts/glossary/generate_concept_tags.py --tenant-id {args.tenant_id}")

def check_database(args):
    """Check Neo4j database content"""
    os.system(f"python3 scripts/database/check_neo4j.py --tenant-id {args.tenant_id}")

def check_graph(args):
    """Check the graph structure in Neo4j"""
    os.system(f"python3 scripts/database/check_graph.py --tenant-id {args.tenant_id}")

def utils_clear(args):
    """Clear and reload the database"""
    os.system(f"python3 scripts/database/clear_and_reload.py --tenant-id {args.tenant_id}")

if __name__ == "__main__":
    # Create main parser
    parser = argparse.ArgumentParser(description="Text2SQL Command Line Interface")
    subparsers = parser.add_subparsers(dest='command', help='Command category')
    
    # Schema commands
    schema_parser = subparsers.add_parser('schema', help='Schema management commands')
    schema_subparsers = schema_parser.add_subparsers(dest='subcommand', help='Schema command')
    
    # Schema load
    schema_load_parser = schema_subparsers.add_parser('load', help='Load schema into Neo4j')
    schema_load_parser.add_argument('--tenant-id', required=True, help='Tenant ID')
    schema_load_parser.add_argument('--schema-type', choices=['merchandising', 'financial', 'hr'], 
                               help='Type of schema to load')
    schema_load_parser.set_defaults(func=schema_load)
    
    # Schema relationships
    schema_rel_parser = schema_subparsers.add_parser('relationships', help='Detect relationships')
    schema_rel_parser.add_argument('--tenant-id', required=True, help='Tenant ID')
    schema_rel_parser.add_argument('--use-llm', action='store_true', help='Use LLM for relationship detection')
    schema_rel_parser.set_defaults(func=schema_relationships)
    
    # Enhancement commands
    enhance_parser = subparsers.add_parser('enhance', help='Metadata enhancement commands')
    enhance_subparsers = enhance_parser.add_subparsers(dest='subcommand', help='Enhancement command')
    
    # Enhance run
    enhance_run_parser = enhance_subparsers.add_parser('run', help='Run enhancement workflow')
    enhance_run_parser.add_argument('--tenant-id', required=True, help='Tenant ID')
    enhance_run_parser.add_argument('--direct', action='store_true', 
                               help='Use direct enhancement with graph structure')
    enhance_run_parser.add_argument('--use-enhanced-glossary', action='store_true',
                               help='Use enhanced business glossary with multi-agent approach')
    enhance_run_parser.add_argument('--use-legacy-glossary', action='store_true',
                               help='Use legacy business glossary approach')
    enhance_run_parser.set_defaults(func=enhance_run)
    
    # Enhance glossary
    enhance_glossary_parser = enhance_subparsers.add_parser('glossary', help='Generate business glossary')
    enhance_glossary_parser.add_argument('--tenant-id', required=True, help='Tenant ID')
    enhance_glossary_parser.set_defaults(func=enhance_glossary)
    
    # Enhance normalize
    enhance_normalize_parser = enhance_subparsers.add_parser('normalize', help='Normalize glossary')
    enhance_normalize_parser.add_argument('--tenant-id', required=True, help='Tenant ID')
    enhance_normalize_parser.set_defaults(func=enhance_normalize)
    
    # Enhance concepts
    enhance_concepts_parser = enhance_subparsers.add_parser('concepts', help='Generate concept tags')
    enhance_concepts_parser.add_argument('--tenant-id', required=True, help='Tenant ID')
    enhance_concepts_parser.set_defaults(func=enhance_concepts)
    
    # Check commands
    check_parser = subparsers.add_parser('check', help='Database checking commands')
    check_subparsers = check_parser.add_subparsers(dest='subcommand', help='Check command')
    
    # Check database
    check_db_parser = check_subparsers.add_parser('database', help='Check Neo4j database content')
    check_db_parser.add_argument('--tenant-id', required=True, help='Tenant ID')
    check_db_parser.set_defaults(func=check_database)
    
    # Check graph
    check_graph_parser = check_subparsers.add_parser('graph', help='Check graph structure')
    check_graph_parser.add_argument('--tenant-id', required=True, help='Tenant ID')
    check_graph_parser.set_defaults(func=check_graph)
    
    # Utils commands
    utils_parser = subparsers.add_parser('utils', help='Utility commands')
    utils_subparsers = utils_parser.add_subparsers(dest='subcommand', help='Utility command')
    
    # Utils clear
    utils_clear_parser = utils_subparsers.add_parser('clear', help='Clear and reload database')
    utils_clear_parser.add_argument('--tenant-id', required=True, help='Tenant ID')
    utils_clear_parser.set_defaults(func=utils_clear)
    
    # Parse arguments
    args = parser.parse_args()
    
    # Execute function
    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()