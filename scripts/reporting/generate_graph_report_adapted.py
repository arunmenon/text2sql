#!/usr/bin/env python3
"""
Semantic Graph Report Generator - Adapted for Production Schema

This script generates a comprehensive report on the semantic graph structure
for a given tenant, adapted to match the actual schema in the production database.

Usage:
    python generate_graph_report_adapted.py --tenant walmart-stores [--output report.md]
"""

import os
import sys
import asyncio
import argparse
import logging
import json
from datetime import datetime
from dotenv import load_dotenv
from tabulate import tabulate
from collections import Counter, defaultdict

from src.graph_storage.neo4j_client import Neo4jClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class AdaptedGraphReportGenerator:
    """Generates comprehensive reports on semantic graph structure, adapted to the production schema."""
    
    def __init__(self, neo4j_client, tenant_id):
        """Initialize the report generator."""
        self.neo4j_client = neo4j_client
        self.tenant_id = tenant_id
        
    def get_tenant_info(self):
        """Get basic information about the tenant."""
        query = """
        MATCH (t:Tenant {id: $tenant_id})
        RETURN t.id as id, t.name as name, t.description as description
        """
        
        result = self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
        if not result:
            return {"id": self.tenant_id, "name": "Unknown", "description": "Unknown"}
        
        return result[0]
        
    def get_table_stats(self):
        """Get statistics about tables for this tenant."""
        query = """
        MATCH (t:Table {tenant_id: $tenant_id})
        RETURN count(t) as table_count
        """
        
        result = self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
        if not result:
            return {"table_count": 0}
        
        return result[0]
        
    def get_column_stats(self):
        """Get statistics about columns for this tenant."""
        query = """
        MATCH (t:Table {tenant_id: $tenant_id})-[:HAS_COLUMN]->(c:Column)
        RETURN count(c) as column_count
        """
        
        result = self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
        if not result:
            return {"column_count": 0}
        
        return result[0]
        
    def get_relationship_stats(self):
        """Get statistics about relationships for this tenant."""
        query = """
        MATCH (s:Column {tenant_id: $tenant_id})-[r:LIKELY_REFERENCES]->(t:Column {tenant_id: $tenant_id})
        RETURN count(r) as relationship_count,
               avg(r.confidence) as avg_confidence
        """
        
        result = self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
        if not result:
            return {"relationship_count": 0, "avg_confidence": 0}
        
        return result[0]
        
    def get_tables_and_columns(self):
        """Get all tables and their columns."""
        query = """
        MATCH (t:Table {tenant_id: $tenant_id})
        OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
        WITH t, collect({name: c.name, data_type: c.data_type, description: c.description}) as columns, count(c) as column_count
        RETURN t.name as table_name, t.description as table_description,
               columns, column_count
        ORDER BY t.name
        """
        
        return self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
        
    def get_relationships(self):
        """Get relationships between tables."""
        query = """
        MATCH (s:Table {tenant_id: $tenant_id})-[:HAS_COLUMN]->(sc:Column)-[r:LIKELY_REFERENCES]->(tc:Column)<-[:HAS_COLUMN]-(t:Table {tenant_id: $tenant_id})
        WHERE s <> t
        RETURN s.name as source_table, sc.name as source_column,
               t.name as target_table, tc.name as target_column,
               r.confidence as confidence, r.relationship_type as relationship_type,
               r.detection_method as detection_method
        ORDER BY s.name, t.name
        """
        
        return self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
        
    def get_glossary_stats(self):
        """Get statistics about glossary entities."""
        queries = {
            "business_terms": """
                MATCH (g:BusinessGlossary {tenant_id: $tenant_id})-[:HAS_TERM]->(t:GlossaryTerm)
                RETURN count(t) as count
            """,
            "business_metrics": """
                MATCH (g:BusinessGlossary {tenant_id: $tenant_id})-[:HAS_METRIC]->(m:BusinessMetric)
                RETURN count(m) as count
            """,
            "composite_concepts": """
                MATCH (g:BusinessGlossary {tenant_id: $tenant_id})-[:HAS_COMPOSITE_CONCEPT]->(cc:CompositeConcept)
                RETURN count(cc) as count
            """,
            "business_processes": """
                MATCH (g:BusinessGlossary {tenant_id: $tenant_id})-[:HAS_BUSINESS_PROCESS]->(bp:BusinessProcess)
                RETURN count(bp) as count
            """,
            "relationship_concepts": """
                MATCH (g:BusinessGlossary {tenant_id: $tenant_id})-[:HAS_RELATIONSHIP_CONCEPT]->(rc:RelationshipConcept)
                RETURN count(rc) as count
            """,
            "hierarchical_concepts": """
                MATCH (g:BusinessGlossary {tenant_id: $tenant_id})-[:HAS_HIERARCHICAL_CONCEPT]->(hc:HierarchicalConcept)
                RETURN count(hc) as count
            """
        }
        
        results = {}
        for entity_type, query in queries.items():
            result = self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
            results[entity_type] = result[0]["count"] if result else 0
        
        return results
        
    def get_business_terms(self):
        """Get all business terms."""
        query = """
        MATCH (g:BusinessGlossary {tenant_id: $tenant_id})-[:HAS_TERM]->(t:GlossaryTerm)
        OPTIONAL MATCH (t)-[:MAPS_TO]->(tab:Table)
        OPTIONAL MATCH (t)-[:MAPS_TO]->(col:Column)
        RETURN t.name as name, t.definition as definition,
               collect(DISTINCT tab.name) as mapped_tables,
               collect(DISTINCT col.name) as mapped_columns
        ORDER BY t.name
        """
        
        return self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
        
    def get_business_metrics(self):
        """Get all business metrics."""
        query = """
        MATCH (g:BusinessGlossary {tenant_id: $tenant_id})-[:HAS_METRIC]->(m:BusinessMetric)
        RETURN m.name as name, m.definition as definition
        ORDER BY m.name
        """
        
        return self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
        
    def get_composite_concepts(self):
        """Get all composite concepts."""
        query = """
        MATCH (g:BusinessGlossary {tenant_id: $tenant_id})-[:HAS_COMPOSITE_CONCEPT]->(cc:CompositeConcept)
        OPTIONAL MATCH (cc)-[:COMPRISES]->(t:Table)
        RETURN cc.name as name, cc.definition as definition, 
               cc.business_importance as business_importance,
               collect(DISTINCT t.name) as tables
        ORDER BY cc.name
        """
        
        return self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
        
    def get_business_processes(self):
        """Get all business processes."""
        query = """
        MATCH (g:BusinessGlossary {tenant_id: $tenant_id})-[:HAS_BUSINESS_PROCESS]->(bp:BusinessProcess)
        OPTIONAL MATCH (bp)-[:INVOLVES]->(t:Table)
        RETURN bp.name as name, bp.definition as definition,
               collect(DISTINCT t.name) as tables
        ORDER BY bp.name
        """
        
        return self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
        
    def get_relationship_concepts(self):
        """Get all relationship concepts."""
        query = """
        MATCH (g:BusinessGlossary {tenant_id: $tenant_id})-[:HAS_RELATIONSHIP_CONCEPT]->(rc:RelationshipConcept)
        OPTIONAL MATCH (rc)-[:RELATES_TO]->(t:Table)
        RETURN rc.name as name, rc.definition as definition,
               collect(DISTINCT t.name) as tables
        ORDER BY rc.name
        """
        
        return self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
        
    def get_hierarchical_concepts(self):
        """Get all hierarchical concepts."""
        query = """
        MATCH (g:BusinessGlossary {tenant_id: $tenant_id})-[:HAS_HIERARCHICAL_CONCEPT]->(hc:HierarchicalConcept)
        OPTIONAL MATCH (hc)-[:PRIMARY_TABLE]->(t:Table)
        RETURN hc.name as name, hc.definition as definition,
               collect(DISTINCT t.name) as tables
        ORDER BY hc.name
        """
        
        return self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
        
    def get_term_relationships(self):
        """Get relationships between business terms."""
        query = """
        MATCH (t1:GlossaryTerm {tenant_id: $tenant_id})-[r:RELATED_TO]->(t2:GlossaryTerm {tenant_id: $tenant_id})
        RETURN t1.name as source_term, t2.name as target_term
        ORDER BY t1.name, t2.name
        """
        
        return self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
        
    def generate_report(self):
        """Generate a comprehensive report on the semantic graph."""
        tenant_info = self.get_tenant_info()
        table_stats = self.get_table_stats()
        column_stats = self.get_column_stats()
        relationship_stats = self.get_relationship_stats()
        
        tables_and_columns = self.get_tables_and_columns()
        relationships = self.get_relationships()
        
        glossary_stats = self.get_glossary_stats()
        business_terms = self.get_business_terms()
        business_metrics = self.get_business_metrics()
        composite_concepts = self.get_composite_concepts()
        business_processes = self.get_business_processes()
        relationship_concepts = self.get_relationship_concepts()
        hierarchical_concepts = self.get_hierarchical_concepts()
        term_relationships = self.get_term_relationships()
        
        # Create the report
        report = []
        
        # Title
        report.append(f"# Semantic Graph Report for {tenant_info['name']} ({self.tenant_id})")
        report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # Executive Summary
        report.append("## Executive Summary")
        report.append(f"{tenant_info.get('description', 'No description available')}\n")
        
        report.append("### Graph Statistics")
        report.append("| Metric | Count |")
        report.append("| --- | --- |")
        report.append(f"| Tables | {table_stats.get('table_count', 0)} |")
        report.append(f"| Columns | {column_stats.get('column_count', 0)} |")
        report.append(f"| Relationships | {relationship_stats.get('relationship_count', 0)} |")
        report.append(f"| Business Terms | {glossary_stats.get('business_terms', 0)} |")
        report.append(f"| Business Metrics | {glossary_stats.get('business_metrics', 0)} |")
        report.append(f"| Composite Concepts | {glossary_stats.get('composite_concepts', 0)} |")
        report.append(f"| Business Processes | {glossary_stats.get('business_processes', 0)} |")
        report.append(f"| Relationship Concepts | {glossary_stats.get('relationship_concepts', 0)} |")
        report.append(f"| Hierarchical Concepts | {glossary_stats.get('hierarchical_concepts', 0)} |")
        report.append("\n")
        
        # Value Proposition
        report.append("## Value Proposition of the Semantic Graph")
        report.append(
            "The semantic graph represents a comprehensive knowledge model of your business data, "
            "connecting technical metadata with business concepts and relationships. This provides "
            "significant value in several key areas:\n"
        )
        
        report.append("### Data Discovery and Understanding")
        report.append(
            "- **Business Context**: Rich business descriptions of tables and columns make data discoverable using business terminology\n"
            "- **Cross-Table Relationships**: Automatically detected relationships show how data connects across systems\n"
            "- **Graph Navigation**: Intuitive exploration of data assets and their relationships\n"
        )
        
        report.append("### Enhanced Business Intelligence")
        report.append(
            "- **Business Metrics**: Pre-defined business KPIs with clear definitions and calculation methods\n"
            "- **Composite Concepts**: Complex business entities spanning multiple tables are explicitly modeled\n"
            "- **Business Processes**: End-to-end workflows captured as first-class entities\n"
        )
        
        report.append("### Natural Language Data Access")
        report.append(
            "- **Text-to-SQL Translation**: Business glossary empowers natural language queries over complex data\n"
            "- **Consistent Terminology**: Standard business vocabulary across the organization\n"
            "- **Self-Service Analytics**: Non-technical users can access data through natural language\n"
        )
        
        # Technical Graph Structure
        report.append("## Technical Graph Structure")
        
        # Tables
        report.append("### Tables and Columns")
        report.append(f"The semantic graph contains {table_stats.get('table_count', 0)} tables with {column_stats.get('column_count', 0)} total columns.\n")
        
        report.append("#### Top Tables by Column Count")
        if tables_and_columns:
            top_tables = sorted(tables_and_columns, key=lambda x: x.get("column_count", 0), reverse=True)[:10]
            
            top_tables_data = []
            for table in top_tables:
                top_tables_data.append([
                    table.get("table_name", ""),
                    table.get("table_description", "No description"),
                    table.get("column_count", 0)
                ])
            
            report.append(tabulate(top_tables_data, headers=["Table", "Description", "Column Count"], tablefmt="pipe"))
        else:
            report.append("No tables found.")
        report.append("\n")
        
        # Relationships
        report.append("### Table Relationships")
        if relationships:
            rel_count_by_table = {}
            for rel in relationships:
                source = rel.get("source_table", "")
                target = rel.get("target_table", "")
                
                if source not in rel_count_by_table:
                    rel_count_by_table[source] = {}
                    
                if target not in rel_count_by_table[source]:
                    rel_count_by_table[source][target] = 0
                    
                rel_count_by_table[source][target] += 1
            
            # Find tables with most relationships
            table_rel_counts = []
            for source, targets in rel_count_by_table.items():
                outgoing_count = sum(targets.values())
                incoming_count = 0
                
                for other_source, other_targets in rel_count_by_table.items():
                    if source in other_targets:
                        incoming_count += other_targets[source]
                
                table_rel_counts.append((source, outgoing_count, incoming_count))
            
            table_rel_counts.sort(key=lambda x: x[1] + x[2], reverse=True)
            
            # Show top tables by relationship count
            report.append("#### Top Tables by Relationship Count")
            top_rel_tables = []
            for table, outgoing, incoming in table_rel_counts[:10]:
                top_rel_tables.append([table, outgoing, incoming, outgoing + incoming])
            
            report.append(tabulate(top_rel_tables, headers=["Table", "Outgoing Relationships", "Incoming Relationships", "Total"], tablefmt="pipe"))
            report.append("\n")
            
            # Show top relationships by confidence
            report.append("#### Top Relationships by Confidence")
            top_relationships = sorted(relationships, key=lambda x: x.get("confidence", 0), reverse=True)[:15]
            
            top_rel_data = []
            for rel in top_relationships:
                top_rel_data.append([
                    f"{rel.get('source_table', '')}.{rel.get('source_column', '')}",
                    f"{rel.get('target_table', '')}.{rel.get('target_column', '')}",
                    f"{rel.get('confidence', 0):.2f}",
                    rel.get("relationship_type", "references"),
                    rel.get("detection_method", "unknown")
                ])
            
            report.append(tabulate(top_rel_data, headers=["Source", "Target", "Confidence", "Type", "Detection Method"], tablefmt="pipe"))
        else:
            report.append("No relationships found.")
        report.append("\n")
        
        # Business Glossary Section
        report.append("## Business Glossary")
        
        # Business Terms
        report.append("### Business Terms")
        report.append(f"The semantic graph contains {glossary_stats.get('business_terms', 0)} business terms providing business context for technical data elements.\n")
        
        if business_terms:
            # Show sample business terms
            report.append("#### Sample Business Terms")
            
            # Sort by number of mappings to find the most connected terms
            sorted_terms = sorted(business_terms, 
                                 key=lambda x: len(x.get("mapped_tables", [])) + len(x.get("mapped_columns", [])), 
                                 reverse=True)
            
            sample_terms = []
            for term in sorted_terms[:10]:
                mapped_to = []
                for table in term.get("mapped_tables", []):
                    if table:
                        mapped_to.append(table)
                        
                for column in term.get("mapped_columns", []):
                    if column:
                        mapped_to.append(column)
                
                sample_terms.append([
                    term.get("name", ""),
                    term.get("definition", "No definition")[:100] + "..." if term.get("definition", "") and len(term.get("definition", "")) > 100 else term.get("definition", "No definition"),
                    ", ".join(mapped_to[:3]) + (", ..." if len(mapped_to) > 3 else "")
                ])
            
            report.append(tabulate(sample_terms, headers=["Term", "Definition", "Mapped To"], tablefmt="pipe"))
        else:
            report.append("No business terms found.")
        report.append("\n")
        
        # Business Metrics
        report.append("### Business Metrics")
        report.append(f"The semantic graph contains {glossary_stats.get('business_metrics', 0)} business metrics that define KPIs and measurements.\n")
        
        if business_metrics:
            # Show sample business metrics
            report.append("#### Sample Business Metrics")
            
            sample_metrics = []
            for metric in business_metrics[:10]:
                sample_metrics.append([
                    metric.get("name", ""),
                    metric.get("definition", "No definition")[:100] + "..." if metric.get("definition", "") and len(metric.get("definition", "")) > 100 else metric.get("definition", "No definition")
                ])
            
            report.append(tabulate(sample_metrics, headers=["Metric", "Definition"], tablefmt="pipe"))
        else:
            report.append("No business metrics found.")
        report.append("\n")
        
        # Graph-Aware Glossary Entities
        report.append("## Graph-Aware Glossary Entities")
        report.append(
            "The following entity types leverage the graph structure to provide rich semantic understanding "
            "that spans multiple tables and captures business meaning across the data model.\n"
        )
        
        # Composite Concepts
        report.append("### Composite Concepts")
        report.append(
            "Composite concepts represent business entities that span multiple tables, capturing complex real-world "
            "business objects that cannot be represented by a single table.\n"
        )
        report.append(f"The semantic graph contains {glossary_stats.get('composite_concepts', 0)} composite concepts.\n")
        
        if composite_concepts:
            # Show all composite concepts
            cc_data = []
            for cc in composite_concepts:
                tables = cc.get("tables", [])
                if isinstance(tables, str):
                    tables = [tables]
                
                cc_data.append([
                    cc.get("name", ""),
                    cc.get("definition", "No definition")[:100] + "..." if cc.get("definition", "") and len(cc.get("definition", "")) > 100 else cc.get("definition", "No definition"),
                    cc.get("business_importance", ""),
                    ", ".join(tables[:3]) + (", ..." if len(tables) > 3 else "")
                ])
            
            report.append(tabulate(cc_data, headers=["Concept", "Definition", "Business Importance", "Tables"], tablefmt="pipe"))
        else:
            report.append("No composite concepts found.")
        report.append("\n")
        
        # Business Processes
        report.append("### Business Processes")
        report.append(
            "Business processes represent workflows and procedures that span multiple tables, "
            "capturing the operational flow of business activities.\n"
        )
        report.append(f"The semantic graph contains {glossary_stats.get('business_processes', 0)} business processes.\n")
        
        if business_processes:
            # Show all business processes
            bp_data = []
            for bp in business_processes:
                tables = bp.get("tables", [])
                if isinstance(tables, str):
                    tables = [tables]
                
                bp_data.append([
                    bp.get("name", ""),
                    bp.get("definition", "No definition")[:100] + "..." if bp.get("definition", "") and len(bp.get("definition", "")) > 100 else bp.get("definition", "No definition"),
                    ", ".join(tables[:3]) + (", ..." if len(tables) > 3 else "")
                ])
            
            report.append(tabulate(bp_data, headers=["Process", "Definition", "Tables"], tablefmt="pipe"))
        else:
            report.append("No business processes found.")
        report.append("\n")
        
        # Relationship Concepts
        report.append("### Relationship Concepts")
        report.append(
            "Relationship concepts capture business-meaningful connections between tables, "
            "providing semantic context to technical foreign key relationships.\n"
        )
        report.append(f"The semantic graph contains {glossary_stats.get('relationship_concepts', 0)} relationship concepts.\n")
        
        if relationship_concepts:
            # Show all relationship concepts
            rc_data = []
            for rc in relationship_concepts:
                tables = rc.get("tables", [])
                if isinstance(tables, str):
                    tables = [tables]
                
                rc_data.append([
                    rc.get("name", ""),
                    rc.get("definition", "No definition")[:100] + "..." if rc.get("definition", "") and len(rc.get("definition", "")) > 100 else rc.get("definition", "No definition"),
                    ", ".join(tables[:3]) + (", ..." if len(tables) > 3 else "")
                ])
            
            report.append(tabulate(rc_data, headers=["Relationship", "Definition", "Tables"], tablefmt="pipe"))
        else:
            report.append("No relationship concepts found.")
        report.append("\n")
        
        # Hierarchical Concepts
        report.append("### Hierarchical Concepts")
        report.append(
            "Hierarchical concepts represent parent-child structures in the data, "
            "providing insight into classification hierarchies and organizational structures.\n"
        )
        report.append(f"The semantic graph contains {glossary_stats.get('hierarchical_concepts', 0)} hierarchical concepts.\n")
        
        if hierarchical_concepts:
            # Show all hierarchical concepts
            hc_data = []
            for hc in hierarchical_concepts:
                tables = hc.get("tables", [])
                if isinstance(tables, str):
                    tables = [tables]
                
                hc_data.append([
                    hc.get("name", ""),
                    hc.get("definition", "No definition")[:100] + "..." if hc.get("definition", "") and len(hc.get("definition", "")) > 100 else hc.get("definition", "No definition"),
                    ", ".join(tables[:3]) + (", ..." if len(tables) > 3 else "")
                ])
            
            report.append(tabulate(hc_data, headers=["Hierarchy", "Definition", "Tables"], tablefmt="pipe"))
        else:
            report.append("No hierarchical concepts found.")
        report.append("\n")
        
        # Network Analysis
        report.append("## Network Analysis")
        
        # Term Relationships
        report.append("### Business Term Relationships")
        
        if term_relationships:
            # Analyze term relationship network
            term_connections = defaultdict(int)
            
            for rel in term_relationships:
                source = rel.get("source_term", "")
                target = rel.get("target_term", "")
                
                if source and target:
                    term_connections[source] += 1
                    term_connections[target] += 1
            
            # Find most connected terms
            most_connected = sorted(term_connections.items(), key=lambda x: x[1], reverse=True)[:10]
            
            mc_data = []
            for term, connections in most_connected:
                mc_data.append([term, connections])
            
            report.append("#### Most Connected Business Terms")
            report.append(tabulate(mc_data, headers=["Term", "Connection Count"], tablefmt="pipe"))
        else:
            report.append("No term relationships found.")
        report.append("\n")
        
        # Business Value in Action
        report.append("## Business Value in Action")
        
        # Text-to-SQL Examples
        report.append("### Example Natural Language Queries")
        report.append(
            "The semantic graph enables natural language queries that leverage the business glossary. "
            "Here are some example queries that can be translated to SQL:\n"
        )
        
        # Dynamically generate example queries based on the glossary
        example_queries = []
        
        # Use business terms for simple queries
        if business_terms and len(business_terms) > 5:
            # Find terms with table mappings
            terms_with_mappings = [t for t in business_terms if t.get("mapped_tables")]
            if terms_with_mappings:
                term1 = terms_with_mappings[0].get("name")
                example_queries.append(f"\"Show me all {term1}\"")
                
                if len(terms_with_mappings) > 1:
                    term2 = terms_with_mappings[1].get("name")
                    example_queries.append(f"\"What is the total number of {term2}?\"")
                    
                    if len(terms_with_mappings) > 2:
                        term3 = terms_with_mappings[2].get("name")
                        example_queries.append(f"\"List the top 10 {term3} by count\"")
        
        # Use business metrics for aggregation queries
        if business_metrics and len(business_metrics) > 2:
            metric1 = business_metrics[0].get("name")
            example_queries.append(f"\"Calculate the {metric1} for each department\"")
            
            if len(business_metrics) > 1:
                metric2 = business_metrics[1].get("name")
                example_queries.append(f"\"Show me the trend of {metric2} over the last 12 months\"")
        
        # Use composite concepts for complex queries
        if composite_concepts and len(composite_concepts) > 1:
            cc1 = composite_concepts[0].get("name")
            example_queries.append(f"\"Find all {cc1} with associated details\"")
        
        # Use business processes for workflow queries
        if business_processes and len(business_processes) > 1:
            bp1 = business_processes[0].get("name")
            example_queries.append(f"\"Show the status of {bp1} with timestamps\"")
        
        # Add default examples if we couldn't generate good ones
        if len(example_queries) < 5:
            default_examples = [
                "\"Show me all store locations with negative inventory variance\"",
                "\"What is the total maintenance cost by asset category?\"",
                "\"List the top 10 products with highest stock-outs last month\"",
                "\"Show me all work orders for critical assets that haven't been completed\"",
                "\"Calculate the average time to complete maintenance requests by priority\""
            ]
            example_queries.extend(default_examples[:5 - len(example_queries)])
        
        for query in example_queries[:5]:
            report.append(f"- {query}")
        report.append("\n")
        
        # Use Cases
        report.append("### Key Use Cases")
        report.append(
            "The semantic graph enables various high-value business use cases:\n\n"
            
            "1. **Integrated Business Intelligence**\n"
            "   - Connect dashboards directly to the semantic layer\n"
            "   - Consistent definitions across all reporting\n"
            "   - Automatic business context for technical metrics\n\n"
            
            "2. **Self-Service Analytics**\n"
            "   - Non-technical users can query using familiar business language\n"
            "   - Reduced dependency on data teams for simple queries\n"
            "   - Faster time-to-insight for business users\n\n"
            
            "3. **Data Lineage and Impact Analysis**\n"
            "   - Trace data flows across systems and tables\n"
            "   - Understand upstream and downstream dependencies\n"
            "   - Assess potential impact of schema changes\n\n"
            
            "4. **Knowledge Management**\n"
            "   - Central repository for business definitions\n"
            "   - Preserved institutional knowledge about data\n"
            "   - Reduced onboarding time for new analysts\n\n"
            
            "5. **Data Governance and Compliance**\n"
            "   - Clear ownership and definitions of business terms\n"
            "   - Consistent implementation of business rules\n"
            "   - Audit trail of data understanding\n"
        )
        
        # Conclusion
        report.append("## Conclusion")
        report.append(
            "The semantic graph for the Walmart-stores tenant provides a comprehensive knowledge layer that bridges "
            "technical metadata with business understanding. By connecting tables, relationships, and rich business "
            "entities, it enables more intuitive data exploration, better self-service analytics, and improved "
            "business intelligence.\n\n"
            
            "The graph-aware approach captures complex business concepts that span multiple tables, uncovering "
            "insights that would be difficult to discover through traditional approaches. This semantic layer "
            "continues to evolve as more data is added, relationships are discovered, and business understanding "
            "is enhanced.\n\n"
            
            "Next steps include expanding the graph to additional datasets, enriching the business glossary with "
            "domain expertise, and integrating the semantic graph with existing business intelligence tools to "
            "maximize the value across the organization."
        )
        
        return "\n".join(report)

async def main():
    """Generate a comprehensive semantic graph report."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Generate a semantic graph report.')
    parser.add_argument('--tenant', type=str, default="walmart-stores", help='Tenant ID')
    parser.add_argument('--output', type=str, default="walmart_comprehensive_report.md", help='Output filename')
    args = parser.parse_args()
    
    # Get Neo4j connection details
    neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
    
    tenant_id = args.tenant
    output_file = args.output
    
    try:
        # Initialize Neo4j client
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        
        # Initialize report generator
        reporter = AdaptedGraphReportGenerator(neo4j_client, tenant_id)
        
        # Generate report
        logger.info(f"Generating adapted report for tenant {tenant_id}...")
        report = reporter.generate_report()
        
        # Write report to file
        with open(output_file, 'w') as f:
            f.write(report)
            
        logger.info(f"Report generated successfully and saved to {output_file}")
        
        # Close Neo4j client
        neo4j_client.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        return False

if __name__ == "__main__":
    asyncio.run(main())