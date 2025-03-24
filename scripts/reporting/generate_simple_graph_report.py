#!/usr/bin/env python3
"""
Simplified Semantic Graph Report Generator

This script generates a simplified report on the semantic graph structure
for the walmart-stores tenant, focusing on basic statistics and entity counts.

Usage:
    python generate_simple_graph_report.py
"""

import os
import asyncio
import logging
from dotenv import load_dotenv
from tabulate import tabulate
from datetime import datetime

from src.graph_storage.neo4j_client import Neo4jClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class SimpleGraphReportGenerator:
    """Generates a simplified report on the semantic graph structure."""
    
    def __init__(self, neo4j_client, tenant_id="walmart-stores"):
        """Initialize the report generator."""
        self.neo4j_client = neo4j_client
        self.tenant_id = tenant_id
    
    def get_table_count(self):
        """Get count of tables for this tenant."""
        query = """
        MATCH (t:Table {tenant_id: $tenant_id})
        RETURN count(t) as count
        """
        
        result = self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
        return result[0]["count"] if result else 0
    
    def get_column_count(self):
        """Get count of columns for this tenant."""
        query = """
        MATCH (c:Column {tenant_id: $tenant_id})
        RETURN count(c) as count
        """
        
        result = self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
        return result[0]["count"] if result else 0
    
    def get_glossary_entity_counts(self):
        """Get counts of all glossary entity types."""
        queries = {
            "business_terms": """
                MATCH (t:GlossaryTerm {tenant_id: $tenant_id})
                RETURN count(t) as count
            """,
            "business_metrics": """
                MATCH (m:BusinessMetric {tenant_id: $tenant_id})
                RETURN count(m) as count
            """,
            "composite_concepts": """
                MATCH (cc:CompositeConcept {tenant_id: $tenant_id})
                RETURN count(cc) as count
            """,
            "business_processes": """
                MATCH (bp:BusinessProcess {tenant_id: $tenant_id})
                RETURN count(bp) as count
            """,
            "relationship_concepts": """
                MATCH (rc:RelationshipConcept {tenant_id: $tenant_id})
                RETURN count(rc) as count
            """,
            "hierarchical_concepts": """
                MATCH (hc:HierarchicalConcept {tenant_id: $tenant_id})
                RETURN count(hc) as count
            """
        }
        
        results = {}
        for entity_type, query in queries.items():
            try:
                result = self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
                results[entity_type] = result[0]["count"] if result else 0
            except Exception as e:
                logger.warning(f"Error getting count for {entity_type}: {e}")
                results[entity_type] = 0
                
        return results
    
    def get_business_terms(self, limit=10):
        """Get sample business terms."""
        query = """
        MATCH (t:GlossaryTerm {tenant_id: $tenant_id})
        RETURN t.name as name, t.definition as definition
        LIMIT $limit
        """
        
        return self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id, "limit": limit})
    
    def get_business_metrics(self, limit=10):
        """Get sample business metrics."""
        query = """
        MATCH (m:BusinessMetric {tenant_id: $tenant_id})
        RETURN m.name as name, m.definition as definition
        LIMIT $limit
        """
        
        return self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id, "limit": limit})
    
    def get_composite_concepts(self, limit=10):
        """Get sample composite concepts."""
        query = """
        MATCH (cc:CompositeConcept {tenant_id: $tenant_id})
        RETURN cc.name as name, cc.definition as definition
        LIMIT $limit
        """
        
        return self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id, "limit": limit})
    
    def get_business_processes(self, limit=10):
        """Get sample business processes."""
        query = """
        MATCH (bp:BusinessProcess {tenant_id: $tenant_id})
        RETURN bp.name as name, bp.definition as definition
        LIMIT $limit
        """
        
        return self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id, "limit": limit})
    
    def get_relationship_concepts(self, limit=10):
        """Get sample relationship concepts."""
        query = """
        MATCH (rc:RelationshipConcept {tenant_id: $tenant_id})
        RETURN rc.name as name, rc.definition as definition
        LIMIT $limit
        """
        
        return self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id, "limit": limit})
    
    def get_hierarchical_concepts(self, limit=10):
        """Get sample hierarchical concepts."""
        query = """
        MATCH (hc:HierarchicalConcept {tenant_id: $tenant_id})
        RETURN hc.name as name, hc.definition as definition
        LIMIT $limit
        """
        
        return self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id, "limit": limit})
    
    def generate_report(self):
        """Generate a simple report on the semantic graph structure."""
        try:
            # Get counts
            table_count = self.get_table_count()
            column_count = self.get_column_count()
            glossary_counts = self.get_glossary_entity_counts()
            
            # Get sample entities
            business_terms = self.get_business_terms()
            business_metrics = self.get_business_metrics()
            composite_concepts = self.get_composite_concepts()
            business_processes = self.get_business_processes()
            relationship_concepts = self.get_relationship_concepts()
            hierarchical_concepts = self.get_hierarchical_concepts()
            
            # Create the report
            report = []
            
            # Title
            report.append(f"# Semantic Graph Report for Walmart Stores")
            report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            
            # Executive Summary
            report.append("## Executive Summary")
            report.append(
                "This report provides an overview of the semantic graph for Walmart Stores, "
                "showcasing the rich metadata and business context captured in the knowledge graph.\n"
            )
            
            # Graph Statistics
            report.append("## Graph Statistics")
            report.append("### Core Graph Metrics")
            
            # Basic statistics table
            stats_data = [
                ["Tables", table_count],
                ["Columns", column_count],
                ["Business Terms", glossary_counts.get("business_terms", 0)],
                ["Business Metrics", glossary_counts.get("business_metrics", 0)],
                ["Composite Concepts", glossary_counts.get("composite_concepts", 0)],
                ["Business Processes", glossary_counts.get("business_processes", 0)],
                ["Relationship Concepts", glossary_counts.get("relationship_concepts", 0)],
                ["Hierarchical Concepts", glossary_counts.get("hierarchical_concepts", 0)]
            ]
            
            report.append(tabulate(stats_data, headers=["Metric", "Count"], tablefmt="pipe"))
            report.append("\n")
            
            # Business Glossary Section
            report.append("## Business Glossary")
            
            # Business Terms
            report.append("### Business Terms")
            report.append(f"The semantic graph contains {glossary_counts.get('business_terms', 0)} business terms providing business context for technical data elements.")
            
            if business_terms:
                report.append("\n#### Sample Business Terms:")
                term_data = []
                for term in business_terms:
                    definition = term.get("definition", "")
                    if definition and len(definition) > 100:
                        definition = definition[:100] + "..."
                    term_data.append([term.get("name", ""), definition])
                
                report.append(tabulate(term_data, headers=["Term", "Definition"], tablefmt="pipe"))
            
            report.append("\n")
            
            # Business Metrics
            report.append("### Business Metrics")
            report.append(f"The semantic graph contains {glossary_counts.get('business_metrics', 0)} business metrics that define KPIs and measurements.")
            
            if business_metrics:
                report.append("\n#### Sample Business Metrics:")
                metric_data = []
                for metric in business_metrics:
                    definition = metric.get("definition", "")
                    if definition and len(definition) > 100:
                        definition = definition[:100] + "..."
                    metric_data.append([metric.get("name", ""), definition])
                
                report.append(tabulate(metric_data, headers=["Metric", "Definition"], tablefmt="pipe"))
            
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
            
            if composite_concepts:
                cc_data = []
                for cc in composite_concepts:
                    definition = cc.get("definition", "")
                    if definition and len(definition) > 100:
                        definition = definition[:100] + "..."
                    cc_data.append([cc.get("name", ""), definition])
                
                report.append(tabulate(cc_data, headers=["Concept", "Definition"], tablefmt="pipe"))
            else:
                report.append("No composite concepts found.")
            
            report.append("\n")
            
            # Business Processes
            report.append("### Business Processes")
            report.append(
                "Business processes represent workflows and procedures that span multiple tables, "
                "capturing the operational flow of business activities.\n"
            )
            
            if business_processes:
                bp_data = []
                for bp in business_processes:
                    definition = bp.get("definition", "")
                    if definition and len(definition) > 100:
                        definition = definition[:100] + "..."
                    bp_data.append([bp.get("name", ""), definition])
                
                report.append(tabulate(bp_data, headers=["Process", "Definition"], tablefmt="pipe"))
            else:
                report.append("No business processes found.")
            
            report.append("\n")
            
            # Relationship Concepts
            report.append("### Relationship Concepts")
            report.append(
                "Relationship concepts capture business-meaningful connections between tables, "
                "providing semantic context to technical foreign key relationships.\n"
            )
            
            if relationship_concepts:
                rc_data = []
                for rc in relationship_concepts:
                    definition = rc.get("definition", "")
                    if definition and len(definition) > 100:
                        definition = definition[:100] + "..."
                    rc_data.append([rc.get("name", ""), definition])
                
                report.append(tabulate(rc_data, headers=["Relationship", "Definition"], tablefmt="pipe"))
            else:
                report.append("No relationship concepts found.")
            
            report.append("\n")
            
            # Hierarchical Concepts
            report.append("### Hierarchical Concepts")
            report.append(
                "Hierarchical concepts represent parent-child structures in the data, "
                "providing insight into classification hierarchies and organizational structures.\n"
            )
            
            if hierarchical_concepts:
                hc_data = []
                for hc in hierarchical_concepts:
                    definition = hc.get("definition", "")
                    if definition and len(definition) > 100:
                        definition = definition[:100] + "..."
                    hc_data.append([hc.get("name", ""), definition])
                
                report.append(tabulate(hc_data, headers=["Hierarchy", "Definition"], tablefmt="pipe"))
            else:
                report.append("No hierarchical concepts found.")
            
            report.append("\n")
            
            # Business Value
            report.append("## Business Value")
            report.append(
                "### Value Proposition of the Semantic Graph\n\n"
                "The semantic graph provides significant business value through:\n\n"
                
                "1. **Enhanced Data Discovery and Understanding**\n"
                "   - Rich business context for technical data elements\n"
                "   - Cross-table semantic connections\n"
                "   - Domain-specific knowledge capture\n\n"
                
                "2. **Natural Language Querying**\n"
                "   - Business terminology for data access\n"
                "   - Self-service analytics for non-technical users\n"
                "   - More intuitive interaction with complex data\n\n"
                
                "3. **Knowledge Management**\n"
                "   - Preservation of institutional knowledge\n"
                "   - Consistent business definitions\n"
                "   - Reduced onboarding time for new analysts\n\n"
                
                "4. **Cross-Table Business Insights**\n"
                "   - Understanding of complex business entities\n"
                "   - Visibility into business processes\n"
                "   - Recognition of important data relationships\n\n"
                
                "5. **Improved Data Governance**\n"
                "   - Clear ownership of business terms\n"
                "   - Consistent implementation of business rules\n"
                "   - Better compliance documentation\n"
            )
            
            # Conclusion
            report.append("## Conclusion")
            report.append(
                "The semantic graph for Walmart Stores provides a comprehensive knowledge layer that bridges "
                "technical metadata with business understanding. By implementing the graph-aware approach with "
                "table-by-table processing, we're able to handle large data volumes while still capturing rich "
                "semantic relationships.\n\n"
                
                "The graph-aware business glossary goes beyond traditional approaches by capturing composite concepts, "
                "business processes, relationship semantics, and hierarchical structures. This provides a more "
                "complete business context that enhances the value of the underlying data.\n\n"
                
                "With continued enrichment and expansion, this semantic graph will serve as a foundational "
                "component of Walmart's data strategy, enabling more intuitive data exploration, better self-service "
                "analytics, and improved business intelligence."
            )
            
            return "\n".join(report)
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")
            return f"Error generating report: {e}"

async def main():
    """Generate a simplified semantic graph report."""
    try:
        # Get Neo4j connection details
        neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        # Initialize Neo4j client
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        
        # Initialize report generator
        reporter = SimpleGraphReportGenerator(neo4j_client)
        
        # Generate report
        logger.info("Generating report for Walmart Stores tenant...")
        report = reporter.generate_report()
        
        # Write report to file
        output_file = "walmart_graph_report.md"
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