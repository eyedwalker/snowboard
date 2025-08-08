#!/usr/bin/env python3
"""
Eyecare Table Discovery and Migration Script
Analyzes 783 tables in blink_dev1 to identify key eyecare data for migration
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.ingestion.database_connector import DatabaseConnector, ConnectionConfig

# Load environment variables
load_dotenv(project_root / ".env")

console = Console()
logger = logging.getLogger(__name__)

class EyecareTableDiscovery:
    """Discover and categorize eyecare tables for migration"""
    
    def __init__(self):
        """Initialize with database connections"""
        # Source database config
        self.source_config = ConnectionConfig(
            db_type='sqlserver',
            host=os.getenv('SOURCE_DB_HOST'),
            port=int(os.getenv('SOURCE_DB_PORT', 1433)),
            database=os.getenv('SOURCE_DB_NAME'),
            username=os.getenv('SOURCE_DB_USER'),
            password=os.getenv('SOURCE_DB_PASSWORD')
        )
        
        # Snowflake config
        self.snowflake_config = ConnectionConfig(
            db_type='snowflake',
            host=os.getenv('SNOWFLAKE_ACCOUNT'),
            port=443,  # Standard Snowflake port
            database=os.getenv('SNOWFLAKE_DATABASE'),
            username=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            schema='RAW',
            additional_params={
                'role': os.getenv('SNOWFLAKE_ROLE'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE')
            }
        )
        
        self.db_connector = DatabaseConnector()
        
        # Eyecare business domain keywords
        self.eyecare_keywords = {
            'patient': ['patient', 'customer', 'person', 'individual', 'client'],
            'appointment': ['appointment', 'visit', 'schedule', 'booking', 'exam'],
            'billing': ['billing', 'invoice', 'payment', 'charge', 'claim', 'insurance'],
            'inventory': ['inventory', 'product', 'frame', 'lens', 'contact', 'supply'],
            'prescription': ['prescription', 'rx', 'refraction', 'vision', 'eye'],
            'employee': ['employee', 'staff', 'doctor', 'optometrist', 'technician'],
            'location': ['location', 'office', 'store', 'branch', 'clinic'],
            'order': ['order', 'sale', 'transaction', 'purchase'],
            'lab': ['lab', 'laboratory', 'manufacturing', 'production'],
            'vendor': ['vendor', 'supplier', 'manufacturer', 'provider']
        }
    
    def discover_all_tables(self) -> List[Dict]:
        """Discover all tables in the source database"""
        console.print(Panel("🔍 Discovering All Tables in blink_dev1", style="blue"))
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Scanning database tables...", total=None)
            
            # Connect to source database
            source_engine = self.db_connector.create_source_engine(self.source_config)
            source_conn = source_engine.connect()
            
            # Get all tables with metadata
            tables_query = """
            SELECT 
                t.TABLE_SCHEMA,
                t.TABLE_NAME,
                t.TABLE_TYPE,
                COALESCE(p.row_count, 0) as ROW_COUNT,
                COALESCE(c.column_count, 0) as COLUMN_COUNT
            FROM INFORMATION_SCHEMA.TABLES t
            LEFT JOIN (
                SELECT 
                    SCHEMA_NAME(o.schema_id) as TABLE_SCHEMA,
                    o.name as TABLE_NAME,
                    SUM(p.rows) as row_count
                FROM sys.objects o
                INNER JOIN sys.partitions p ON o.object_id = p.object_id
                WHERE o.type = 'U' AND p.index_id IN (0,1)
                GROUP BY SCHEMA_NAME(o.schema_id), o.name
            ) p ON t.TABLE_SCHEMA = p.TABLE_SCHEMA AND t.TABLE_NAME = p.TABLE_NAME
            LEFT JOIN (
                SELECT 
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    COUNT(*) as column_count
                FROM INFORMATION_SCHEMA.COLUMNS
                GROUP BY TABLE_SCHEMA, TABLE_NAME
            ) c ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
            WHERE t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY COALESCE(p.row_count, 0) DESC, t.TABLE_NAME
            """
            
            tables_df = pd.read_sql(tables_query, source_conn)
            source_conn.close()
            
            progress.update(task, completed=100)
        
        console.print(f"✅ Found {len(tables_df)} tables")
        return tables_df.to_dict('records')
    
    def categorize_eyecare_tables(self, tables: List[Dict]) -> Dict[str, List[Dict]]:
        """Categorize tables by eyecare business domain"""
        console.print(Panel("🏥 Categorizing Tables by Eyecare Domain", style="green"))
        
        categorized = {domain: [] for domain in self.eyecare_keywords.keys()}
        categorized['other'] = []
        
        for table in tables:
            table_name = table['TABLE_NAME'].lower()
            schema_name = table['TABLE_SCHEMA'].lower()
            full_name = f"{schema_name}.{table_name}"
            
            # Score table against each domain
            domain_scores = {}
            for domain, keywords in self.eyecare_keywords.items():
                score = sum(1 for keyword in keywords if keyword in full_name)
                if score > 0:
                    domain_scores[domain] = score
            
            # Assign to highest scoring domain
            if domain_scores:
                best_domain = max(domain_scores, key=domain_scores.get)
                table['domain_score'] = domain_scores[best_domain]
                categorized[best_domain].append(table)
            else:
                categorized['other'].append(table)
        
        return categorized
    
    def prioritize_migration_tables(self, categorized_tables: Dict[str, List[Dict]]) -> List[Dict]:
        """Prioritize tables for migration based on business importance and data volume"""
        console.print(Panel("📊 Prioritizing Tables for Migration", style="yellow"))
        
        # Priority weights by domain
        domain_weights = {
            'patient': 10,
            'appointment': 9,
            'billing': 8,
            'prescription': 8,
            'order': 7,
            'inventory': 6,
            'employee': 5,
            'location': 5,
            'lab': 4,
            'vendor': 3,
            'other': 1
        }
        
        priority_tables = []
        
        for domain, tables in categorized_tables.items():
            domain_weight = domain_weights.get(domain, 1)
            
            for table in tables:
                # Calculate priority score
                row_count = table.get('ROW_COUNT', 0)
                column_count = table.get('COLUMN_COUNT', 0)
                domain_score = table.get('domain_score', 0)
                
                # Priority formula: domain_weight * (log(rows) + columns/10 + domain_score)
                import math
                priority_score = domain_weight * (
                    math.log10(max(row_count, 1)) + 
                    column_count / 10 + 
                    domain_score * 2
                )
                
                table['priority_score'] = round(priority_score, 2)
                table['domain'] = domain
                priority_tables.append(table)
        
        # Sort by priority score
        priority_tables.sort(key=lambda x: x['priority_score'], reverse=True)
        
        return priority_tables
    
    def display_migration_plan(self, priority_tables: List[Dict]):
        """Display the migration plan with prioritized tables"""
        console.print(Panel("🚀 Eyecare Data Migration Plan", style="bold blue"))
        
        # Top 20 priority tables
        top_tables = priority_tables[:20]
        
        table = Table(title="Top 20 Priority Tables for Migration")
        table.add_column("Rank", style="cyan", no_wrap=True)
        table.add_column("Schema", style="blue")
        table.add_column("Table Name", style="green")
        table.add_column("Domain", style="yellow")
        table.add_column("Rows", style="magenta")
        table.add_column("Columns", style="red")
        table.add_column("Priority", style="bold white")
        
        for i, tbl in enumerate(top_tables, 1):
            table.add_row(
                str(i),
                tbl['TABLE_SCHEMA'],
                tbl['TABLE_NAME'],
                tbl['domain'].title(),
                f"{tbl['ROW_COUNT']:,}",
                str(tbl['COLUMN_COUNT']),
                str(tbl['priority_score'])
            )
        
        console.print(table)
        
        # Domain summary
        domain_summary = {}
        for tbl in priority_tables:
            domain = tbl['domain']
            if domain not in domain_summary:
                domain_summary[domain] = {'count': 0, 'total_rows': 0}
            domain_summary[domain]['count'] += 1
            domain_summary[domain]['total_rows'] += tbl['ROW_COUNT']
        
        summary_table = Table(title="Migration Summary by Domain")
        summary_table.add_column("Domain", style="cyan")
        summary_table.add_column("Tables", style="green")
        summary_table.add_column("Total Rows", style="magenta")
        summary_table.add_column("Avg Priority", style="yellow")
        
        for domain, stats in domain_summary.items():
            avg_priority = sum(t['priority_score'] for t in priority_tables if t['domain'] == domain) / stats['count']
            summary_table.add_row(
                domain.title(),
                str(stats['count']),
                f"{stats['total_rows']:,}",
                f"{avg_priority:.1f}"
            )
        
        console.print(summary_table)
        
        return top_tables
    
    def save_migration_plan(self, priority_tables: List[Dict]):
        """Save migration plan to file"""
        output_dir = project_root / "docs" / "migration"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save as CSV
        df = pd.DataFrame(priority_tables)
        csv_path = output_dir / "eyecare_migration_plan.csv"
        df.to_csv(csv_path, index=False)
        
        console.print(f"✅ Migration plan saved to: {csv_path}")
        
        return csv_path

def main():
    """Main execution function"""
    console.print(Panel("🏥 Eyecare Table Discovery & Migration Planning", style="bold green"))
    
    try:
        discovery = EyecareTableDiscovery()
        
        # Step 1: Discover all tables
        all_tables = discovery.discover_all_tables()
        
        # Step 2: Categorize by eyecare domain
        categorized = discovery.categorize_eyecare_tables(all_tables)
        
        # Step 3: Prioritize for migration
        priority_tables = discovery.prioritize_migration_tables(categorized)
        
        # Step 4: Display migration plan
        top_tables = discovery.display_migration_plan(priority_tables)
        
        # Step 5: Save migration plan
        discovery.save_migration_plan(priority_tables)
        
        console.print(Panel(
            f"🎉 Discovery Complete!\n\n"
            f"• Total Tables: {len(all_tables)}\n"
            f"• Top Priority Tables: {len(top_tables)}\n"
            f"• Ready for Migration: ✅",
            style="bold green"
        ))
        
    except Exception as e:
        console.print(f"❌ Discovery failed: {str(e)}", style="bold red")
        logger.error(f"Discovery failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
