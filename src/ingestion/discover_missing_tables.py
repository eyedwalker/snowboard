#!/usr/bin/env python3
"""
Discover Missing Tables
Find the correct table names for failed migrations
"""

import os
import sys
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from sqlalchemy import create_engine, text

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Load environment variables
load_dotenv(project_root / ".env")

console = Console()

class TableDiscovery:
    """Discover correct table names for failed migrations"""
    
    def __init__(self):
        """Initialize table discovery"""
        # Source connection
        self.source_conn_str = f"mssql+pymssql://{os.getenv('SOURCE_DB_USER')}:{os.getenv('SOURCE_DB_PASSWORD')}@{os.getenv('SOURCE_DB_HOST')}:{os.getenv('SOURCE_DB_PORT')}/{os.getenv('SOURCE_DB_NAME')}?charset=utf8"
        
        # Failed table patterns to search for
        self.failed_patterns = [
            'InvoiceDetail',
            'OrderItem', 
            'BillingClaimDetail',
            'AppSchedule'
        ]
    
    def discover_table_names(self):
        """Discover actual table names in the database"""
        console.print(Panel("🔍 Discovering Actual Table Names", style="bold blue"))
        
        engine = create_engine(self.source_conn_str)
        
        try:
            with engine.connect() as conn:
                # Get all table names
                all_tables_query = """
                SELECT 
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
                """
                
                all_tables_df = pd.read_sql(all_tables_query, conn)
                
                console.print(f"✅ Found {len(all_tables_df)} total tables")
                
                # Search for patterns
                discovered_tables = {}
                
                for pattern in self.failed_patterns:
                    console.print(f"\n🔍 Searching for tables matching '{pattern}'...")
                    
                    # Case-insensitive search
                    matches = all_tables_df[
                        all_tables_df['TABLE_NAME'].str.contains(pattern, case=False, na=False)
                    ]
                    
                    if not matches.empty:
                        console.print(f"✅ Found {len(matches)} matches for '{pattern}':")
                        
                        match_table = Table(title=f"Matches for '{pattern}'")
                        match_table.add_column("Schema", style="cyan")
                        match_table.add_column("Table Name", style="green")
                        match_table.add_column("Type", style="yellow")
                        
                        for _, row in matches.iterrows():
                            match_table.add_row(
                                row['TABLE_SCHEMA'],
                                row['TABLE_NAME'],
                                row['TABLE_TYPE']
                            )
                        
                        console.print(match_table)
                        discovered_tables[pattern] = matches.to_dict('records')
                    else:
                        console.print(f"❌ No matches found for '{pattern}'")
                        discovered_tables[pattern] = []
                
                return discovered_tables, all_tables_df
                
        finally:
            engine.dispose()
    
    def search_similar_names(self, all_tables_df: pd.DataFrame):
        """Search for similar table names using fuzzy matching"""
        console.print(Panel("🔍 Searching for Similar Table Names", style="bold yellow"))
        
        # Keywords that might be in the table names
        keywords = [
            'invoice', 'detail', 'order', 'item', 'billing', 'claim', 
            'schedule', 'appointment', 'app', 'sch'
        ]
        
        similar_tables = {}
        
        for keyword in keywords:
            matches = all_tables_df[
                all_tables_df['TABLE_NAME'].str.contains(keyword, case=False, na=False)
            ]
            
            if not matches.empty:
                similar_tables[keyword] = matches.to_dict('records')
        
        # Display results
        for keyword, tables in similar_tables.items():
            if tables:
                console.print(f"\n📋 Tables containing '{keyword}':")
                
                keyword_table = Table()
                keyword_table.add_column("Schema", style="cyan")
                keyword_table.add_column("Table Name", style="green")
                
                for table in tables[:10]:  # Show first 10
                    keyword_table.add_row(
                        table['TABLE_SCHEMA'],
                        table['TABLE_NAME']
                    )
                
                console.print(keyword_table)
        
        return similar_tables
    
    def suggest_migration_targets(self, discovered_tables: dict, similar_tables: dict):
        """Suggest the best migration targets"""
        console.print(Panel("🎯 Migration Target Suggestions", style="bold green"))
        
        suggestions = []
        
        # Analyze discovered tables
        for pattern, matches in discovered_tables.items():
            if matches:
                # Pick the best match (usually the first one)
                best_match = matches[0]
                suggestions.append({
                    'Original_Pattern': pattern,
                    'Suggested_Schema': best_match['TABLE_SCHEMA'],
                    'Suggested_Table': best_match['TABLE_NAME'],
                    'Confidence': 'High'
                })
            else:
                # Look in similar tables for alternatives
                alternatives = []
                
                if pattern.lower() == 'invoicedetail':
                    alternatives = [t for t in similar_tables.get('invoice', []) if 'detail' in t['TABLE_NAME'].lower()]
                elif pattern.lower() == 'orderitem':
                    alternatives = [t for t in similar_tables.get('order', []) if 'item' in t['TABLE_NAME'].lower()]
                elif pattern.lower() == 'billingclaimdetail':
                    alternatives = [t for t in similar_tables.get('billing', []) if 'detail' in t['TABLE_NAME'].lower()]
                elif pattern.lower() == 'appschedule':
                    alternatives = [t for t in similar_tables.get('schedule', []) + similar_tables.get('app', [])]
                
                if alternatives:
                    best_alt = alternatives[0]
                    suggestions.append({
                        'Original_Pattern': pattern,
                        'Suggested_Schema': best_alt['TABLE_SCHEMA'],
                        'Suggested_Table': best_alt['TABLE_NAME'],
                        'Confidence': 'Medium'
                    })
                else:
                    suggestions.append({
                        'Original_Pattern': pattern,
                        'Suggested_Schema': 'N/A',
                        'Suggested_Table': 'NOT_FOUND',
                        'Confidence': 'None'
                    })
        
        # Display suggestions
        suggestion_table = Table(title="🎯 Migration Target Suggestions")
        suggestion_table.add_column("Original Pattern", style="red")
        suggestion_table.add_column("Suggested Schema", style="cyan")
        suggestion_table.add_column("Suggested Table", style="green")
        suggestion_table.add_column("Confidence", style="yellow")
        
        for suggestion in suggestions:
            suggestion_table.add_row(
                suggestion['Original_Pattern'],
                suggestion['Suggested_Schema'],
                suggestion['Suggested_Table'],
                suggestion['Confidence']
            )
        
        console.print(suggestion_table)
        
        return suggestions

def main():
    """Main execution"""
    console.print(Panel("🔍 Table Name Discovery for Failed Migrations", style="bold green"))
    
    try:
        discovery = TableDiscovery()
        
        # Step 1: Discover exact table names
        discovered_tables, all_tables_df = discovery.discover_table_names()
        
        # Step 2: Search for similar names
        similar_tables = discovery.search_similar_names(all_tables_df)
        
        # Step 3: Suggest migration targets
        suggestions = discovery.suggest_migration_targets(discovered_tables, similar_tables)
        
        # Save results
        output_dir = project_root / "docs" / "migration"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save all tables for reference
        all_tables_path = output_dir / "all_tables_discovered.csv"
        all_tables_df.to_csv(all_tables_path, index=False)
        
        # Save suggestions
        suggestions_df = pd.DataFrame(suggestions)
        suggestions_path = output_dir / "migration_suggestions.csv"
        suggestions_df.to_csv(suggestions_path, index=False)
        
        console.print(Panel(
            f"🎉 Discovery Complete!\n\n"
            f"• Total Tables Found: {len(all_tables_df)}\n"
            f"• Suggestions Generated: {len(suggestions)}\n"
            f"• Results saved to: {suggestions_path}\n"
            f"• Ready to retry failed migrations! ✅",
            style="bold green"
        ))
        
    except Exception as e:
        console.print(f"❌ Discovery failed: {str(e)}", style="bold red")

if __name__ == "__main__":
    main()
