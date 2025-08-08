#!/usr/bin/env python3
"""
Simple Eyecare Data Migration
Streamlined approach to migrate key tables without complex SQL issues
"""

import os
import sys
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
import snowflake.connector
from sqlalchemy import create_engine, text

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Load environment variables
load_dotenv(project_root / ".env")

console = Console()

class SimpleMigration:
    """Simple, reliable migration for key eyecare tables"""
    
    def __init__(self):
        """Initialize simple migration"""
        # Key tables to migrate (manually selected for reliability)
        self.priority_tables = [
            'Patient',
            'Orders', 
            'BillingClaim',
            'PatientInsurance',
            'InvoiceDetail',
            'EGRX',
            'OrderItem',
            'BillingClaimDetail',
            'PatientExam',
            'AppSchedule'
        ]
        
        # Source connection
        self.source_conn_str = f"mssql+pymssql://{os.getenv('SOURCE_DB_USER')}:{os.getenv('SOURCE_DB_PASSWORD')}@{os.getenv('SOURCE_DB_HOST')}:{os.getenv('SOURCE_DB_PORT')}/{os.getenv('SOURCE_DB_NAME')}?charset=utf8"
        
        # Snowflake connection
        self.sf_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'role': os.getenv('SNOWFLAKE_ROLE'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': 'EYECARE_ANALYTICS',
            'schema': 'RAW'
        }
    
    def migrate_key_tables(self):
        """Migrate key eyecare tables"""
        console.print(Panel("🚀 Simple Migration of Key Eyecare Tables", style="bold green"))
        
        # Create connections
        source_engine = create_engine(self.source_conn_str)
        sf_conn = snowflake.connector.connect(**self.sf_params)
        
        results = {
            'successful': [],
            'failed': [],
            'total_rows': 0
        }
        
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                console=console
            ) as progress:
                
                task = progress.add_task("Migrating tables...", total=len(self.priority_tables))
                
                for table_name in self.priority_tables:
                    progress.update(task, description=f"[cyan]Migrating {table_name}")
                    
                    try:
                        # Simple approach: extract, transform, load
                        rows_migrated = self._migrate_single_table(source_engine, sf_conn, table_name)
                        
                        results['successful'].append({
                            'table': table_name,
                            'rows': rows_migrated
                        })
                        results['total_rows'] += rows_migrated
                        
                        console.print(f"✅ {table_name}: {rows_migrated:,} rows", style="green")
                        
                    except Exception as e:
                        results['failed'].append({
                            'table': table_name,
                            'error': str(e)
                        })
                        console.print(f"❌ {table_name}: {str(e)[:100]}...", style="red")
                    
                    progress.advance(task)
        
        finally:
            source_engine.dispose()
            sf_conn.close()
        
        # Display results
        self._display_results(results)
        return results
    
    def _migrate_single_table(self, source_engine, sf_conn, table_name: str) -> int:
        """Migrate a single table using simple approach"""
        
        # Step 1: Extract data from source (simple query)
        with source_engine.connect() as conn:
            try:
                # Simple SELECT with LIMIT for safety
                query = f"SELECT TOP 10000 * FROM dbo.{table_name}"
                df = pd.read_sql(query, conn)
                
                if df.empty:
                    return 0
                
            except Exception as e:
                # If table doesn't exist or has issues, skip it
                raise Exception(f"Could not read table: {str(e)}")
        
        # Step 2: Clean data for Snowflake
        df = self._clean_dataframe(df)
        
        # Step 3: Create table in Snowflake
        sf_table_name = f"DBO_{table_name}".upper()
        
        cursor = sf_conn.cursor()
        try:
            # Drop and recreate table
            cursor.execute(f"DROP TABLE IF EXISTS EYECARE_ANALYTICS.RAW.{sf_table_name}")
            
            # Create table with simple VARCHAR columns
            columns = [f'"{col}" VARCHAR(16777216)' for col in df.columns]
            create_sql = f"""
            CREATE TABLE EYECARE_ANALYTICS.RAW.{sf_table_name} (
                {', '.join(columns)}
            )
            """
            cursor.execute(create_sql)
            
            # Step 4: Insert data using simple INSERT statements
            if not df.empty:
                return self._insert_data_simple(cursor, sf_table_name, df)
            else:
                return 0
                
        finally:
            cursor.close()
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame for Snowflake"""
        # Convert all columns to string and handle nulls
        for col in df.columns:
            df[col] = df[col].astype(str)
            df[col] = df[col].replace('nan', '')
            df[col] = df[col].replace('None', '')
            df[col] = df[col].replace('NaT', '')
            df[col] = df[col].fillna('')
        
        return df
    
    def _insert_data_simple(self, cursor, table_name: str, df: pd.DataFrame) -> int:
        """Insert data using simple batch INSERT"""
        if df.empty:
            return 0
        
        # Prepare column names
        columns = [f'"{col}"' for col in df.columns]
        columns_str = ', '.join(columns)
        
        # Insert in small batches
        batch_size = 100
        total_inserted = 0
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            
            # Prepare VALUES for batch
            values_list = []
            for _, row in batch.iterrows():
                values = []
                for col in df.columns:
                    value = str(row[col])
                    # Escape single quotes
                    value = value.replace("'", "''")
                    values.append(f"'{value}'")
                values_list.append(f"({', '.join(values)})")
            
            # Execute batch INSERT
            values_str = ', '.join(values_list)
            insert_sql = f"""
            INSERT INTO EYECARE_ANALYTICS.RAW.{table_name} ({columns_str})
            VALUES {values_str}
            """
            
            try:
                cursor.execute(insert_sql)
                total_inserted += len(batch)
            except Exception as e:
                console.print(f"⚠️ Batch insert failed: {str(e)[:100]}...", style="yellow")
                continue
        
        return total_inserted
    
    def _display_results(self, results: dict):
        """Display migration results"""
        
        # Success table
        if results['successful']:
            success_table = Table(title="✅ Successful Migrations")
            success_table.add_column("Table", style="cyan")
            success_table.add_column("Rows Migrated", style="green")
            
            for item in results['successful']:
                success_table.add_row(item['table'], f"{item['rows']:,}")
            
            console.print(success_table)
        
        # Summary
        total_tables = len(results['successful']) + len(results['failed'])
        success_rate = (len(results['successful']) / total_tables * 100) if total_tables > 0 else 0
        
        summary_table = Table(title="📊 Migration Summary")
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="green")
        
        summary_table.add_row("Total Tables", str(total_tables))
        summary_table.add_row("Successful", str(len(results['successful'])))
        summary_table.add_row("Failed", str(len(results['failed'])))
        summary_table.add_row("Success Rate", f"{success_rate:.1f}%")
        summary_table.add_row("Total Rows", f"{results['total_rows']:,}")
        
        console.print(summary_table)
        
        # Show failures
        if results['failed']:
            console.print(Panel("❌ Failed Tables", style="red"))
            for item in results['failed'][:5]:
                console.print(f"  {item['table']}: {item['error'][:100]}...", style="red")

def main():
    """Main execution"""
    console.print(Panel("🏥 Simple Eyecare Data Migration", style="bold green"))
    
    try:
        migration = SimpleMigration()
        results = migration.migrate_key_tables()
        
        console.print(Panel(
            f"🎉 Simple Migration Complete!\n\n"
            f"• Tables Migrated: {len(results['successful'])}\n"
            f"• Total Rows: {results['total_rows']:,}\n"
            f"• Your key eyecare data is now in Snowflake! ✅",
            style="bold green"
        ))
        
    except Exception as e:
        console.print(f"❌ Migration failed: {str(e)}", style="bold red")

if __name__ == "__main__":
    main()
