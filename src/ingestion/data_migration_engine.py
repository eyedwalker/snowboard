#!/usr/bin/env python3
"""
Eyecare Data Migration Engine
Migrates tables from SQL Server to Snowflake RAW layer with comprehensive monitoring
"""

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import time
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from rich.live import Live
import snowflake.connector
from sqlalchemy import text

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.ingestion.database_connector import DatabaseConnector, ConnectionConfig

# Load environment variables
load_dotenv(project_root / ".env")

console = Console()
logger = logging.getLogger(__name__)

class DataMigrationEngine:
    """Comprehensive data migration engine for eyecare analytics"""
    
    def __init__(self):
        """Initialize migration engine"""
        # Source database config
        self.source_config = ConnectionConfig(
            db_type='sqlserver',
            host=os.getenv('SOURCE_DB_HOST'),
            port=int(os.getenv('SOURCE_DB_PORT', 1433)),
            database=os.getenv('SOURCE_DB_NAME'),
            username=os.getenv('SOURCE_DB_USER'),
            password=os.getenv('SOURCE_DB_PASSWORD')
        )
        
        self.db_connector = DatabaseConnector()
        self.migration_stats = {
            'total_tables': 0,
            'successful_migrations': 0,
            'failed_migrations': 0,
            'total_rows_migrated': 0,
            'start_time': None,
            'end_time': None,
            'errors': []
        }
        
        # Load migration plan
        self.migration_plan = self._load_migration_plan()
        
    def _load_migration_plan(self) -> List[Dict]:
        """Load the migration plan from CSV"""
        plan_path = project_root / "docs" / "migration" / "eyecare_migration_plan.csv"
        if plan_path.exists():
            df = pd.read_csv(plan_path)
            return df.to_dict('records')
        else:
            console.print("❌ Migration plan not found. Run table discovery first.", style="bold red")
            return []
    
    def migrate_priority_tables(self, top_n: int = 20) -> Dict:
        """Migrate top N priority tables"""
        console.print(Panel(f"🚀 Starting Migration of Top {top_n} Priority Tables", style="bold green"))
        
        if not self.migration_plan:
            console.print("❌ No migration plan available", style="bold red")
            return self.migration_stats
        
        priority_tables = self.migration_plan[:top_n]
        return self._execute_migration(priority_tables, f"Priority Top {top_n}")
    
    def migrate_all_tables(self) -> Dict:
        """Migrate all tables in the migration plan"""
        console.print(Panel("🚀 Starting Full Migration of All Tables", style="bold green"))
        
        if not self.migration_plan:
            console.print("❌ No migration plan available", style="bold red")
            return self.migration_stats
        
        return self._execute_migration(self.migration_plan, "Full Migration")
    
    def migrate_by_domain(self, domain: str) -> Dict:
        """Migrate tables by specific business domain"""
        console.print(Panel(f"🚀 Starting Migration of {domain.title()} Tables", style="bold green"))
        
        domain_tables = [t for t in self.migration_plan if t.get('domain', '').lower() == domain.lower()]
        
        if not domain_tables:
            console.print(f"❌ No tables found for domain: {domain}", style="bold red")
            return self.migration_stats
        
        return self._execute_migration(domain_tables, f"{domain.title()} Domain")
    
    def _execute_migration(self, tables: List[Dict], migration_type: str) -> Dict:
        """Execute migration for a list of tables"""
        self.migration_stats['start_time'] = datetime.now()
        self.migration_stats['total_tables'] = len(tables)
        
        # Create connections
        source_engine = self.db_connector.create_source_engine(self.source_config)
        snowflake_conn = self.db_connector.create_snowflake_connection()
        
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeElapsedColumn(),
                console=console
            ) as progress:
                
                main_task = progress.add_task(f"[cyan]{migration_type}", total=len(tables))
                
                for i, table_info in enumerate(tables):
                    table_name = table_info['TABLE_NAME']
                    schema_name = table_info['TABLE_SCHEMA']
                    row_count = table_info.get('ROW_COUNT', 0)
                    
                    progress.update(main_task, description=f"[cyan]Migrating {schema_name}.{table_name}")
                    
                    try:
                        # Migrate single table
                        migrated_rows = self._migrate_single_table(
                            source_engine, snowflake_conn, schema_name, table_name, row_count
                        )
                        
                        self.migration_stats['successful_migrations'] += 1
                        self.migration_stats['total_rows_migrated'] += migrated_rows
                        
                        console.print(f"✅ {schema_name}.{table_name}: {migrated_rows:,} rows", style="green")
                        
                    except Exception as e:
                        self.migration_stats['failed_migrations'] += 1
                        error_msg = f"❌ {schema_name}.{table_name}: {str(e)}"
                        self.migration_stats['errors'].append(error_msg)
                        console.print(error_msg, style="red")
                        logger.error(f"Migration failed for {schema_name}.{table_name}: {e}")
                    
                    progress.advance(main_task)
                    
                    # Small delay to prevent overwhelming the databases
                    time.sleep(0.1)
        
        finally:
            # Close connections
            source_engine.dispose()
            snowflake_conn.close()
            
            self.migration_stats['end_time'] = datetime.now()
        
        # Display final results
        self._display_migration_results(migration_type)
        
        return self.migration_stats
    
    def _migrate_single_table(self, source_engine, snowflake_conn, schema_name: str, 
                            table_name: str, expected_rows: int) -> int:
        """Migrate a single table from source to Snowflake"""
        
        # Step 1: Extract data from source
        with source_engine.connect() as source_conn:
            # Get table structure first
            columns_query = f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
            """
            
            columns_df = pd.read_sql(columns_query, source_conn)
            
            if columns_df.empty:
                raise Exception(f"No columns found for table {schema_name}.{table_name}")
            
            # Extract table data
            data_query = f'SELECT * FROM [{schema_name}].[{table_name}]'
            
            # Handle large tables with chunking
            if expected_rows > 100000:
                # For large tables, use chunking
                chunk_size = 10000
                all_chunks = []
                
                for chunk in pd.read_sql(data_query, source_conn, chunksize=chunk_size):
                    all_chunks.append(chunk)
                
                if all_chunks:
                    data_df = pd.concat(all_chunks, ignore_index=True)
                else:
                    data_df = pd.DataFrame()
            else:
                # For smaller tables, load all at once
                data_df = pd.read_sql(data_query, source_conn)
        
        # Step 2: Create table in Snowflake RAW schema
        snowflake_table_name = f"{schema_name}_{table_name}".upper()
        
        # Generate CREATE TABLE statement
        create_table_sql = self._generate_snowflake_create_table(
            snowflake_table_name, columns_df
        )
        
        cursor = snowflake_conn.cursor()
        
        try:
            # Drop table if exists and create new one
            cursor.execute(f"DROP TABLE IF EXISTS EYECARE_ANALYTICS.RAW.{snowflake_table_name}")
            cursor.execute(create_table_sql)
            
            # Step 3: Load data into Snowflake
            if not data_df.empty:
                # Prepare data for Snowflake
                data_df = self._prepare_data_for_snowflake(data_df)
                
                # Use Snowflake's write_pandas for efficient loading
                from snowflake.connector.pandas_tools import write_pandas
                
                success, nchunks, nrows, _ = write_pandas(
                    conn=snowflake_conn,
                    df=data_df,
                    table_name=snowflake_table_name,
                    database='EYECARE_ANALYTICS',
                    schema='RAW',
                    auto_create_table=False,  # We already created it
                    overwrite=True
                )
                
                if success:
                    return nrows
                else:
                    raise Exception("Failed to write data to Snowflake")
            else:
                return 0
                
        finally:
            cursor.close()
    
    def _generate_snowflake_create_table(self, table_name: str, columns_df: pd.DataFrame) -> str:
        """Generate CREATE TABLE statement for Snowflake"""
        
        # SQL Server to Snowflake type mapping
        type_mapping = {
            'int': 'INTEGER',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'tinyint': 'SMALLINT',
            'bit': 'BOOLEAN',
            'decimal': 'DECIMAL',
            'numeric': 'DECIMAL',
            'money': 'DECIMAL(19,4)',
            'smallmoney': 'DECIMAL(10,4)',
            'float': 'FLOAT',
            'real': 'REAL',
            'datetime': 'TIMESTAMP',
            'datetime2': 'TIMESTAMP',
            'smalldatetime': 'TIMESTAMP',
            'date': 'DATE',
            'time': 'TIME',
            'char': 'CHAR',
            'varchar': 'VARCHAR',
            'text': 'TEXT',
            'nchar': 'CHAR',
            'nvarchar': 'VARCHAR',
            'ntext': 'TEXT',
            'binary': 'BINARY',
            'varbinary': 'VARBINARY',
            'image': 'VARBINARY',
            'uniqueidentifier': 'VARCHAR(36)',
            'xml': 'TEXT'
        }
        
        columns = []
        for _, row in columns_df.iterrows():
            col_name = row['COLUMN_NAME']
            data_type = row['DATA_TYPE'].lower()
            max_length = row['CHARACTER_MAXIMUM_LENGTH']
            is_nullable = row['IS_NULLABLE'] == 'YES'
            
            # Map SQL Server type to Snowflake type
            if data_type in type_mapping:
                sf_type = type_mapping[data_type]
            else:
                sf_type = 'VARCHAR(16777216)'  # Default to large VARCHAR
            
            # Handle length for string types
            if data_type in ['varchar', 'nvarchar', 'char', 'nchar'] and max_length:
                if max_length > 0:
                    sf_type = f"VARCHAR({min(max_length, 16777216)})"
                else:
                    sf_type = "VARCHAR(16777216)"
            
            # Add nullable constraint
            nullable = "" if is_nullable else " NOT NULL"
            
            columns.append(f'    "{col_name}" {sf_type}{nullable}')
        
        create_sql = f"""
        CREATE TABLE EYECARE_ANALYTICS.RAW.{table_name} (
{','.join(columns)}
        )
        """
        
        return create_sql
    
    def _prepare_data_for_snowflake(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare DataFrame for Snowflake ingestion"""
        # Handle common data type issues
        for col in df.columns:
            # Convert datetime columns
            if df[col].dtype == 'object':
                # Try to convert to datetime if it looks like a date
                try:
                    if df[col].astype(str).str.match(r'\d{4}-\d{2}-\d{2}').any():
                        df[col] = pd.to_datetime(df[col], errors='ignore')
                except:
                    pass
            
            # Handle NaN values
            if df[col].dtype == 'object':
                df[col] = df[col].fillna('')
            else:
                df[col] = df[col].fillna(0)
        
        return df
    
    def _display_migration_results(self, migration_type: str):
        """Display comprehensive migration results"""
        duration = self.migration_stats['end_time'] - self.migration_stats['start_time']
        
        results_table = Table(title=f"{migration_type} - Migration Results")
        results_table.add_column("Metric", style="cyan")
        results_table.add_column("Value", style="green")
        
        results_table.add_row("Total Tables", str(self.migration_stats['total_tables']))
        results_table.add_row("Successful Migrations", str(self.migration_stats['successful_migrations']))
        results_table.add_row("Failed Migrations", str(self.migration_stats['failed_migrations']))
        results_table.add_row("Total Rows Migrated", f"{self.migration_stats['total_rows_migrated']:,}")
        results_table.add_row("Duration", str(duration).split('.')[0])
        results_table.add_row("Success Rate", f"{(self.migration_stats['successful_migrations']/self.migration_stats['total_tables']*100):.1f}%")
        
        console.print(results_table)
        
        # Show errors if any
        if self.migration_stats['errors']:
            console.print(Panel("❌ Migration Errors", style="red"))
            for error in self.migration_stats['errors'][:10]:  # Show first 10 errors
                console.print(f"  {error}", style="red")
            
            if len(self.migration_stats['errors']) > 10:
                console.print(f"  ... and {len(self.migration_stats['errors']) - 10} more errors", style="red")

def main():
    """Main execution function"""
    console.print(Panel("🏥 Eyecare Data Migration Engine", style="bold green"))
    
    try:
        migration_engine = DataMigrationEngine()
        
        # Phase 1: Migrate top 20 priority tables
        console.print("\n🎯 PHASE 1: Priority Tables Migration")
        priority_stats = migration_engine.migrate_priority_tables(20)
        
        # Brief pause between phases
        console.print("\n⏸️  Pausing 5 seconds before full migration...")
        time.sleep(5)
        
        # Phase 2: Migrate all remaining tables
        console.print("\n🚀 PHASE 2: Full Migration")
        full_stats = migration_engine.migrate_all_tables()
        
        console.print(Panel(
            f"🎉 Complete Migration Finished!\n\n"
            f"• Priority Tables: {priority_stats['successful_migrations']}/{priority_stats['total_tables']} successful\n"
            f"• Full Migration: {full_stats['successful_migrations']}/{full_stats['total_tables']} successful\n"
            f"• Total Rows Migrated: {full_stats['total_rows_migrated']:,}\n"
            f"• Your Snowflake RAW layer is ready! ✅",
            style="bold green"
        ))
        
    except Exception as e:
        console.print(f"❌ Migration failed: {str(e)}", style="bold red")
        logger.error(f"Migration failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
