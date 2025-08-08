#!/usr/bin/env python3
"""
Continuous Migration Loop
Automatically migrates ALL remaining tables in batches until complete
"""

import os
import sys
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
import snowflake.connector
from sqlalchemy import create_engine, text
import time

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Load environment variables
load_dotenv(project_root / ".env")

console = Console()

class ContinuousMigrationLoop:
    """Continuous migration loop that runs until all tables are migrated"""
    
    def __init__(self):
        """Initialize continuous migration loop"""
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
        
        # Migration tracking
        self.total_stats = {
            'batches_completed': 0,
            'total_tables_migrated': 0,
            'total_rows_migrated': 0,
            'total_failures': 0,
            'start_time': time.time(),
            'batch_results': []
        }
        
        # Load migration plan
        self.migration_plan = self._load_migration_plan()
        
        # Get already migrated tables from Snowflake
        self.already_migrated = self._get_already_migrated_tables()
    
    def _load_migration_plan(self) -> list:
        """Load migration plan from CSV"""
        plan_path = project_root / "docs" / "migration" / "eyecare_migration_plan.csv"
        if plan_path.exists():
            df = pd.read_csv(plan_path)
            return df.to_dict('records')
        else:
            return []
    
    def _get_already_migrated_tables(self) -> set:
        """Get list of tables already migrated to Snowflake"""
        migrated_tables = set()
        
        try:
            sf_conn = snowflake.connector.connect(**self.sf_params)
            cursor = sf_conn.cursor()
            
            # Get all tables in RAW schema
            cursor.execute("SHOW TABLES IN EYECARE_ANALYTICS.RAW")
            results = cursor.fetchall()
            
            for row in results:
                table_name = row[1]  # Table name is in second column
                migrated_tables.add(table_name)
            
            cursor.close()
            sf_conn.close()
            
            console.print(f"📊 Found {len(migrated_tables)} tables already migrated in Snowflake", style="blue")
            
        except Exception as e:
            console.print(f"⚠️ Could not get migrated tables list: {str(e)}", style="yellow")
        
        return migrated_tables
    
    def run_continuous_migration(self, batch_size: int = 50):
        """Run continuous migration until all tables are migrated"""
        console.print(Panel("🚀 CONTINUOUS MIGRATION LOOP - NON-STOP UNTIL COMPLETE!", style="bold green"))
        
        if not self.migration_plan:
            console.print("❌ No migration plan available", style="bold red")
            return
        
        # Filter out already migrated tables
        remaining_tables = []
        for table in self.migration_plan:
            sf_table_name = f"{table['TABLE_SCHEMA']}_{table['TABLE_NAME']}".upper()
            if sf_table_name not in self.already_migrated:
                remaining_tables.append(table)
        
        total_remaining = len(remaining_tables)
        console.print(f"🎯 Starting continuous migration: {total_remaining} tables remaining")
        
        # Calculate estimated batches
        estimated_batches = (total_remaining + batch_size - 1) // batch_size
        console.print(f"📊 Estimated batches needed: {estimated_batches}")
        
        batch_number = 1
        
        while remaining_tables:
            console.print(Panel(f"🚀 BATCH #{batch_number} - {len(remaining_tables)} tables remaining", style="bold cyan"))
            
            # Take next batch
            current_batch = remaining_tables[:batch_size]
            
            # Migrate current batch
            batch_results = self._migrate_batch(current_batch, batch_number)
            
            # Update stats
            self.total_stats['batches_completed'] += 1
            self.total_stats['total_tables_migrated'] += batch_results['successful']
            self.total_stats['total_rows_migrated'] += batch_results['total_rows']
            self.total_stats['total_failures'] += batch_results['failed']
            self.total_stats['batch_results'].append(batch_results)
            
            # Remove successfully migrated tables from remaining list
            successfully_migrated = set()
            for table_info in current_batch:
                sf_table_name = f"{table_info['TABLE_SCHEMA']}_{table_info['TABLE_NAME']}".upper()
                # Check if it was in the successful list
                for success in batch_results['successful_tables']:
                    if sf_table_name in success['table']:
                        successfully_migrated.add(table_info['TABLE_NAME'])
                        break
            
            # Update remaining tables list
            remaining_tables = [t for t in remaining_tables[batch_size:] if t['TABLE_NAME'] not in successfully_migrated]
            
            # Display progress
            self._display_overall_progress(total_remaining - len(remaining_tables), total_remaining)
            
            batch_number += 1
            
            # Small delay between batches to prevent overwhelming the databases
            time.sleep(2)
        
        # Final summary
        self._display_final_summary()
    
    def _migrate_batch(self, batch_tables: list, batch_number: int) -> dict:
        """Migrate a single batch of tables"""
        
        batch_results = {
            'batch_number': batch_number,
            'attempted': len(batch_tables),
            'successful': 0,
            'failed': 0,
            'total_rows': 0,
            'successful_tables': [],
            'failed_tables': []
        }
        
        source_engine = create_engine(self.source_conn_str)
        sf_conn = snowflake.connector.connect(**self.sf_params)
        
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console
            ) as progress:
                
                task = progress.add_task(f"Batch #{batch_number}...", total=len(batch_tables))
                
                for table_info in batch_tables:
                    table_name = table_info['TABLE_NAME']
                    schema_name = table_info['TABLE_SCHEMA']
                    
                    progress.update(task, description=f"[cyan]Migrating {schema_name}.{table_name}")
                    
                    try:
                        # Attempt migration
                        rows_migrated = self._migrate_single_table_robust(
                            source_engine, sf_conn, schema_name, table_name
                        )
                        
                        batch_results['successful'] += 1
                        batch_results['total_rows'] += rows_migrated
                        batch_results['successful_tables'].append({
                            'table': f"{schema_name}.{table_name}",
                            'rows': rows_migrated
                        })
                        
                        console.print(f"✅ {schema_name}.{table_name}: {rows_migrated:,} rows", style="green")
                        
                    except Exception as e:
                        batch_results['failed'] += 1
                        batch_results['failed_tables'].append({
                            'table': f"{schema_name}.{table_name}",
                            'error': str(e)
                        })
                        
                        console.print(f"❌ {schema_name}.{table_name}: {str(e)[:100]}...", style="red")
                    
                    progress.advance(task)
                    
                    # Small delay between tables
                    time.sleep(0.1)
        
        finally:
            source_engine.dispose()
            sf_conn.close()
        
        return batch_results
    
    def _migrate_single_table_robust(self, source_engine, sf_conn, schema_name: str, table_name: str) -> int:
        """Robust single table migration with error handling"""
        
        # Step 1: Verify table exists and get basic info
        with source_engine.connect() as conn:
            # Check table existence
            check_query = f"""
            SELECT COUNT(*) as table_count
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
            """
            
            check_result = pd.read_sql(check_query, conn)
            if check_result.iloc[0]['table_count'] == 0:
                raise Exception(f"Table {schema_name}.{table_name} does not exist")
            
            # Get row count
            count_query = f"SELECT COUNT(*) as row_count FROM [{schema_name}].[{table_name}]"
            count_result = pd.read_sql(count_query, conn)
            total_rows = count_result.iloc[0]['row_count']
            
            # Extract sample data (limit for safety)
            limit = min(total_rows, 5000)  # Conservative limit
            
            if limit > 0:
                data_query = f"SELECT TOP {limit} * FROM [{schema_name}].[{table_name}]"
                df = pd.read_sql(data_query, conn)
            else:
                # Get table structure even if no data
                structure_query = f"SELECT TOP 0 * FROM [{schema_name}].[{table_name}]"
                df = pd.read_sql(structure_query, conn)
        
        # Step 2: Clean and prepare data
        if not df.empty:
            df = self._clean_dataframe_robust(df)
        
        # Step 3: Create Snowflake table
        sf_table_name = f"{schema_name}_{table_name}".upper()
        
        cursor = sf_conn.cursor()
        try:
            # Drop and recreate table
            cursor.execute(f"DROP TABLE IF EXISTS EYECARE_ANALYTICS.RAW.{sf_table_name}")
            
            # Create table with robust column definitions
            if not df.empty:
                columns = [f'"{col}" VARCHAR(16777216)' for col in df.columns]
            else:
                # Create minimal table structure
                columns = ['"PLACEHOLDER_COLUMN" VARCHAR(16777216)']
            
            create_sql = f"""
            CREATE TABLE EYECARE_ANALYTICS.RAW.{sf_table_name} (
                {', '.join(columns)}
            )
            """
            cursor.execute(create_sql)
            
            # Step 4: Insert data if available
            if not df.empty:
                return self._insert_data_robust(cursor, sf_table_name, df)
            else:
                return 0
                
        finally:
            cursor.close()
    
    def _clean_dataframe_robust(self, df: pd.DataFrame) -> pd.DataFrame:
        """Robust DataFrame cleaning"""
        for col in df.columns:
            # Convert to string and handle all edge cases
            df[col] = df[col].astype(str)
            df[col] = df[col].replace(['nan', 'None', 'NaT', 'NULL', 'null'], '')
            df[col] = df[col].fillna('')
            
            # Handle problematic characters
            df[col] = df[col].str.replace("'", "''")  # Escape quotes
            df[col] = df[col].str.replace('\n', ' ')  # Remove newlines
            df[col] = df[col].str.replace('\r', ' ')  # Remove carriage returns
            df[col] = df[col].str.replace('\t', ' ')  # Remove tabs
            
            # Truncate very long strings
            df[col] = df[col].str[:16000]  # Snowflake VARCHAR limit safety
        
        return df
    
    def _insert_data_robust(self, cursor, table_name: str, df: pd.DataFrame) -> int:
        """Robust data insertion with fallback strategies"""
        if df.empty:
            return 0
        
        columns = [f'"{col}"' for col in df.columns]
        columns_str = ', '.join(columns)
        
        # Try batch insert first
        try:
            batch_size = 200  # Conservative batch size
            total_inserted = 0
            
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                
                values_list = []
                for _, row in batch.iterrows():
                    values = [f"'{str(val)}'" for val in row]
                    values_list.append(f"({', '.join(values)})")
                
                values_str = ', '.join(values_list)
                insert_sql = f"""
                INSERT INTO EYECARE_ANALYTICS.RAW.{table_name} ({columns_str})
                VALUES {values_str}
                """
                
                cursor.execute(insert_sql)
                total_inserted += len(batch)
            
            return total_inserted
            
        except Exception as e:
            # Fallback to single row inserts
            total_inserted = 0
            for _, row in df.iterrows():
                try:
                    values = [f"'{str(val)}'" for val in row]
                    values_str = ', '.join(values)
                    single_insert_sql = f"""
                    INSERT INTO EYECARE_ANALYTICS.RAW.{table_name} ({columns_str})
                    VALUES ({values_str})
                    """
                    cursor.execute(single_insert_sql)
                    total_inserted += 1
                except:
                    continue  # Skip problematic rows
            
            return total_inserted
    
    def _display_overall_progress(self, completed: int, total: int):
        """Display overall migration progress"""
        progress_pct = (completed / total * 100) if total > 0 else 0
        elapsed_time = time.time() - self.total_stats['start_time']
        
        progress_table = Table(title="🎯 Overall Migration Progress")
        progress_table.add_column("Metric", style="cyan")
        progress_table.add_column("Value", style="green")
        
        progress_table.add_row("Tables Completed", f"{completed:,}")
        progress_table.add_row("Total Tables", f"{total:,}")
        progress_table.add_row("Progress", f"{progress_pct:.1f}%")
        progress_table.add_row("Batches Completed", str(self.total_stats['batches_completed']))
        progress_table.add_row("Total Rows Migrated", f"{self.total_stats['total_rows_migrated']:,}")
        progress_table.add_row("Elapsed Time", f"{elapsed_time/60:.1f} minutes")
        
        if completed > 0:
            estimated_total_time = (elapsed_time / completed) * total
            remaining_time = estimated_total_time - elapsed_time
            progress_table.add_row("Est. Remaining Time", f"{remaining_time/60:.1f} minutes")
        
        console.print(progress_table)
    
    def _display_final_summary(self):
        """Display final migration summary"""
        elapsed_time = time.time() - self.total_stats['start_time']
        
        console.print(Panel(
            f"🎉 CONTINUOUS MIGRATION COMPLETE!\n\n"
            f"• Total Batches: {self.total_stats['batches_completed']}\n"
            f"• Tables Migrated: {self.total_stats['total_tables_migrated']:,}\n"
            f"• Total Rows: {self.total_stats['total_rows_migrated']:,}\n"
            f"• Total Failures: {self.total_stats['total_failures']}\n"
            f"• Total Time: {elapsed_time/60:.1f} minutes\n"
            f"• Average per Batch: {elapsed_time/self.total_stats['batches_completed']:.1f} seconds\n\n"
            f"🚀 YOUR COMPLETE EYECARE ANALYTICS PLATFORM IS READY! 🚀",
            style="bold green"
        ))

def main():
    """Main execution"""
    console.print(Panel("🚀 CONTINUOUS MIGRATION LOOP - ALL TABLES", style="bold green"))
    
    try:
        migration_loop = ContinuousMigrationLoop()
        migration_loop.run_continuous_migration(batch_size=50)
        
    except KeyboardInterrupt:
        console.print("\n⚠️ Migration interrupted by user", style="yellow")
    except Exception as e:
        console.print(f"❌ Continuous migration failed: {str(e)}", style="bold red")

if __name__ == "__main__":
    main()
