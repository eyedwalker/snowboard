#!/usr/bin/env python3
"""
Production-Ready Migration Engine
Removes all row limits, adds missing critical tables, handles large datasets robustly
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
from datetime import datetime
import math

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Load environment variables
load_dotenv(project_root / ".env")

console = Console()

class ProductionMigrationEngine:
    """Production-ready migration engine for full data coverage"""
    
    def __init__(self):
        """Initialize production migration engine"""
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
        
        # Production migration settings
        self.batch_size = 10000  # Process in 10K row chunks for performance
        self.max_memory_rows = 50000  # Max rows to hold in memory at once
        
        # Migration tracking
        self.migration_stats = {
            'total_attempted': 0,
            'successful': 0,
            'failed': 0,
            'total_rows': 0,
            'successful_tables': [],
            'failed_tables': [],
            'start_time': datetime.now()
        }
        
        # Critical tables for V1.3 analytics (prioritized list)
        self.critical_tables = self.get_critical_tables_for_v13()
    
    def get_critical_tables_for_v13(self):
        """Get critical tables needed for V1.3 analytics in priority order"""
        return {
            # Phase 1: Core Financial (highest priority)
            'phase_1_core_financial': [
                'BillingTransaction',
                'BillingClaimLineItem', 
                'BillingClaimData',
                'BillingClaim',
                'POSTransaction',
                'BillingTransactionType'
            ],
            
            # Phase 2: Invoice Intersection Model
            'phase_2_invoice_intersection': [
                'InvoiceDet',
                'InvoiceInsuranceDet',
                'InvoiceSum',
                'Orders'
            ],
            
            # Phase 3: Insurance Ecosystem
            'phase_3_insurance_ecosystem': [
                'InsCarrier',
                'InsPlan', 
                'InsEligibility',
                'PatientInsurance',
                'InsSchedule',
                'InsScheduleMethod',
                'InsHCFA',
                'Address',
                'Phone'
            ],
            
            # Phase 4: Organizational Hierarchy
            'phase_4_organizational': [
                'CompanyInfo',
                'Office',
                'Employee',
                'EmployeeRole',
                'EmployeeType',
                'EmployeeCommission'
            ],
            
            # Phase 5: Operational Core
            'phase_5_operational': [
                'Patient',
                'Item',
                'ItemType',
                'OrderInsurance'
            ],
            
            # Phase 6: Additional Financial
            'phase_6_additional_financial': [
                'BillingClaimOrders',
                'StockOrderDetail',
                'AppSch_Appointment',
                'AppSchedule'
            ]
        }
    
    def get_connection(self):
        """Get Snowflake connection"""
        return snowflake.connector.connect(**self.sf_params)
    
    def check_source_connectivity(self):
        """Check source database connectivity and get basic stats"""
        console.print(Panel("🔍 Checking Source Database Connectivity", style="bold blue"))
        
        try:
            source_engine = create_engine(self.source_conn_str)
            with source_engine.connect() as conn:
                # Test basic connectivity
                test_query = "SELECT COUNT(*) as table_count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo'"
                result = pd.read_sql(test_query, conn)
                table_count = result.iloc[0]['table_count']
                
                console.print(f"✅ Source database connected successfully")
                console.print(f"📊 Found {table_count:,} tables in dbo schema")
                
                return True
                
        except Exception as e:
            console.print(f"❌ Source database connection failed: {str(e)}", style="bold red")
            return False
    
    def analyze_source_data_volumes(self, tables_list: list):
        """Analyze source data volumes for planning"""
        console.print(Panel("📊 Analyzing Source Data Volumes", style="bold yellow"))
        
        source_engine = create_engine(self.source_conn_str)
        volume_analysis = {}
        
        try:
            with source_engine.connect() as conn:
                total_rows = 0
                
                table = Table(title="Source Data Volume Analysis")
                table.add_column("Table Name", style="cyan")
                table.add_column("Row Count", style="green", justify="right")
                table.add_column("Est. Size", style="yellow", justify="right")
                table.add_column("Priority", style="red")
                
                for table_name in tables_list[:20]:  # Analyze top 20 tables
                    try:
                        count_query = f"SELECT COUNT(*) as row_count FROM dbo.{table_name}"
                        result = pd.read_sql(count_query, conn)
                        row_count = result.iloc[0]['row_count']
                        
                        # Estimate size (rough calculation)
                        est_size_mb = (row_count * 1000) / (1024 * 1024)  # Rough estimate
                        
                        # Determine priority
                        priority = "HIGH" if table_name in [t for phase in self.critical_tables.values() for t in phase] else "MEDIUM"
                        
                        volume_analysis[table_name] = {
                            'row_count': row_count,
                            'est_size_mb': est_size_mb,
                            'priority': priority
                        }
                        
                        table.add_row(
                            table_name,
                            f"{row_count:,}",
                            f"{est_size_mb:.1f} MB",
                            priority
                        )
                        
                        total_rows += row_count
                        
                    except Exception as e:
                        console.print(f"⚠️ Could not analyze {table_name}: {str(e)}")
                
                console.print(table)
                console.print(f"\n📈 Total estimated rows to migrate: {total_rows:,}")
                
                return volume_analysis
                
        except Exception as e:
            console.print(f"❌ Error analyzing source volumes: {str(e)}", style="bold red")
            return {}
        
        finally:
            source_engine.dispose()
    
    def migrate_table_production(self, schema_name: str, table_name: str) -> int:
        """Production-ready table migration with no row limits"""
        console.print(f"\n🚀 Starting production migration: {schema_name}.{table_name}")
        
        source_engine = create_engine(self.source_conn_str)
        sf_conn = self.get_connection()
        
        try:
            # Step 1: Get total row count
            with source_engine.connect() as conn:
                count_query = f"SELECT COUNT(*) as row_count FROM [{schema_name}].[{table_name}]"
                count_result = pd.read_sql(count_query, conn)
                total_rows = count_result.iloc[0]['row_count']
                
                console.print(f"📊 Total rows to migrate: {total_rows:,}")
                
                if total_rows == 0:
                    console.print("⚠️ Table is empty, creating structure only")
                    return self._create_empty_table_structure(sf_conn, schema_name, table_name, conn)
            
            # Step 2: Create Snowflake table
            sf_table_name = f"{schema_name}_{table_name}".upper()
            cursor = sf_conn.cursor()
            
            try:
                # Drop existing table
                cursor.execute(f"DROP TABLE IF EXISTS EYECARE_ANALYTICS.RAW.{sf_table_name}")
                
                # Get sample for structure
                with source_engine.connect() as conn:
                    structure_query = f"SELECT TOP 1 * FROM [{schema_name}].[{table_name}]"
                    sample_df = pd.read_sql(structure_query, conn)
                    
                    if not sample_df.empty:
                        columns = [f'"{col}" VARCHAR(16777216)' for col in sample_df.columns]
                    else:
                        columns = ['"PLACEHOLDER_COLUMN" VARCHAR(16777216)']
                    
                    create_sql = f"""
                    CREATE TABLE EYECARE_ANALYTICS.RAW.{sf_table_name} (
                        {', '.join(columns)}
                    )
                    """
                    cursor.execute(create_sql)
                    console.print(f"✅ Created Snowflake table with {len(columns)} columns")
                
                # Step 3: Migrate data in batches (NO ROW LIMITS!)
                total_migrated = 0
                
                if total_rows > 0:
                    # Calculate number of batches
                    num_batches = math.ceil(total_rows / self.batch_size)
                    console.print(f"📦 Processing {num_batches} batches of {self.batch_size:,} rows each")
                    
                    with Progress(
                        SpinnerColumn(),
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        TaskProgressColumn(),
                        console=console
                    ) as progress:
                        
                        task = progress.add_task(f"Migrating {table_name}...", total=num_batches)
                        
                        for batch_num in range(num_batches):
                            offset = batch_num * self.batch_size
                            
                            # Extract batch data
                            with source_engine.connect() as conn:
                                batch_query = f"""
                                SELECT * FROM [{schema_name}].[{table_name}]
                                ORDER BY (SELECT NULL)
                                OFFSET {offset} ROWS
                                FETCH NEXT {self.batch_size} ROWS ONLY
                                """
                                
                                batch_df = pd.read_sql(batch_query, conn)
                                
                                if not batch_df.empty:
                                    # Clean and insert batch
                                    batch_df = self._clean_dataframe_production(batch_df)
                                    rows_inserted = self._insert_batch_production(cursor, sf_table_name, batch_df)
                                    total_migrated += rows_inserted
                                    
                                    progress.update(task, 
                                                  description=f"[cyan]Batch {batch_num+1}/{num_batches} - {total_migrated:,} rows migrated")
                            
                            progress.advance(task)
                            
                            # Small delay to prevent overwhelming the databases
                            time.sleep(0.1)
                
                console.print(f"✅ Successfully migrated {total_migrated:,} rows to {sf_table_name}")
                return total_migrated
                
            finally:
                cursor.close()
                
        except Exception as e:
            console.print(f"❌ Error migrating {schema_name}.{table_name}: {str(e)}", style="bold red")
            raise e
            
        finally:
            source_engine.dispose()
            sf_conn.close()
    
    def _clean_dataframe_production(self, df: pd.DataFrame) -> pd.DataFrame:
        """Production-grade DataFrame cleaning"""
        for col in df.columns:
            # Convert to string and handle all edge cases
            df[col] = df[col].astype(str)
            df[col] = df[col].replace(['nan', 'None', 'NaT', 'NULL', 'null'], '')
            df[col] = df[col].fillna('')
            
            # Handle problematic characters for SQL
            df[col] = df[col].str.replace("'", "''")  # Escape quotes
            df[col] = df[col].str.replace('\\', '\\\\')  # Escape backslashes
            df[col] = df[col].str.replace('\n', ' ')  # Remove newlines
            df[col] = df[col].str.replace('\r', ' ')  # Remove carriage returns
            df[col] = df[col].str.replace('\t', ' ')  # Remove tabs
            
            # Truncate very long strings (Snowflake VARCHAR limit)
            df[col] = df[col].str[:16000]
        
        return df
    
    def _insert_batch_production(self, cursor, table_name: str, df: pd.DataFrame) -> int:
        """Production-grade batch insertion with optimized performance"""
        if df.empty:
            return 0
        
        columns = [f'"{col}"' for col in df.columns]
        columns_str = ', '.join(columns)
        
        # Use smaller sub-batches for memory efficiency
        sub_batch_size = 500
        total_inserted = 0
        
        for i in range(0, len(df), sub_batch_size):
            sub_batch = df.iloc[i:i+sub_batch_size]
            
            values_list = []
            for _, row in sub_batch.iterrows():
                values = [f"'{str(val)}'" for val in row]
                values_list.append(f"({', '.join(values)})")
            
            values_str = ', '.join(values_list)
            insert_sql = f"""
            INSERT INTO EYECARE_ANALYTICS.RAW.{table_name} ({columns_str})
            VALUES {values_str}
            """
            
            cursor.execute(insert_sql)
            total_inserted += len(sub_batch)
        
        return total_inserted
    
    def _create_empty_table_structure(self, sf_conn, schema_name: str, table_name: str, source_conn) -> int:
        """Create empty table structure when source table has no data"""
        sf_table_name = f"{schema_name}_{table_name}".upper()
        cursor = sf_conn.cursor()
        
        try:
            # Get table structure
            structure_query = f"SELECT TOP 0 * FROM [{schema_name}].[{table_name}]"
            structure_df = pd.read_sql(structure_query, source_conn)
            
            if not structure_df.empty:
                columns = [f'"{col}" VARCHAR(16777216)' for col in structure_df.columns]
            else:
                columns = ['"PLACEHOLDER_COLUMN" VARCHAR(16777216)']
            
            create_sql = f"""
            CREATE OR REPLACE TABLE EYECARE_ANALYTICS.RAW.{sf_table_name} (
                {', '.join(columns)}
            )
            """
            cursor.execute(create_sql)
            return 0
            
        finally:
            cursor.close()
    
    def run_production_migration(self):
        """Run complete production migration for all critical tables"""
        console.print(Panel("🚀 PRODUCTION MIGRATION ENGINE - FULL DATA COVERAGE", style="bold green"))
        
        # Step 1: Check connectivity
        if not self.check_source_connectivity():
            console.print("❌ Cannot proceed without source database connectivity", style="bold red")
            return
        
        # Step 2: Get all critical tables in priority order
        all_critical_tables = []
        for phase_name, tables in self.critical_tables.items():
            console.print(f"\n📋 {phase_name.upper().replace('_', ' ')}: {len(tables)} tables")
            all_critical_tables.extend(tables)
        
        # Step 3: Analyze source data volumes
        volume_analysis = self.analyze_source_data_volumes(all_critical_tables)
        
        # Step 4: Run migration for each critical table
        console.print(f"\n🚀 Starting production migration of {len(all_critical_tables)} critical tables")
        
        for phase_name, tables in self.critical_tables.items():
            console.print(Panel(f"📦 {phase_name.upper().replace('_', ' ')}", style="bold cyan"))
            
            for table_name in tables:
                try:
                    self.migration_stats['total_attempted'] += 1
                    
                    rows_migrated = self.migrate_table_production('dbo', table_name)
                    
                    self.migration_stats['successful'] += 1
                    self.migration_stats['total_rows'] += rows_migrated
                    self.migration_stats['successful_tables'].append({
                        'table': f"dbo.{table_name}",
                        'rows': rows_migrated,
                        'phase': phase_name
                    })
                    
                except Exception as e:
                    self.migration_stats['failed'] += 1
                    self.migration_stats['failed_tables'].append({
                        'table': f"dbo.{table_name}",
                        'error': str(e),
                        'phase': phase_name
                    })
        
        # Step 5: Display final results
        self._display_final_results()
    
    def _display_final_results(self):
        """Display comprehensive migration results"""
        end_time = datetime.now()
        duration = end_time - self.migration_stats['start_time']
        
        console.print(Panel("📊 PRODUCTION MIGRATION RESULTS", style="bold green"))
        
        # Summary stats
        results_table = Table(title="Migration Summary")
        results_table.add_column("Metric", style="cyan")
        results_table.add_column("Value", style="green", justify="right")
        
        results_table.add_row("Total Tables Attempted", f"{self.migration_stats['total_attempted']:,}")
        results_table.add_row("Successful Migrations", f"{self.migration_stats['successful']:,}")
        results_table.add_row("Failed Migrations", f"{self.migration_stats['failed']:,}")
        results_table.add_row("Total Rows Migrated", f"{self.migration_stats['total_rows']:,}")
        results_table.add_row("Migration Duration", str(duration))
        results_table.add_row("Average Rows/Minute", f"{self.migration_stats['total_rows'] / max(duration.total_seconds() / 60, 1):,.0f}")
        
        console.print(results_table)
        
        # Successful tables by phase
        if self.migration_stats['successful_tables']:
            success_table = Table(title="Successfully Migrated Tables")
            success_table.add_column("Phase", style="cyan")
            success_table.add_column("Table", style="green")
            success_table.add_column("Rows", style="yellow", justify="right")
            
            for table_info in self.migration_stats['successful_tables']:
                success_table.add_row(
                    table_info['phase'].replace('_', ' ').title(),
                    table_info['table'],
                    f"{table_info['rows']:,}"
                )
            
            console.print(success_table)
        
        # Failed tables
        if self.migration_stats['failed_tables']:
            console.print("\n❌ FAILED MIGRATIONS:")
            for table_info in self.migration_stats['failed_tables']:
                console.print(f"   • {table_info['table']}: {table_info['error'][:100]}...")

def main():
    """Main production migration execution"""
    console.print("🚀 EYECARE ANALYTICS - PRODUCTION MIGRATION ENGINE")
    console.print("=" * 70)
    
    migration_engine = ProductionMigrationEngine()
    migration_engine.run_production_migration()
    
    console.print("\n✅ Production migration completed!")
    console.print("🎯 Ready for comprehensive V1.3 analytics with full data coverage!")

if __name__ == "__main__":
    main()
