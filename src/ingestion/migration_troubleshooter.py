#!/usr/bin/env python3
"""
Migration Troubleshooter
Fixes failed migrations: encoding errors, invalid object names, schema issues
"""

import os
import sys
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import snowflake.connector
from sqlalchemy import create_engine, text
import pymssql

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Load environment variables
load_dotenv(project_root / ".env")

console = Console()

class MigrationTroubleshooter:
    """Troubleshoot and fix failed migrations"""
    
    def __init__(self):
        """Initialize troubleshooter"""
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
        
        # Failed tables to investigate
        self.failed_tables = {
            'CompanyInfo': 'encoding_error',
            'EmployeeRole': 'invalid_object',
            'EmployeeType': 'invalid_object', 
            'EmployeeCommission': 'invalid_object',
            'AppSchedule': 'invalid_object'
        }
    
    def investigate_invalid_object_names(self):
        """Find actual table names for invalid object errors"""
        console.print(Panel("🔍 Investigating Invalid Object Names", style="bold yellow"))
        
        source_engine = create_engine(self.source_conn_str)
        
        try:
            with source_engine.connect() as conn:
                # Get all table names in database
                all_tables_query = """
                SELECT TABLE_SCHEMA, TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
                """
                
                all_tables = pd.read_sql(all_tables_query, conn)
                
                console.print(f"📊 Found {len(all_tables)} total tables in database")
                
                # Investigate each failed table
                for failed_table in ['EmployeeRole', 'EmployeeType', 'EmployeeCommission', 'AppSchedule']:
                    console.print(f"\n🔍 Investigating: {failed_table}")
                    
                    # Look for exact matches
                    exact_matches = all_tables[all_tables['TABLE_NAME'].str.upper() == failed_table.upper()]
                    
                    if not exact_matches.empty:
                        console.print("✅ Found exact matches:")
                        for _, row in exact_matches.iterrows():
                            console.print(f"   • {row['TABLE_SCHEMA']}.{row['TABLE_NAME']}")
                    else:
                        # Look for similar names
                        similar_matches = all_tables[all_tables['TABLE_NAME'].str.contains(failed_table[:6], case=False, na=False)]
                        
                        if not similar_matches.empty:
                            console.print("🔍 Found similar matches:")
                            for _, row in similar_matches.iterrows():
                                console.print(f"   • {row['TABLE_SCHEMA']}.{row['TABLE_NAME']}")
                        else:
                            # Look for employee-related tables
                            if 'Employee' in failed_table:
                                employee_tables = all_tables[all_tables['TABLE_NAME'].str.contains('Employee', case=False, na=False)]
                                console.print("👥 Found employee-related tables:")
                                for _, row in employee_tables.iterrows():
                                    console.print(f"   • {row['TABLE_SCHEMA']}.{row['TABLE_NAME']}")
                            
                            # Look for appointment/schedule tables
                            elif 'App' in failed_table or 'Schedule' in failed_table:
                                schedule_tables = all_tables[
                                    (all_tables['TABLE_NAME'].str.contains('App', case=False, na=False)) |
                                    (all_tables['TABLE_NAME'].str.contains('Schedule', case=False, na=False)) |
                                    (all_tables['TABLE_NAME'].str.contains('Appointment', case=False, na=False))
                                ]
                                console.print("📅 Found appointment/schedule-related tables:")
                                for _, row in schedule_tables.iterrows():
                                    console.print(f"   • {row['TABLE_SCHEMA']}.{row['TABLE_NAME']}")
                
                return all_tables
                
        except Exception as e:
            console.print(f"❌ Error investigating object names: {str(e)}", style="bold red")
            return pd.DataFrame()
        
        finally:
            source_engine.dispose()
    
    def investigate_encoding_error(self, table_name: str):
        """Investigate and fix encoding errors"""
        console.print(Panel(f"🔍 Investigating Encoding Error: {table_name}", style="bold yellow"))
        
        try:
            # Try different connection approaches
            conn = pymssql.connect(
                server=os.getenv('SOURCE_DB_HOST'),
                port=int(os.getenv('SOURCE_DB_PORT')),
                database=os.getenv('SOURCE_DB_NAME'),
                user=os.getenv('SOURCE_DB_USER'),
                password=os.getenv('SOURCE_DB_PASSWORD'),
                charset='utf8'
            )
            
            cursor = conn.cursor()
            
            # First, check if table exists
            cursor.execute(f"""
                SELECT COUNT(*) as table_count
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
            """)
            
            table_count = cursor.fetchone()[0]
            
            if table_count == 0:
                console.print(f"❌ Table {table_name} does not exist")
                return None
            
            # Get table structure
            cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
            """)
            
            columns = cursor.fetchall()
            
            console.print(f"📊 Table structure for {table_name}:")
            structure_table = Table()
            structure_table.add_column("Column", style="cyan")
            structure_table.add_column("Data Type", style="green")
            structure_table.add_column("Max Length", style="yellow")
            
            text_columns = []
            for col in columns:
                col_name, data_type, max_length = col
                structure_table.add_row(col_name, data_type, str(max_length) if max_length else "N/A")
                
                # Identify potentially problematic text columns
                if data_type.lower() in ['varchar', 'nvarchar', 'text', 'ntext', 'char', 'nchar']:
                    text_columns.append(col_name)
            
            console.print(structure_table)
            
            # Try to read a small sample with encoding handling
            console.print(f"\n🔍 Testing data extraction with encoding handling...")
            
            try:
                # Try reading just the first few rows
                cursor.execute(f"SELECT TOP 5 * FROM dbo.{table_name}")
                sample_data = cursor.fetchall()
                
                if sample_data:
                    console.print(f"✅ Successfully read {len(sample_data)} sample rows")
                    
                    # Check for problematic characters in text columns
                    for row in sample_data:
                        for i, value in enumerate(row):
                            if isinstance(value, (str, bytes)):
                                try:
                                    # Try to encode/decode to check for issues
                                    if isinstance(value, bytes):
                                        decoded = value.decode('utf-8', errors='replace')
                                        console.print(f"⚠️ Found bytes data in column {i}: {decoded[:50]}...")
                                    elif isinstance(value, str):
                                        encoded = value.encode('utf-8', errors='replace')
                                        console.print(f"✅ Text data in column {i}: {value[:50]}...")
                                except Exception as e:
                                    console.print(f"❌ Encoding issue in column {i}: {str(e)}")
                
            except Exception as e:
                console.print(f"❌ Error reading sample data: {str(e)}")
            
            cursor.close()
            conn.close()
            
            return {
                'table_exists': True,
                'columns': columns,
                'text_columns': text_columns,
                'sample_readable': True
            }
            
        except Exception as e:
            console.print(f"❌ Error investigating encoding: {str(e)}", style="bold red")
            return None
    
    def migrate_with_encoding_fix(self, table_name: str):
        """Migrate table with encoding error handling"""
        console.print(Panel(f"🔧 Migrating {table_name} with Encoding Fix", style="bold green"))
        
        try:
            # Connect with explicit encoding handling
            conn = pymssql.connect(
                server=os.getenv('SOURCE_DB_HOST'),
                port=int(os.getenv('SOURCE_DB_PORT')),
                database=os.getenv('SOURCE_DB_NAME'),
                user=os.getenv('SOURCE_DB_USER'),
                password=os.getenv('SOURCE_DB_PASSWORD'),
                charset='utf8'
            )
            
            cursor = conn.cursor()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
            total_rows = cursor.fetchone()[0]
            
            console.print(f"📊 Total rows to migrate: {total_rows:,}")
            
            if total_rows == 0:
                console.print("⚠️ Table is empty")
                return 0
            
            # Read data with encoding error handling
            cursor.execute(f"SELECT * FROM dbo.{table_name}")
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch data with error handling
            rows = []
            for row in cursor.fetchall():
                clean_row = []
                for value in row:
                    if isinstance(value, bytes):
                        # Handle bytes data
                        try:
                            clean_value = value.decode('utf-8', errors='replace')
                        except:
                            clean_value = str(value)
                    elif isinstance(value, str):
                        # Handle string data with potential encoding issues
                        try:
                            clean_value = value.encode('utf-8', errors='replace').decode('utf-8')
                        except:
                            clean_value = str(value)
                    else:
                        clean_value = str(value) if value is not None else ''
                    
                    clean_row.append(clean_value)
                
                rows.append(clean_row)
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=columns)
            
            # Additional cleaning
            for col in df.columns:
                df[col] = df[col].astype(str)
                df[col] = df[col].replace(['nan', 'None', 'NaT', 'NULL', 'null'], '')
                df[col] = df[col].fillna('')
                
                # Handle problematic characters
                df[col] = df[col].str.replace("'", "''")  # Escape quotes
                df[col] = df[col].str.replace('\n', ' ')  # Remove newlines
                df[col] = df[col].str.replace('\r', ' ')  # Remove carriage returns
                df[col] = df[col].str.replace('\t', ' ')  # Remove tabs
                df[col] = df[col].str.replace('\x00', '')  # Remove null bytes
                
                # Truncate very long strings
                df[col] = df[col].str[:16000]
            
            cursor.close()
            conn.close()
            
            # Insert into Snowflake
            sf_conn = snowflake.connector.connect(**self.sf_params)
            sf_cursor = sf_conn.cursor()
            
            try:
                sf_table_name = f"DBO_{table_name}".upper()
                
                # Drop and recreate table
                sf_cursor.execute(f"DROP TABLE IF EXISTS EYECARE_ANALYTICS.RAW.{sf_table_name}")
                
                # Create table
                columns_def = [f'"{col}" VARCHAR(16777216)' for col in df.columns]
                create_sql = f"""
                CREATE TABLE EYECARE_ANALYTICS.RAW.{sf_table_name} (
                    {', '.join(columns_def)}
                )
                """
                sf_cursor.execute(create_sql)
                
                # Insert data in batches
                batch_size = 500
                total_inserted = 0
                
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i+batch_size]
                    
                    values_list = []
                    for _, row in batch.iterrows():
                        values = [f"'{str(val)}'" for val in row]
                        values_list.append(f"({', '.join(values)})")
                    
                    values_str = ', '.join(values_list)
                    columns_str = ', '.join([f'"{col}"' for col in df.columns])
                    
                    insert_sql = f"""
                    INSERT INTO EYECARE_ANALYTICS.RAW.{sf_table_name} ({columns_str})
                    VALUES {values_str}
                    """
                    
                    sf_cursor.execute(insert_sql)
                    total_inserted += len(batch)
                
                console.print(f"✅ Successfully migrated {total_inserted:,} rows to {sf_table_name}")
                return total_inserted
                
            finally:
                sf_cursor.close()
                sf_conn.close()
            
        except Exception as e:
            console.print(f"❌ Error in encoding fix migration: {str(e)}", style="bold red")
            return 0
    
    def run_troubleshooting(self):
        """Run complete troubleshooting for failed migrations"""
        console.print(Panel("🔧 MIGRATION TROUBLESHOOTER", style="bold green"))
        
        # Step 1: Investigate invalid object names
        all_tables = self.investigate_invalid_object_names()
        
        # Step 2: Investigate encoding error
        if 'CompanyInfo' in self.failed_tables:
            encoding_info = self.investigate_encoding_error('CompanyInfo')
            
            if encoding_info and encoding_info['table_exists']:
                console.print(f"\n🔧 Attempting to migrate CompanyInfo with encoding fix...")
                rows_migrated = self.migrate_with_encoding_fix('CompanyInfo')
                
                if rows_migrated > 0:
                    console.print(f"✅ CompanyInfo migration successful: {rows_migrated:,} rows")
                else:
                    console.print("❌ CompanyInfo migration failed")
        
        # Step 3: Provide recommendations
        console.print(Panel("🎯 TROUBLESHOOTING RECOMMENDATIONS", style="bold blue"))
        
        console.print("1. **Invalid Object Names:**")
        console.print("   • Check the table list above for actual table names")
        console.print("   • Update migration plan with correct table names")
        console.print("   • Consider case sensitivity and schema differences")
        
        console.print("\n2. **Encoding Errors:**")
        console.print("   • CompanyInfo migration attempted with encoding fix")
        console.print("   • Use charset='utf8' and error='replace' for problematic tables")
        console.print("   • Consider data cleaning for binary/special characters")
        
        console.print("\n3. **Next Steps:**")
        console.print("   • Update production migration with correct table names")
        console.print("   • Add encoding error handling for all text-heavy tables")
        console.print("   • Re-run migration for fixed tables")

def main():
    """Main troubleshooting execution"""
    troubleshooter = MigrationTroubleshooter()
    troubleshooter.run_troubleshooting()

if __name__ == "__main__":
    main()
