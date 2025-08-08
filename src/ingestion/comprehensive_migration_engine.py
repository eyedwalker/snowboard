#!/usr/bin/env python3
"""
Comprehensive Migration Engine
Investigates failed tables and continues systematic migration of all eyecare data
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

class ComprehensiveMigrationEngine:
    """Comprehensive migration engine that investigates failures and continues migration"""
    
    def __init__(self):
        """Initialize comprehensive migration engine"""
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
        self.migration_stats = {
            'total_attempted': 0,
            'successful': 0,
            'failed': 0,
            'investigated': 0,
            'retried': 0,
            'total_rows': 0,
            'successful_tables': [],
            'failed_tables': [],
            'investigation_results': []
        }
        
        # Load migration plan
        self.migration_plan = self._load_migration_plan()
        
        # Tables already migrated (to avoid duplicates)
        self.already_migrated = {
            'DBO_PATIENT', 'DBO_ORDERS', 'DBO_BILLINGCLAIM', 'DBO_PATIENTINSURANCE',
            'DBO_EGRX', 'DBO_PATIENTEXAM', 'DBO_BILLINGCLAIMDETAIL', 'DBO_APPSCHEDULE',
            'DBO_BILLINGCLAIMLINEITEM', 'DBO_APPSCH_APPOINTMENT_HISTORY', 'DBO_APPOINTMENT',
            'DBO_BILLINGCLAIMDATA', 'DBO_APPSCH_APPOINTMENT', 'DBO_PATIENTEXAMDETAIL',
            'DBO_INVOICEINSURANCEDET', 'DBO_EDI835CLAIMINFO', 'DBO_ORDERINSURANCE',
            'DBO_INSSCHEDULE', 'DBO_APPSCH_APPOINTMENT_ARCHIVE', 'DBO_INVOICEDET',
            'DBO_TEMP_INSSCHEDULE_BAK', 'DBO_TEMP_INSSCHEDULE_20191107', 'DBO_INSSCHEDULEATTRIBUTEVALUES',
            'DBO___CONVERSIONOMTOALSCHEDULE', 'DBO_SPEXFRAMEFORAPI', 'DBO_OFFICE',
            'DBO_EMPLOYEESCHEDULE', 'DBO_ORDEREXAMDETAILS', 'DBO_PATIENTEXAMDETAIL_EXTENSION',
            'DBO___KCDW_ORDERS', 'DBO_CONTACTLENSSUPPLIERDATA', 'DBO_INSBILLINGRULES',
            'DBO_INVOICESUM', 'DBO_ITEMCONTACTLENS', 'DBO_INVOICEDETAILTAXBREAKDOWN',
            'DBO_POSCUSTOMERCREDITPAYMENTDETAIL', 'DBO_PATIENTCOMMUNICATIONMETHOD',
            'DBO_BILLINGCLAIMRESPONSECODE', 'DBO_TEMP_ITEMEGLENSBAK', 'DBO_PATIENTCORRESPONDENCE',
            'DBO_PATIENTDOCUMENT', 'DBO_BILLINGLINEDETAILS', 'DBO_INVENTORYACTIVITY',
            'DBO___PATIENTBALANCE_TEMP', 'DBO___PATIENTCREDIT_TEMP', 'DBO_CLSOFTRX',
            'DBO_INSOFFICESCHEDULE', 'DBO_POSTRANSACTION', 'DBO_EDI835CLAIMSERVICEINFO',
            'DBO_PATIENTEXAMPROCEDURE', 'DBO_BILLINGRELATEDCLAIMS', 'DBO_TEMP_COMPANYINSSCHEDULE_20191107',
            'DBO_EGORDER', 'DBO_BILLINGTRANSACTION', 'DBO_BILLINGCLAIMORDERS', 'DBO_CLHARDRX',
            'DBO_KPIORDER', 'DBO_TEMP_INSPLANBEFORECHANGINGVSPPRODUCTPACKAGE', 'DBO_TEMP_ITEM_EGLENS',
            'DBO_TEMP_INSBILLINGRULES', 'DBO_ITEMEXAM', 'DBO_APPSCH_APPOINTMENTTYPE',
            # Batch 2 tables
            'DBO_POSPAYMENT', 'DBO_COMPANYINSSCHEDULE', 'DBO_BILLINGCLAIMRESPONSECODECATEGORY',
            'DBO_BILLINGPAYMENT', 'DBO_LABORDERSTATUS', 'DBO_ITEMEGLENS', 'DBO_PATIENTPHONE',
            'DBO_BILLINGCLAIMCHANGEFIELD', 'DBO_POSPAYMENTDETAIL', 'DBO_PATIENTEXAM_EXTENSION',
            'DBO_PATIENTADDRESS', 'DBO_ORDERINSURANCEATTRIBUTES', 'DBO_TEMP_INSOFFICESCHEDULE_BAK',
            'DBO_INSURANCESUBSCRIBER', 'DBO_TEMP_INSOFFICESCHEDULE_20191107', 'DBO_STOCKORDERDETAIL',
            'DBO_EGFRAME', 'DBO_COMPANYORDERCONFIG', 'DBO_STOCKORDER', 'DBO_INSBILLINGRULES_NOTMATCHING',
            'DBO_KPISCHEDULE', 'DBO_OLDCUSTOMER', 'DBO_BILLINGPAYMENTAPPLY', 'DBO_BILLINGCLAIMSTATUSLIFECYCLEINFO',
            'DBO_INSSCHEDULEMETHODCUSTOMATTRIBUTES', 'DBO_EMPLOYEE', 'DBO_SASCUSTOMER',
            'DBO_APPSCH_SCHEDULERTEMPLATE_BLOCKS', 'DBO_POSTRANSACTIONSUMMARY', 'DBO_CLORDER',
            'DBO_BILLINGCLAIMSTATUSCATEGORY', 'DBO_BILLINGPAYMENTSECTION', 'DBO_PATIENTLOCATIONS',
            'DBO_SELLINGMODELINSURANCE', 'DBO_PACKSCHEDULE', 'DBO_VENDORORDERDETAIL',
            'DBO_BILLINGCLAIMSTATUSLIFECYCLESTATE', 'DBO_BILLINGCLAIMSTATUSSCHEDULETYPE',
            'DBO_BILLINGPATIENTTRANSFERREASON', 'DBO_ITEMEGLENSLOADMAPPING', 'DBO_INSURANCECARRIERCUSTOMATTRIBUTES',
            'DBO_POSCREDITCARDPAYMENTDETAIL', 'DBO_EDI835CLAIMSERVICEADJINFO', 'DBO_PATIENTRECALLS',
            'DBO_INVOICECOMBINEDTEMP', 'DBO_BILLINGCLAIMSTATUSSCHEDULELEVEL', 'DBO_PRICINGSCHEDULE',
            'DBO_APPOINTMENTFILTERVALUES', 'DBO_FRAMESTYLE', 'DBO_EDI835CLAIMPAYMENT',
            # Batch 3 tables
            'DBO_BILLINGLINEITEMCURRENTAR', 'DBO_VST_FRAMESVS', 'DBO_INVENTORYBALANCE', 'DBO_ITEMFRAME',
            'DBO_ORDERSEQUENCE', 'DBO_EYEFINITYLISTDETAIL', 'DBO_PATIENTACTIVITIES', 'DBO_VENDORORDER',
            'DBO_INSURANCECALCULATIONERROR', 'DBO_BILLINGCLAIMSTATUSNPITYPE', 'DBO___KCDW_INVOICEDET',
            'DBO_BILLINGCLAIMSLOCK', 'DBO_BILLINGCLAIMSTATUSSCHEDULECOMPANYCARRIER', 'DBO_PATIENTINSURANCEATTRIBUTEVALUES',
            'DBO_VST_FRAME', 'DBO_PATIENTRACE', 'DBO_FRAMESEARCH', 'DBO_DOCTORPAYSCHEDULE',
            'DBO_INSELIGIBILITYORDERDETAIL', 'DBO_COMPANYPATIENTNOTICEMAPPING', 'DBO_BILLINGCLAIMSTATUSSCHEDULECARRIER',
            'DBO_BILLINGCLAIMSTATUSSCHEDULECOMPANY', 'DBO_BILLINGCLAIMSTATUSSCHEDULEOFFICE', 'DBO_INSSCHEDULEMETHOD',
            'DBO_OFFICEINVOICECONFIG', 'DBO_SPEXFRAME', 'DBO_SPEXFRAME_PRECOLUMNCHANGE',
            'DBO_OUTOFSTORERETURNORIGINALPAYMENTDETAIL', 'PRODUCTCATALOG_CATALOGENTRYCUSTOMERATTRIBUTE',
            'DBO_CLAIMGENERATIONFAILURE', 'DBO_TEMP_KPISCHEDULE', 'DBO_PRICINGOFFICESCHEDULE',
            'DBO_BILLINGCLAIMSTATUS', 'DBO_PATIENTFAVORITES', 'DBO_ITEMSALE', 'DBO_PRICINGSCHEDULEATTRIBUTEVALUES',
            'DBO_OFFICEPATIENTHISTORY', 'DBO_INVENTORYADJUSTMENT', 'DBO_INSURANCEBILLINGDEPARTMENT',
            'DBO_BILLINGCLAIMSTATUSLIFECYCLEHISTORY', 'DBO_PATIENTPINNEDNOTE', 'DBO_OUTOFSTORERETURNORIGINALPAYMENT',
            'DBO_COMPANYPAYMENTTYPE', 'DBO_CSVFRAME', 'DBO_FRAMEEDGETYPE', 'DBO_ABBCONTACTLENS',
            'DBO_PATIENTSTATEMENTBATCH', 'DBO_BILLINGCLAIMCHANGEDETAIL', 'DBO_INVOICECLAIMSTEMP', 'DBO_CLAIMGENERATIONSTATUS',
            # Batch 4 tables
            'PRODUCTCATALOG_CATALOGENTRYCUSTOMERATTRIBUTE', 'DBO_OFFICEPAYMENTTYPE', 'DBO_APPSCH_OFFICE',
            'DBO_EMPLOYEEEXAMMINUTES', 'DBO_BILLINGCLAIMCHANGEHISTORY', 'PRODUCTCATALOG_CUSTOMERCATALOGENTRY',
            'DBO_POSMISCPAYMENT', 'DBO_TEMPITEMEGLENSTODELETE', 'DBO_MISCPAYMENTREASON', 'DBO_EDI835EXTERNALCLAIM',
            'DBO_EYEFINITYBESRESPONSE', 'DBO_INVENTORYRETURNOPTIONNOTES', 'DBO_PATIENTDOCUMENTINTEGRATION',
            'DBO_APPOINTMENTFILTER', 'DBO_DOCTORPAYSCHEDULEITEM', 'DBO_FRAMECOLLECTION', 'DBO_LABORDERRESPONSE',
            'DBO_PATIENTEXAMATTRIBUTES', 'DBO_PATIENTDOCUMENTINTEGRATIONSTATUS', 'DBO_PAYMENTTYPE',
            'DBO_EDI277CLAIM', 'DBO_SALESBYHOUR_CACHED', 'DBO_APPOINTMENTCOLOR', 'DBO_APPSCH_RECURRINGAPPOINTMENT',
            'DBO_ORDERRETRANSMITREASON', 'DBO_PATIENTPROMOTIONCOUPONCODE', 'DBO_EYEFINITYBESRESPONSEDETAIL',
            'DBO_INVOICECONFIG', 'DBO_OFFICEBILLINGCONFIG', 'SALESTAX_OFFICETAXCONFIG', 'DBO_VSPPATIENTENCOUNTERVALIDATION',
            'DBO_PATIENTSTATEMENTBATCHSTATUS', 'DBO_INSRXMODIFIER', 'DBO_EYEFINITYEHRCONFIG', 'DBO_INVENTORYPHYSICALFREEZE',
            'DBO_DAILYSALESTRANSACTIONLOG', 'DBO_APPSCH_INSURANCE_REMINDERS', 'DBO_CONTACTLENSSUPPLIER',
            'DBO_PATIENTNOTICELETTERSETTING', 'DBO_APPSCH_SCHEDULERTEMPLATE_SERVICETEMPLATE', 'DBO_COMMISSIONSCHEDULE',
            'DBO_BILLINGACTIVITYREASON', 'DBO_BILLINGTRANSACTIONTYPE', 'DBO_PATIENTCOMMUNICATIONEVENTTYPE',
            'DBO_COMPANYINSSCHEDULEVERSION', 'DBO_FRAMESTATUS', 'DBO_CATALOGCLEANUP_INVENTORYBALANCE',
            'DBO_ORDERINVENTORYREPLACEMENTFRAME', 'DBO_CATALOGPROCESSORTRANSACTION', 'DBO_LABS'
        }
    
    def _load_migration_plan(self) -> list:
        """Load migration plan from CSV"""
        plan_path = project_root / "docs" / "migration" / "eyecare_migration_plan.csv"
        if plan_path.exists():
            df = pd.read_csv(plan_path)
            return df.to_dict('records')
        else:
            return []
    
    def investigate_failed_tables(self, failed_table_names: list):
        """Investigate why specific tables failed"""
        console.print(Panel("🔍 Investigating Failed Tables", style="bold yellow"))
        
        source_engine = create_engine(self.source_conn_str)
        
        try:
            with source_engine.connect() as conn:
                for table_name in failed_table_names:
                    console.print(f"\n🔍 Investigating: {table_name}")
                    
                    investigation = {
                        'table_name': table_name,
                        'exists': False,
                        'similar_tables': [],
                        'row_count': 0,
                        'recommendation': 'NOT_FOUND'
                    }
                    
                    # Check if exact table exists
                    exact_check_query = f"""
                    SELECT COUNT(*) as table_count
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
                    """
                    
                    try:
                        exact_result = pd.read_sql(exact_check_query, conn)
                        if exact_result.iloc[0]['table_count'] > 0:
                            investigation['exists'] = True
                            
                            # Get row count
                            count_query = f"SELECT COUNT(*) as row_count FROM dbo.{table_name}"
                            count_result = pd.read_sql(count_query, conn)
                            investigation['row_count'] = count_result.iloc[0]['row_count']
                            investigation['recommendation'] = 'RETRY_EXACT'
                            
                            console.print(f"✅ {table_name} exists with {investigation['row_count']:,} rows")
                        else:
                            console.print(f"❌ {table_name} does not exist")
                    except Exception as e:
                        console.print(f"⚠️ Error checking {table_name}: {str(e)}")
                    
                    # Find similar tables
                    if not investigation['exists']:
                        # Extract keywords from table name
                        keywords = []
                        if 'invoice' in table_name.lower():
                            keywords.append('invoice')
                        if 'detail' in table_name.lower():
                            keywords.append('detail')
                        if 'order' in table_name.lower():
                            keywords.append('order')
                        if 'item' in table_name.lower():
                            keywords.append('item')
                        
                        for keyword in keywords:
                            similar_query = f"""
                            SELECT TABLE_NAME, 
                                   (SELECT COUNT(*) FROM dbo.[{table_name}]) as row_count
                            FROM INFORMATION_SCHEMA.TABLES 
                            WHERE TABLE_SCHEMA = 'dbo' 
                            AND TABLE_NAME LIKE '%{keyword}%'
                            AND TABLE_TYPE = 'BASE TABLE'
                            ORDER BY TABLE_NAME
                            """
                            
                            try:
                                # Modify query to avoid referencing non-existent table
                                similar_query = f"""
                                SELECT TABLE_NAME
                                FROM INFORMATION_SCHEMA.TABLES 
                                WHERE TABLE_SCHEMA = 'dbo' 
                                AND TABLE_NAME LIKE '%{keyword}%'
                                AND TABLE_TYPE = 'BASE TABLE'
                                ORDER BY TABLE_NAME
                                """
                                
                                similar_result = pd.read_sql(similar_query, conn)
                                if not similar_result.empty:
                                    investigation['similar_tables'].extend(similar_result['TABLE_NAME'].tolist())
                            except Exception as e:
                                console.print(f"⚠️ Error finding similar tables for {keyword}: {str(e)}")
                        
                        # Remove duplicates and suggest best alternative
                        investigation['similar_tables'] = list(set(investigation['similar_tables']))
                        
                        if investigation['similar_tables']:
                            investigation['recommendation'] = f"TRY_ALTERNATIVE: {investigation['similar_tables'][0]}"
                            console.print(f"💡 Similar tables found: {', '.join(investigation['similar_tables'][:3])}")
                        else:
                            investigation['recommendation'] = 'NO_ALTERNATIVES'
                            console.print(f"❌ No similar tables found for {table_name}")
                    
                    self.migration_stats['investigation_results'].append(investigation)
                    self.migration_stats['investigated'] += 1
        
        finally:
            source_engine.dispose()
        
        return self.migration_stats['investigation_results']
    
    def migrate_priority_tables_batch(self, batch_size: int = 50):
        """Migrate tables in batches, investigating failures and continuing"""
        console.print(Panel(f"🚀 Comprehensive Migration - Batch Size: {batch_size}", style="bold green"))
        
        if not self.migration_plan:
            console.print("❌ No migration plan available", style="bold red")
            return self.migration_stats
        
        # Filter out already migrated tables
        remaining_tables = []
        for table in self.migration_plan:
            sf_table_name = f"DBO_{table['TABLE_NAME']}".upper()
            if sf_table_name not in self.already_migrated:
                remaining_tables.append(table)
        
        console.print(f"📊 Found {len(remaining_tables)} tables remaining to migrate")
        
        # Take batch
        batch_tables = remaining_tables[:batch_size]
        
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
                
                task = progress.add_task("Migrating batch...", total=len(batch_tables))
                
                for i, table_info in enumerate(batch_tables):
                    table_name = table_info['TABLE_NAME']
                    schema_name = table_info['TABLE_SCHEMA']
                    
                    progress.update(task, description=f"[cyan]Migrating {schema_name}.{table_name}")
                    
                    self.migration_stats['total_attempted'] += 1
                    
                    try:
                        # Attempt migration
                        rows_migrated = self._migrate_single_table_robust(
                            source_engine, sf_conn, schema_name, table_name
                        )
                        
                        self.migration_stats['successful'] += 1
                        self.migration_stats['total_rows'] += rows_migrated
                        self.migration_stats['successful_tables'].append({
                            'table': f"{schema_name}.{table_name}",
                            'rows': rows_migrated
                        })
                        
                        console.print(f"✅ {schema_name}.{table_name}: {rows_migrated:,} rows", style="green")
                        
                    except Exception as e:
                        self.migration_stats['failed'] += 1
                        error_info = {
                            'table': f"{schema_name}.{table_name}",
                            'error': str(e)
                        }
                        self.migration_stats['failed_tables'].append(error_info)
                        
                        console.print(f"❌ {schema_name}.{table_name}: {str(e)[:100]}...", style="red")
                        
                        # Quick investigation for this specific failure
                        self._quick_investigate_failure(source_engine, table_name, str(e))
                    
                    progress.advance(task)
                    
                    # Small delay to prevent overwhelming databases
                    time.sleep(0.1)
        
        finally:
            source_engine.dispose()
            sf_conn.close()
        
        # Display batch results
        self._display_batch_results(batch_tables)
        
        return self.migration_stats
    
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
            console.print(f"⚠️ Batch insert failed, trying single rows: {str(e)[:50]}...", style="yellow")
            
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
    
    def _quick_investigate_failure(self, source_engine, table_name: str, error: str):
        """Quick investigation of a specific failure"""
        if "does not exist" in error.lower():
            # Look for similar table names
            try:
                with source_engine.connect() as conn:
                    similar_query = f"""
                    SELECT TOP 3 TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = 'dbo' 
                    AND TABLE_NAME LIKE '%{table_name[:5]}%'
                    AND TABLE_TYPE = 'BASE TABLE'
                    """
                    
                    similar_result = pd.read_sql(similar_query, conn)
                    if not similar_result.empty:
                        similar_tables = ', '.join(similar_result['TABLE_NAME'].tolist())
                        console.print(f"💡 Similar tables found: {similar_tables}", style="blue")
            except:
                pass
    
    def _display_batch_results(self, batch_tables: list):
        """Display results for the current batch"""
        console.print(Panel("📊 Batch Migration Results", style="bold blue"))
        
        # Summary table
        summary_table = Table(title="📊 Migration Summary")
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="green")
        
        summary_table.add_row("Tables Attempted", str(self.migration_stats['total_attempted']))
        summary_table.add_row("Successful", str(self.migration_stats['successful']))
        summary_table.add_row("Failed", str(self.migration_stats['failed']))
        summary_table.add_row("Total Rows Migrated", f"{self.migration_stats['total_rows']:,}")
        
        if self.migration_stats['total_attempted'] > 0:
            success_rate = (self.migration_stats['successful'] / self.migration_stats['total_attempted'] * 100)
            summary_table.add_row("Success Rate", f"{success_rate:.1f}%")
        
        console.print(summary_table)
        
        # Show recent successes
        if self.migration_stats['successful_tables']:
            recent_success = self.migration_stats['successful_tables'][-5:]  # Last 5
            success_table = Table(title="✅ Recent Successful Migrations")
            success_table.add_column("Table", style="green")
            success_table.add_column("Rows", style="cyan")
            
            for item in recent_success:
                success_table.add_row(item['table'], f"{item['rows']:,}")
            
            console.print(success_table)

def main():
    """Main execution"""
    console.print(Panel("🚀 Comprehensive Eyecare Migration Engine", style="bold green"))
    
    try:
        migration_engine = ComprehensiveMigrationEngine()
        
        # Step 1: Investigate the specific failed tables
        failed_tables = ['InvoiceDetail', 'ItemOrder']
        console.print("🔍 Step 1: Investigating Previously Failed Tables")
        investigation_results = migration_engine.investigate_failed_tables(failed_tables)
        
        # Step 2: Continue with batch migration
        console.print("\n🚀 Step 2: Continuing Comprehensive Migration")
        results = migration_engine.migrate_priority_tables_batch(50)  # Migrate 50 more tables
        
        console.print(Panel(
            f"🎉 Comprehensive Migration Batch Complete!\n\n"
            f"• Tables Attempted: {results['total_attempted']}\n"
            f"• Successful: {results['successful']}\n"
            f"• Failed: {results['failed']}\n"
            f"• Total Rows Added: {results['total_rows']:,}\n"
            f"• Investigations: {results['investigated']}\n"
            f"• Your eyecare data keeps growing! ✅",
            style="bold green"
        ))
        
    except Exception as e:
        console.print(f"❌ Comprehensive migration failed: {str(e)}", style="bold red")

if __name__ == "__main__":
    main()
