#!/usr/bin/env python3
"""
Eyecare Datamart/Datawarehouse Builder
Creates a clean, optimized analytics layer using all discovered SQL knowledge
"""

import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.progress import Progress

# Load environment variables
project_root = Path(__file__).parent.parent.parent
load_dotenv(project_root / ".env")

console = Console()

class EyecareDatamartBuilder:
    """Build comprehensive eyecare datamart/datawarehouse"""
    
    def __init__(self):
        """Initialize datamart builder"""
        self.sf_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'role': os.getenv('SNOWFLAKE_ROLE'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': 'EYECARE_ANALYTICS',
            'schema': 'RAW'
        }
    
    def get_connection(self):
        """Get Snowflake connection"""
        return snowflake.connector.connect(**self.sf_params)
    
    def execute_query(self, query: str, return_results: bool = False):
        """Execute query with error handling"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            
            if return_results:
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=columns)
                cursor.close()
                conn.close()
                return df
            else:
                cursor.close()
                conn.close()
                return True
                
        except Exception as e:
            console.print(f"[red]Query execution error: {str(e)}[/red]")
            return False if not return_results else pd.DataFrame()
    
    def create_datamart_schemas(self):
        """Create datamart schemas"""
        console.print("[blue]🏗️ Creating datamart schemas...[/blue]")
        
        schemas = [
            "CREATE SCHEMA IF NOT EXISTS EYECARE_ANALYTICS.STAGING",
            "CREATE SCHEMA IF NOT EXISTS EYECARE_ANALYTICS.DATAMART", 
            "CREATE SCHEMA IF NOT EXISTS EYECARE_ANALYTICS.DATAWAREHOUSE"
        ]
        
        for schema_sql in schemas:
            if self.execute_query(schema_sql):
                console.print(f"[green]✅ Created schema[/green]")
    
    def create_insurance_carrier_dimension(self):
        """Create insurance carrier dimension using your SQL knowledge"""
        console.print("[blue]🏥 Creating insurance carrier dimension...[/blue]")
        
        carrier_dim_sql = """
        CREATE OR REPLACE TABLE EYECARE_ANALYTICS.DATAWAREHOUSE.DIM_INSURANCE_CARRIER AS
        SELECT 
            ic."ID" AS carrier_key,
            ic."CarrierCode" AS carrier_code,
            ic."CarrierName" AS carrier_name,
            ic."IsVspCarrier" AS is_vsp_carrier,
            ic."Active" AS is_active,
            addr."Address1" AS address_line_1,
            addr."City" AS city,
            addr."State" AS state,
            phone."PhoneNumber" AS primary_phone,
            CURRENT_TIMESTAMP AS created_at
        FROM EYECARE_ANALYTICS.RAW.DBO_INSCARRIER ic
        LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_ADDRESS addr ON ic."AddressID" = addr."ID"
        LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_PHONE phone ON ic."PhoneID" = phone."ID"
        WHERE ic."CarrierName" IS NOT NULL AND ic."CarrierName" != ''
        """
        
        if self.execute_query(carrier_dim_sql):
            console.print("[green]✅ Created DIM_INSURANCE_CARRIER[/green]")
    
    def create_invoice_fact_table(self):
        """Create invoice fact table using your intersection SQL knowledge"""
        console.print("[blue]💰 Creating invoice fact table...[/blue]")
        
        invoice_fact_sql = """
        CREATE OR REPLACE TABLE EYECARE_ANALYTICS.DATAWAREHOUSE.FACT_INVOICE_DETAILS AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY id."InvoiceID") AS invoice_detail_key,
            id."InvoiceID" AS invoice_id,
            id."ItemID" AS item_key,
            CAST(id."Price" AS DECIMAL(18,2)) AS price,
            CAST(id."Amount" AS DECIMAL(18,2)) AS amount,
            CAST(id."Tax" AS DECIMAL(18,2)) AS tax,
            id."BillToInsurance" AS bill_to_insurance,
            CURRENT_TIMESTAMP AS created_at
        FROM EYECARE_ANALYTICS.RAW.DBO_INVOICEDET id
        WHERE id."Amount" IS NOT NULL AND id."Amount" != ''
        """
        
        if self.execute_query(invoice_fact_sql):
            console.print("[green]✅ Created FACT_INVOICE_DETAILS[/green]")
    
    def create_analytics_views(self):
        """Create analytics views for dashboards"""
        console.print("[blue]📊 Creating analytics views...[/blue]")
        
        revenue_view_sql = """
        CREATE OR REPLACE VIEW EYECARE_ANALYTICS.DATAMART.VW_REVENUE_SUMMARY AS
        SELECT 
            COUNT(*) AS transaction_count,
            SUM(amount) AS total_revenue,
            AVG(amount) AS avg_transaction_amount
        FROM EYECARE_ANALYTICS.DATAWAREHOUSE.FACT_INVOICE_DETAILS
        """
        
        if self.execute_query(revenue_view_sql):
            console.print("[green]✅ Created VW_REVENUE_SUMMARY[/green]")
    
    def build_complete_datamart(self):
        """Build the complete datamart/datawarehouse"""
        console.print("[bold blue]🚀 Building Complete Eyecare Datamart[/bold blue]")
        
        with Progress() as progress:
            task = progress.add_task("[green]Building datamart...", total=5)
            
            progress.update(task, advance=1, description="[green]Creating schemas...")
            self.create_datamart_schemas()
            
            progress.update(task, advance=1, description="[green]Creating dimensions...")
            self.create_insurance_carrier_dimension()
            
            progress.update(task, advance=1, description="[green]Creating fact tables...")
            self.create_invoice_fact_table()
            
            progress.update(task, advance=1, description="[green]Creating views...")
            self.create_analytics_views()
            
            progress.update(task, advance=1, description="[green]Completed!")
        
        console.print("[bold green]🎉 Datamart build completed![/bold green]")

def main():
    """Main execution"""
    builder = EyecareDatamartBuilder()
    builder.build_complete_datamart()

if __name__ == "__main__":
    main()
