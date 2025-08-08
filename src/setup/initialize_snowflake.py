#!/usr/bin/env python3
"""
Snowflake Eyecare Analytics Platform - Initialization Script
Sets up the complete Snowflake environment for eyecare analytics
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional
import snowflake.connector
from snowflake.connector import DictCursor
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.panel import Panel

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Load environment variables
load_dotenv(project_root / ".env")

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

console = Console()

class SnowflakeInitializer:
    """Initialize and configure Snowflake environment for eyecare analytics"""
    
    def __init__(self):
        """Initialize with environment configuration"""
        self.connection_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'role': os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            'database': os.getenv('SNOWFLAKE_DATABASE', 'EYECARE_ANALYTICS'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
        }
        self.connection = None
        self.sql_dir = project_root / "sql"
        
    def connect(self) -> bool:
        """Establish connection to Snowflake"""
        try:
            console.print("[bold blue]Connecting to Snowflake...[/bold blue]")
            self.connection = snowflake.connector.connect(**self.connection_params)
            
            # Test connection
            cursor = self.connection.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()[0]
            
            console.print(f"[green]✅ Connected to Snowflake version: {version}[/green]")
            return True
            
        except Exception as e:
            console.print(f"[red]❌ Connection failed: {e}[/red]")
            return False
    
    def execute_sql_file(self, file_path: Path, description: str = "") -> bool:
        """Execute SQL from file"""
        try:
            with open(file_path, 'r') as f:
                sql_content = f.read()
            
            # Split by semicolon and execute each statement
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            cursor = self.connection.cursor()
            for stmt in statements:
                if stmt:
                    cursor.execute(stmt)
            
            console.print(f"[green]✅ {description or file_path.name} executed successfully[/green]")
            return True
            
        except Exception as e:
            console.print(f"[red]❌ Failed to execute {file_path.name}: {e}[/red]")
            return False
    
    def create_databases_and_schemas(self) -> bool:
        """Create the database and schema structure"""
        console.print("[bold blue]Creating database and schema structure...[/bold blue]")
        
        ddl_statements = [
            # Create main database
            f"CREATE DATABASE IF NOT EXISTS {self.connection_params['database']}",
            
            f"USE DATABASE {self.connection_params['database']}",
            
            # Create schemas for data layers
            "CREATE SCHEMA IF NOT EXISTS RAW COMMENT = 'Raw data from source systems'",
            "CREATE SCHEMA IF NOT EXISTS STAGING COMMENT = 'Cleaned and standardized data'", 
            "CREATE SCHEMA IF NOT EXISTS MARTS COMMENT = 'Business-ready dimensional models'",
            "CREATE SCHEMA IF NOT EXISTS ANALYTICS COMMENT = 'Analytics and reporting views'",
            "CREATE SCHEMA IF NOT EXISTS METADATA COMMENT = 'System metadata and audit logs'",
            
            # Create utility schemas
            "CREATE SCHEMA IF NOT EXISTS UTILS COMMENT = 'Utility functions and procedures'",
            "CREATE SCHEMA IF NOT EXISTS TESTS COMMENT = 'Data quality and testing objects'",
        ]
        
        try:
            cursor = self.connection.cursor()
            for statement in ddl_statements:
                cursor.execute(statement)
            
            console.print("[green]✅ Database and schema structure created[/green]")
            return True
            
        except Exception as e:
            console.print(f"[red]❌ Failed to create database structure: {e}[/red]")
            return False
    
    def create_warehouses(self) -> bool:
        """Create compute warehouses for different workloads"""
        console.print("[bold blue]Creating compute warehouses...[/bold blue]")
        
        warehouses = [
            {
                'name': 'INGESTION_WH',
                'size': 'SMALL',
                'comment': 'Warehouse for data ingestion and ETL processes'
            },
            {
                'name': 'ANALYTICS_WH', 
                'size': 'MEDIUM',
                'comment': 'Warehouse for analytics and reporting workloads'
            },
            {
                'name': 'DEVELOPMENT_WH',
                'size': 'X-SMALL',
                'comment': 'Warehouse for development and testing'
            }
        ]
        
        try:
            cursor = self.connection.cursor()
            for wh in warehouses:
                sql = f"""
                CREATE WAREHOUSE IF NOT EXISTS {wh['name']}
                WITH WAREHOUSE_SIZE = '{wh['size']}'
                AUTO_SUSPEND = 300
                AUTO_RESUME = TRUE
                INITIALLY_SUSPENDED = TRUE
                COMMENT = '{wh['comment']}'
                """
                cursor.execute(sql)
                console.print(f"[green]✅ Created warehouse: {wh['name']} ({wh['size']})[/green]")
            
            return True
            
        except Exception as e:
            console.print(f"[red]❌ Failed to create warehouses: {e}[/red]")
            return False
    
    def create_roles_and_permissions(self) -> bool:
        """Create roles and set up permissions"""
        console.print("[bold blue]Setting up roles and permissions...[/bold blue]")
        
        roles = [
            {
                'name': 'EYECARE_ADMIN',
                'comment': 'Administrative role for eyecare platform'
            },
            {
                'name': 'EYECARE_ANALYST',
                'comment': 'Analyst role for business users'
            },
            {
                'name': 'EYECARE_DEVELOPER',
                'comment': 'Developer role for platform development'
            },
            {
                'name': 'EYECARE_VIEWER',
                'comment': 'Read-only role for dashboard viewers'
            }
        ]
        
        try:
            cursor = self.connection.cursor()
            
            # Create custom roles
            for role in roles:
                cursor.execute(f"CREATE ROLE IF NOT EXISTS {role['name']} COMMENT = '{role['comment']}'")
                console.print(f"[green]✅ Created role: {role['name']}[/green]")
            
            # Grant permissions (basic setup - can be expanded)
            db_name = self.connection_params['database']
            
            permission_grants = [
                f"GRANT USAGE ON DATABASE {db_name} TO ROLE EYECARE_ADMIN",
                f"GRANT USAGE ON ALL SCHEMAS IN DATABASE {db_name} TO ROLE EYECARE_ADMIN",
                f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE {db_name} TO ROLE EYECARE_ADMIN",
                f"GRANT SELECT ON ALL VIEWS IN DATABASE {db_name} TO ROLE EYECARE_ADMIN",
                
                f"GRANT USAGE ON DATABASE {db_name} TO ROLE EYECARE_ANALYST",
                f"GRANT USAGE ON SCHEMA {db_name}.MARTS TO ROLE EYECARE_ANALYST",
                f"GRANT USAGE ON SCHEMA {db_name}.ANALYTICS TO ROLE EYECARE_ANALYST",
                f"GRANT SELECT ON ALL TABLES IN SCHEMA {db_name}.MARTS TO ROLE EYECARE_ANALYST",
                f"GRANT SELECT ON ALL VIEWS IN SCHEMA {db_name}.ANALYTICS TO ROLE EYECARE_ANALYST",
            ]
            
            for grant in permission_grants:
                cursor.execute(grant)
            
            console.print("[green]✅ Roles and permissions configured[/green]")
            return True
            
        except Exception as e:
            console.print(f"[red]❌ Failed to create roles: {e}[/red]")
            return False
    
    def create_monitoring_objects(self) -> bool:
        """Create monitoring and audit objects"""
        console.print("[bold blue]Creating monitoring and audit objects...[/bold blue]")
        
        try:
            cursor = self.connection.cursor()
            
            # Use metadata schema
            cursor.execute(f"USE SCHEMA {self.connection_params['database']}.METADATA")
            
            # Create audit log table
            audit_table_sql = """
            CREATE TABLE IF NOT EXISTS AUDIT_LOG (
                LOG_ID STRING DEFAULT UUID_STRING(),
                TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                USER_NAME STRING DEFAULT CURRENT_USER(),
                ROLE_NAME STRING DEFAULT CURRENT_ROLE(),
                CLIENT_ID STRING,
                ACTION_TYPE STRING,
                OBJECT_TYPE STRING,
                OBJECT_NAME STRING,
                QUERY_TEXT STRING,
                EXECUTION_TIME NUMBER(10,3),
                ROWS_AFFECTED NUMBER,
                STATUS STRING,
                ERROR_MESSAGE STRING,
                SESSION_ID STRING DEFAULT CURRENT_SESSION(),
                WAREHOUSE_NAME STRING DEFAULT CURRENT_WAREHOUSE()
            )
            """
            cursor.execute(audit_table_sql)
            
            # Create data quality log table
            dq_table_sql = """
            CREATE TABLE IF NOT EXISTS DATA_QUALITY_LOG (
                CHECK_ID STRING DEFAULT UUID_STRING(),
                TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                TABLE_NAME STRING,
                CHECK_TYPE STRING,
                CHECK_NAME STRING,
                EXPECTED_VALUE VARIANT,
                ACTUAL_VALUE VARIANT,
                STATUS STRING,
                SEVERITY STRING,
                DETAILS VARIANT
            )
            """
            cursor.execute(dq_table_sql)
            
            # Create system metrics table
            metrics_table_sql = """
            CREATE TABLE IF NOT EXISTS SYSTEM_METRICS (
                METRIC_ID STRING DEFAULT UUID_STRING(),
                TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                METRIC_NAME STRING,
                METRIC_VALUE NUMBER,
                METRIC_UNIT STRING,
                TAGS VARIANT,
                DIMENSIONS VARIANT
            )
            """
            cursor.execute(metrics_table_sql)
            
            console.print("[green]✅ Monitoring and audit objects created[/green]")
            return True
            
        except Exception as e:
            console.print(f"[red]❌ Failed to create monitoring objects: {e}[/red]")
            return False
    
    def display_summary(self):
        """Display initialization summary"""
        console.print("\n")
        
        # Create summary table
        table = Table(title="🏥 Snowflake Eyecare Analytics Platform - Initialization Complete")
        table.add_column("Component", style="bold blue")
        table.add_column("Status", style="bold green")
        table.add_column("Details")
        
        table.add_row("Database", "✅ Created", self.connection_params['database'])
        table.add_row("Schemas", "✅ Created", "RAW, STAGING, MARTS, ANALYTICS, METADATA, UTILS, TESTS")
        table.add_row("Warehouses", "✅ Created", "INGESTION_WH, ANALYTICS_WH, DEVELOPMENT_WH")
        table.add_row("Roles", "✅ Created", "EYECARE_ADMIN, EYECARE_ANALYST, EYECARE_DEVELOPER, EYECARE_VIEWER")
        table.add_row("Monitoring", "✅ Created", "Audit logs, data quality tracking, system metrics")
        
        console.print(table)
        
        # Next steps panel
        next_steps = Panel(
            "[bold yellow]Next Steps:[/bold yellow]\n\n"
            "• Configure source database connections\n"
            "• Run data ingestion pipelines\n"
            "• Deploy analytics dashboards\n"
            "• Set up monitoring and alerts\n"
            "• Configure user access and permissions",
            title="🚀 Ready for Data Pipeline Setup",
            border_style="green"
        )
        console.print(next_steps)
    
    def run_initialization(self) -> bool:
        """Run complete initialization process"""
        console.print(Panel(
            "[bold blue]🏥 Snowflake Eyecare Analytics Platform Initialization[/bold blue]",
            border_style="blue"
        ))
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            
            # Step 1: Connect
            task = progress.add_task("Connecting to Snowflake...", total=None)
            if not self.connect():
                return False
            progress.update(task, description="✅ Connected to Snowflake")
            
            # Step 2: Create database structure  
            task = progress.add_task("Creating database structure...", total=None)
            if not self.create_databases_and_schemas():
                return False
            progress.update(task, description="✅ Database structure created")
            
            # Step 3: Create warehouses
            task = progress.add_task("Creating compute warehouses...", total=None)
            if not self.create_warehouses():
                return False
            progress.update(task, description="✅ Compute warehouses created")
            
            # Step 4: Set up roles and permissions
            task = progress.add_task("Setting up roles and permissions...", total=None)
            if not self.create_roles_and_permissions():
                return False
            progress.update(task, description="✅ Roles and permissions configured")
            
            # Step 5: Create monitoring objects
            task = progress.add_task("Creating monitoring objects...", total=None)
            if not self.create_monitoring_objects():
                return False
            progress.update(task, description="✅ Monitoring objects created")
        
        self.display_summary()
        return True

def main():
    """Main execution function"""
    try:
        initializer = SnowflakeInitializer()
        success = initializer.run_initialization()
        
        if success:
            console.print("\n[bold green]🎉 Snowflake environment initialized successfully![/bold green]")
            sys.exit(0)
        else:
            console.print("\n[bold red]❌ Initialization failed![/bold red]")
            sys.exit(1)
            
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️ Initialization cancelled by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[red]❌ Unexpected error: {e}[/red]")
        sys.exit(1)

if __name__ == "__main__":
    main()
