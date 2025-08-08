#!/usr/bin/env python3
"""
SQLAlchemy-based Database Connector
=================================
Improved database connector using SQLAlchemy for better pandas compatibility
and enhanced performance for stored procedure analysis
"""

import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import urllib.parse

load_dotenv()

class SQLAlchemyConnector:
    def __init__(self):
        self.engine = None
        self.connection_string = None
    
    def create_connection_string(self):
        """Create SQLAlchemy connection string for SQL Server"""
        
        # Get connection parameters
        server = os.getenv('SOURCE_DB_HOST', '10.154.10.204')
        database = os.getenv('SOURCE_DB_DATABASE', 'blink_dev1')
        username = os.getenv('SOURCE_DB_USER', 'sa')
        password = os.getenv('SOURCE_DB_PASSWORD')
        port = os.getenv('SOURCE_DB_PORT', '1433')
        
        if not password:
            raise ValueError("SOURCE_DB_PASSWORD environment variable is required")
        
        # URL encode the password to handle special characters
        encoded_password = urllib.parse.quote_plus(password)
        
        # Create connection string for SQL Server using pymssql driver
        self.connection_string = f"mssql+pymssql://{username}:{encoded_password}@{server}:{port}/{database}"
        
        print(f"📡 Connection string created for server: {server}:{port}")
        return self.connection_string
    
    def connect(self):
        """Create SQLAlchemy engine and test connection"""
        try:
            if not self.connection_string:
                self.create_connection_string()
            
            # Create SQLAlchemy engine
            self.engine = create_engine(
                self.connection_string,
                pool_pre_ping=True,  # Verify connections before use
                pool_recycle=3600,   # Recycle connections after 1 hour
                connect_args={
                    'timeout': 30,
                    'login_timeout': 30
                }
            )
            
            # Test the connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test"))
                test_value = result.scalar()
                
            if test_value == 1:
                print("✅ SQLAlchemy connection successful!")
                return True
            else:
                print("❌ Connection test failed")
                return False
                
        except SQLAlchemyError as e:
            print(f"❌ SQLAlchemy connection failed: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected connection error: {e}")
            return False
    
    def execute_query(self, query, params=None):
        """Execute query and return pandas DataFrame"""
        try:
            if not self.engine:
                if not self.connect():
                    return pd.DataFrame()
            
            # Execute query with pandas (no more warnings!)
            df = pd.read_sql(query, self.engine, params=params)
            print(f"📊 Query executed successfully, returned {len(df)} rows")
            return df
            
        except Exception as e:
            print(f"❌ Query execution failed: {e}")
            return pd.DataFrame()
    
    def get_stored_procedures_comprehensive(self):
        """Get comprehensive stored procedure information"""
        query = """
        SELECT 
            SCHEMA_NAME(p.schema_id) AS schema_name,
            p.name AS procedure_name,
            p.type_desc,
            p.create_date,
            p.modify_date,
            DATEDIFF(day, p.create_date, p.modify_date) as days_between_create_modify,
            CASE 
                WHEN m.definition IS NOT NULL THEN 'Has Source'
                ELSE 'No Source Access'
            END as source_availability,
            CASE 
                WHEN m.definition IS NOT NULL THEN LEN(m.definition)
                ELSE 0
            END as definition_length,
            CASE 
                WHEN m.definition IS NOT NULL THEN LEFT(m.definition, 500)
                ELSE NULL
            END as definition_preview
        FROM sys.procedures p
        LEFT JOIN sys.sql_modules m ON p.object_id = m.object_id
        WHERE p.is_ms_shipped = 0
        ORDER BY p.modify_date DESC
        """
        
        return self.execute_query(query)
    
    def get_procedure_parameters(self):
        """Get procedure parameters with SQLAlchemy"""
        query = """
        SELECT 
            p.name AS procedure_name,
            par.name AS parameter_name,
            t.name AS data_type,
            par.max_length,
            par.precision,
            par.scale,
            par.is_output,
            par.has_default_value,
            par.default_value,
            par.parameter_id
        FROM sys.procedures p
        INNER JOIN sys.parameters par ON p.object_id = par.object_id
        INNER JOIN sys.types t ON par.user_type_id = t.user_type_id
        WHERE p.is_ms_shipped = 0
        ORDER BY p.name, par.parameter_id
        """
        
        return self.execute_query(query)
    
    def get_functions_comprehensive(self):
        """Get comprehensive function information"""
        query = """
        SELECT 
            SCHEMA_NAME(f.schema_id) AS schema_name,
            f.name AS function_name,
            f.type_desc,
            f.create_date,
            f.modify_date,
            CASE 
                WHEN m.definition IS NOT NULL THEN LEN(m.definition)
                ELSE 0
            END as definition_length,
            CASE 
                WHEN m.definition IS NOT NULL THEN LEFT(m.definition, 500)
                ELSE NULL
            END as definition_preview
        FROM sys.objects f
        LEFT JOIN sys.sql_modules m ON f.object_id = m.object_id
        WHERE f.type IN ('FN', 'IF', 'TF')
        AND f.is_ms_shipped = 0
        ORDER BY f.modify_date DESC
        """
        
        return self.execute_query(query)
    
    def get_views_comprehensive(self):
        """Get comprehensive view information"""
        query = """
        SELECT 
            SCHEMA_NAME(v.schema_id) AS schema_name,
            v.name AS view_name,
            v.create_date,
            v.modify_date,
            CASE 
                WHEN m.definition IS NOT NULL THEN LEN(m.definition)
                ELSE 0
            END as definition_length,
            CASE 
                WHEN m.definition IS NOT NULL THEN LEFT(m.definition, 500)
                ELSE NULL
            END as definition_preview
        FROM sys.views v
        LEFT JOIN sys.sql_modules m ON v.object_id = m.object_id
        WHERE v.is_ms_shipped = 0
        ORDER BY v.modify_date DESC
        """
        
        return self.execute_query(query)
    
    def analyze_database_objects(self):
        """Comprehensive analysis using SQLAlchemy"""
        print("🔍 COMPREHENSIVE DATABASE ANALYSIS WITH SQLALCHEMY")
        print("=" * 60)
        
        if not self.connect():
            return False
        
        try:
            # Get all database objects
            print("\n📋 Getting stored procedures...")
            procedures_df = self.get_stored_procedures_comprehensive()
            
            print("\n🔧 Getting procedure parameters...")
            parameters_df = self.get_procedure_parameters()
            
            print("\n🧮 Getting functions...")
            functions_df = self.get_functions_comprehensive()
            
            print("\n👁️ Getting views...")
            views_df = self.get_views_comprehensive()
            
            # Analysis summary
            print(f"\n📊 ANALYSIS SUMMARY:")
            print(f"  • Stored Procedures: {len(procedures_df)}")
            print(f"  • Parameters: {len(parameters_df)}")
            print(f"  • Functions: {len(functions_df)}")
            print(f"  • Views: {len(views_df)}")
            
            # Save results
            os.makedirs('docs', exist_ok=True)
            
            if len(procedures_df) > 0:
                procedures_df.to_csv('docs/stored_procedures_sqlalchemy.csv', index=False)
                print("💾 Procedures saved to: docs/stored_procedures_sqlalchemy.csv")
            
            if len(parameters_df) > 0:
                parameters_df.to_csv('docs/procedure_parameters_sqlalchemy.csv', index=False)
                print("💾 Parameters saved to: docs/procedure_parameters_sqlalchemy.csv")
            
            if len(functions_df) > 0:
                functions_df.to_csv('docs/functions_sqlalchemy.csv', index=False)
                print("💾 Functions saved to: docs/functions_sqlalchemy.csv")
            
            if len(views_df) > 0:
                views_df.to_csv('docs/views_sqlalchemy.csv', index=False)
                print("💾 Views saved to: docs/views_sqlalchemy.csv")
            
            return True
            
        except Exception as e:
            print(f"❌ Analysis failed: {e}")
            return False
    
    def close(self):
        """Close the database connection"""
        if self.engine:
            self.engine.dispose()
            print("🔌 Database connection closed")

def main():
    """Test the SQLAlchemy connector"""
    connector = SQLAlchemyConnector()
    
    try:
        success = connector.analyze_database_objects()
        
        if success:
            print("\n🎉 SQLAlchemy analysis complete!")
            print("✅ No more pandas warnings!")
            print("📈 Improved performance and compatibility")
        else:
            print("❌ Analysis failed")
            
    finally:
        connector.close()

if __name__ == "__main__":
    main()
