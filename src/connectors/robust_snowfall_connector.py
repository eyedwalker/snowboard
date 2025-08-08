#!/usr/bin/env python3
"""
Robust Snowflake Connector for Eyecare Analytics
Handles SSL issues and provides safe query execution
"""

import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

class RobustSnowfallConnector:
    """Robust Snowflake connector with SSL-safe query execution"""
    
    def __init__(self):
        self.connection = None
        self.connect()
    
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            self.connection = snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                role=os.getenv('SNOWFLAKE_ROLE'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                schema=os.getenv('SNOWFLAKE_SCHEMA')
            )
            print("✅ Connected to Snowflake successfully")
        except Exception as e:
            print(f"❌ Failed to connect to Snowflake: {str(e)}")
            raise
    
    def execute_safe_query(self, query, limit=1000):
        """Execute query with safe result set limits to avoid SSL issues"""
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            
            # Add limit if not already present
            if 'LIMIT' not in query.upper():
                query = f"{query} LIMIT {limit}"
            
            cursor.execute(query)
            
            # Fetch results as pandas DataFrame
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            
            df = pd.DataFrame(results, columns=columns)
            cursor.close()
            
            return df
            
        except Exception as e:
            print(f"❌ Query execution failed: {str(e)}")
            return pd.DataFrame()  # Return empty DataFrame on error
    
    def close(self):
        """Close the connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
