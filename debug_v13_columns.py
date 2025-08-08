#!/usr/bin/env python3
"""
Simple V1.3 Column Checker - Debug column names step by step
"""

import sys
import os
sys.path.append('src')

from connectors.robust_snowfall_connector import RobustSnowfallConnector

def check_table_columns(table_name):
    """Check columns in a specific table"""
    connector = RobustSnowfallConnector()
    
    try:
        print(f"\n🔍 Checking {table_name}...")
        result = connector.execute_safe_query(f'SELECT * FROM RAW.{table_name} LIMIT 1')
        
        if not result.empty:
            print(f"✅ {table_name} - {len(result.columns)} columns:")
            for i, col in enumerate(result.columns):
                print(f"  {i+1:2d}. {col}")
            return list(result.columns)
        else:
            print(f"⚠️  {table_name} - Empty result")
            return []
            
    except Exception as e:
        print(f"❌ {table_name} - Error: {str(e)[:100]}")
        return []

def main():
    print("🚀 V1.3 Column Checker - Simple Approach")
    
    # Check key tables one by one
    key_tables = [
        'DBO_ORDERS',
        'DBO_PATIENT', 
        'DBO_BILLINGCLAIMDATA',
        'DBO_BILLINGLINEDETAILS'
    ]
    
    results = {}
    for table in key_tables:
        columns = check_table_columns(table)
        results[table] = columns
        
        # Small delay to avoid overwhelming the connection
        import time
        time.sleep(1)
    
    print("\n📊 Summary of Key Columns:")
    for table, columns in results.items():
        if columns:
            print(f"\n{table}:")
            # Look for key columns we need
            key_patterns = ['customer', 'patient', 'office', 'order', 'claim', 'id']
            for pattern in key_patterns:
                matching = [col for col in columns if pattern.lower() in col.lower()]
                if matching:
                    print(f"  {pattern.upper()}: {matching}")

if __name__ == "__main__":
    main()
