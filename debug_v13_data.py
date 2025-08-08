#!/usr/bin/env python3
"""
V1.3 Data Availability Checker - Debug why no data is returned
"""

import sys
import os
sys.path.append('src')

from connectors.robust_snowfall_connector import RobustSnowfallConnector

def check_data_availability():
    """Check data availability in key V1.3 tables"""
    connector = RobustSnowfallConnector()
    
    print("🔍 V1.3 Data Availability Check")
    print("=" * 50)
    
    # Check basic table row counts
    tables_to_check = [
        'DBO_BILLINGTRANSACTION',
        'DBO_BILLINGCLAIMDATA', 
        'DBO_BILLINGLINEDETAILS',
        'DBO_ORDERS',
        'DBO_PATIENT',
        'DBO_OFFICE',
        'DBO_BILLINGCLAIM',
        'DBO_BILLINGCLAIMORDERS'
    ]
    
    for table in tables_to_check:
        try:
            result = connector.execute_safe_query(f'SELECT COUNT(*) as row_count FROM RAW.{table}')
            if not result.empty:
                count = result.iloc[0]['row_count']
                print(f"✅ {table}: {count:,} rows")
            else:
                print(f"⚠️  {table}: Empty result")
        except Exception as e:
            print(f"❌ {table}: {str(e)[:60]}")
    
    print("\n🔍 Checking Date Ranges in Key Tables:")
    
    # Check date ranges in billing transactions
    try:
        result = connector.execute_safe_query("""
            SELECT 
                MIN("TransactionDate") as min_date,
                MAX("TransactionDate") as max_date,
                COUNT(*) as total_rows
            FROM RAW.DBO_BILLINGTRANSACTION
        """)
        if not result.empty:
            row = result.iloc[0]
            print(f"📅 DBO_BILLINGTRANSACTION dates: {row['min_date']} to {row['max_date']} ({row['total_rows']:,} rows)")
    except Exception as e:
        print(f"❌ Date check error: {str(e)[:60]}")
    
    # Check office numbers
    try:
        result = connector.execute_safe_query("""
            SELECT DISTINCT "OfficeNum", COUNT(*) as order_count
            FROM RAW.DBO_ORDERS 
            GROUP BY "OfficeNum"
            ORDER BY order_count DESC
            LIMIT 10
        """)
        if not result.empty:
            print(f"\n🏢 Top Office Numbers in Orders:")
            for _, row in result.iterrows():
                print(f"  Office {row['OfficeNum']}: {row['order_count']:,} orders")
    except Exception as e:
        print(f"❌ Office check error: {str(e)[:60]}")
    
    # Check if there are any successful joins between key tables
    try:
        result = connector.execute_safe_query("""
            SELECT COUNT(*) as join_count
            FROM RAW.DBO_ORDERS ord
            INNER JOIN RAW.DBO_PATIENT pat ON pat."ID" = ord."CustomerID"
            LIMIT 10
        """)
        if not result.empty:
            count = result.iloc[0]['join_count']
            print(f"\n🔗 Orders-Patient Join Test: {count:,} successful joins")
    except Exception as e:
        print(f"❌ Join test error: {str(e)[:60]}")

if __name__ == "__main__":
    check_data_availability()
