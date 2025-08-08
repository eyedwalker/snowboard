#!/usr/bin/env python3
"""
Quick test to check data availability in our tables
"""

import sys
import os
sys.path.append('src')

from connectors.robust_snowfall_connector import RobustSnowfallConnector

def test_data():
    connector = RobustSnowfallConnector()
    
    print("🔍 Testing data availability...")
    
    # Test POS transaction data
    pos_query = """
    SELECT 
        COUNT(*) as total_rows,
        MIN("TransactionDate") as earliest_date,
        MAX("TransactionDate") as latest_date,
        COUNT(DISTINCT "OfficeNum") as unique_offices,
        SUM(CASE WHEN "Amount" IS NOT NULL THEN 1 ELSE 0 END) as rows_with_amount
    FROM RAW.DBO_POSTRANSACTION 
    """
    
    print("\n📊 POS Transaction Data:")
    pos_result = connector.execute_safe_query(pos_query)
    print(pos_result)
    
    # Test billing transaction data
    billing_query = """
    SELECT 
        COUNT(*) as total_rows,
        MIN("TransactionDate") as earliest_date,
        MAX("TransactionDate") as latest_date,
        COUNT(DISTINCT "OfficeNum") as unique_offices,
        SUM(CASE WHEN "InsAR" IS NOT NULL OR "PatAR" IS NOT NULL THEN 1 ELSE 0 END) as rows_with_ar
    FROM RAW.DBO_BILLINGTRANSACTION 
    """
    
    print("\n💰 Billing Transaction Data:")
    billing_result = connector.execute_safe_query(billing_query)
    print(billing_result)
    
    # Sample recent data
    sample_query = """
    SELECT 
        "TransactionDate",
        "Amount",
        "OfficeNum"
    FROM RAW.DBO_POSTRANSACTION 
    WHERE "Amount" IS NOT NULL
    ORDER BY "TransactionDate" DESC
    LIMIT 10
    """
    
    print("\n📋 Sample Recent POS Data:")
    sample_result = connector.execute_safe_query(sample_query)
    print(sample_result)

if __name__ == "__main__":
    test_data()
