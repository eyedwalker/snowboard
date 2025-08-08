#!/usr/bin/env python3
"""
Final V1.3 Query Test - Test with all schema and syntax fixes
"""

import sys
import os
sys.path.append('src')

from analytics.v13_revenue_cycle_dashboard import convert_v13_query_to_snowflake
from connectors.robust_snowfall_connector import RobustSnowfallConnector

def test_final_v13_query():
    """Test the complete V1.3 query with all fixes applied"""
    print("🧪 Testing FINAL V1.3 query with all fixes applied:")
    
    try:
        # Test the complete V1.3 query with all fixes
        query = convert_v13_query_to_snowflake('999', '2024-10-01', '2025-02-01')
        connector = RobustSnowfallConnector()
        result = connector.execute_safe_query(query)
        
        if not result.empty:
            print(f"🎉 SUCCESS! V1.3 query returned {len(result)} rows!")
            print(f"Columns: {list(result.columns)[:8]}...")
            
            # Show key metrics
            if 'Ins_Total_Balance' in result.columns:
                total_ins_ar = result['Ins_Total_Balance'].sum()
                print(f"Total Insurance A/R: ${total_ins_ar:,.2f}")
            
            if 'Patient_Balance' in result.columns:
                total_pat_ar = result['Patient_Balance'].sum()
                print(f"Total Patient A/R: ${total_pat_ar:,.2f}")
                
            print("\n🎉 V1.3 DASHBOARD IS NOW READY WITH REAL DATA!")
            return True
        else:
            print("⚠️  Query executed successfully but returned no data for the date range")
            print("Try expanding the date range or checking different office numbers")
            return False
            
    except Exception as e:
        print(f"❌ Query error: {str(e)[:150]}")
        return False

if __name__ == "__main__":
    test_final_v13_query()
