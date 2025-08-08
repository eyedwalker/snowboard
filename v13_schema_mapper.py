#!/usr/bin/env python3
"""
V1.3 Schema Mapper - Map V1.3 query fields to actual Snowflake schema
"""

import sys
import os
sys.path.append('src')

from connectors.robust_snowfall_connector import RobustSnowfallConnector

def check_v13_tables():
    """Check all V1.3 tables and their key fields"""
    connector = RobustSnowfallConnector()
    
    # Tables used in V1.3 query
    v13_tables = [
        'DBO_BILLINGCLAIMLINEITEM',
        'DBO_BILLINGLINEDETAILS', 
        'DBO_BILLINGCLAIM',
        'DBO_BILLINGCLAIMDATA',
        'DBO_BILLINGCLAIMORDERS',
        'DBO_INSCARRIER',
        'DBO_ORDERS',
        'DBO_PATIENT',
        'DBO_OFFICE',
        'DBO_BILLINGTRANSACTION',
        'DBO_POSTRANSACTION'
    ]
    
    print("🔍 V1.3 TABLE & FIELD MAPPING")
    print("=" * 50)
    
    results = {}
    
    for table in v13_tables:
        print(f"\n📋 {table}:")
        try:
            result = connector.execute_safe_query(f'SELECT * FROM RAW.{table} LIMIT 1')
            if not result.empty:
                columns = list(result.columns)
                results[table] = columns
                
                # Show key columns for joins
                key_cols = []
                for col in columns:
                    col_lower = col.lower()
                    if any(key in col_lower for key in ['id', 'num', 'date', 'amount', 'claim', 'order', 'customer', 'patient']):
                        key_cols.append(col)
                
                print(f"  ✅ {len(columns)} columns, KEY FIELDS:")
                for col in key_cols[:10]:  # Show first 10 key fields
                    print(f"    • {col}")
                    
            else:
                print(f"  ⚠️  Empty table")
                results[table] = []
                
        except Exception as e:
            print(f"  ❌ ERROR: {str(e)[:50]}")
            results[table] = None
    
    return results

def analyze_join_fields(results):
    """Analyze key join fields across tables"""
    print(f"\n\n🔗 JOIN FIELD ANALYSIS")
    print("=" * 50)
    
    # Key join patterns to check
    join_patterns = {
        'Customer/Patient ID': ['CustomerID', 'CustomerId', 'PatientId', 'PatientID', 'ID'],
        'Order Reference': ['OrderNum', 'OrderId', 'OrderID', 'order_id'],
        'Claim Reference': ['ClaimId', 'ClaimID', 'ClaimDataId', 'claim_id'],
        'Office Reference': ['OfficeNum', 'OfficeNumber', 'office_num'],
        'Transaction Date': ['TransactionDate', 'TransDate', 'Date', 'DateOfService'],
        'Amount Fields': ['Amount', 'Charge', 'Payment', 'amt']
    }
    
    for pattern_name, possible_fields in join_patterns.items():
        print(f"\n🔍 {pattern_name}:")
        found = False
        
        for table, columns in results.items():
            if columns:
                matches = [col for col in columns if col in possible_fields]
                if matches:
                    print(f"  {table}: {matches}")
                    found = True
        
        if not found:
            print(f"  ❌ No matches found")

def generate_v13_fixes(results):
    """Generate specific fixes for V1.3 query"""
    print(f"\n\n🔧 V1.3 QUERY FIXES")
    print("=" * 50)
    
    fixes = []
    
    # Check Orders table
    if 'DBO_ORDERS' in results and results['DBO_ORDERS']:
        orders_cols = results['DBO_ORDERS']
        print(f"\n📋 DBO_ORDERS fixes:")
        
        # Customer ID field
        customer_fields = [col for col in orders_cols if 'customer' in col.lower() and 'id' in col.lower()]
        if customer_fields:
            actual_field = customer_fields[0]
            print(f"  ✅ Customer field: {actual_field}")
            fixes.append(f"Replace 'CustomerId' with '{actual_field}' in Orders joins")
        
        # Order number field  
        order_fields = [col for col in orders_cols if 'order' in col.lower() and ('num' in col.lower() or 'id' in col.lower())]
        if order_fields:
            print(f"  ✅ Order fields: {order_fields}")
    
    # Check BillingTransaction table
    if 'DBO_BILLINGTRANSACTION' in results and results['DBO_BILLINGTRANSACTION']:
        billing_cols = results['DBO_BILLINGTRANSACTION']
        print(f"\n📋 DBO_BILLINGTRANSACTION fixes:")
        
        order_refs = [col for col in billing_cols if 'order' in col.lower()]
        if order_refs:
            actual_field = order_refs[0]
            print(f"  ✅ Order reference: {actual_field}")
            fixes.append(f"Use '{actual_field}' for billing-order joins (not OrderNum)")
    
    # Check BillingClaimOrders table
    if 'DBO_BILLINGCLAIMORDERS' in results and results['DBO_BILLINGCLAIMORDERS']:
        claimord_cols = results['DBO_BILLINGCLAIMORDERS']
        print(f"\n📋 DBO_BILLINGCLAIMORDERS fixes:")
        
        for col in claimord_cols:
            if 'claim' in col.lower():
                print(f"  ✅ Claim field: {col}")
                fixes.append(f"Use '{col}' for claim joins")
            elif 'order' in col.lower():
                print(f"  ✅ Order field: {col}")
    
    # Check Patient table
    if 'DBO_PATIENT' in results and results['DBO_PATIENT']:
        patient_cols = results['DBO_PATIENT']
        print(f"\n📋 DBO_PATIENT fixes:")
        
        birth_fields = [col for col in patient_cols if any(term in col.lower() for term in ['birth', 'dob'])]
        if birth_fields:
            actual_field = birth_fields[0]
            print(f"  ✅ Birth date field: {actual_field}")
            fixes.append(f"Use '{actual_field}' for patient birth date (not BirthDate)")
    
    print(f"\n🎯 SUMMARY OF REQUIRED FIXES:")
    for i, fix in enumerate(fixes, 1):
        print(f"  {i}. {fix}")
    
    return fixes

def main():
    print("🚀 V1.3 SCHEMA MAPPER - Field & Table Analysis")
    
    # Check all tables
    results = check_v13_tables()
    
    # Analyze join fields
    analyze_join_fields(results)
    
    # Generate fixes
    fixes = generate_v13_fixes(results)
    
    print(f"\n🎉 ANALYSIS COMPLETE!")
    print(f"Next: Apply these fixes to the V1.3 dashboard query")

if __name__ == "__main__":
    main()
