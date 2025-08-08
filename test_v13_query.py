#!/usr/bin/env python3
"""
V1.3 Query Tester - Debug why dashboard shows no data
"""

import sys
import os
sys.path.append('src')

from connectors.robust_snowfall_connector import RobustSnowfallConnector

def test_v13_query_step_by_step():
    """Test V1.3 query components step by step"""
    connector = RobustSnowfallConnector()
    
    print("🔍 V1.3 QUERY DEBUG - Step by Step")
    print("=" * 50)
    
    # Test 1: Basic billing transaction query
    print("\n🧪 TEST 1: Basic billing transactions for office 999")
    try:
        result = connector.execute_safe_query("""
            SELECT COUNT(*) as count
            FROM RAW.DBO_BILLINGTRANSACTION b
            INNER JOIN RAW.DBO_ORDERS ord ON CAST(ord."OrderNum" AS VARCHAR) = CAST(b."OrderId" AS VARCHAR)
            WHERE ord."OfficeNum" = '999'
        """)
        if not result.empty:
            count = result.iloc[0, 0]
            print(f"✅ Found {count:,} billing transactions")
        else:
            print("❌ No billing transactions found")
    except Exception as e:
        print(f"❌ Error: {str(e)[:80]}")
    
    # Test 2: Check date filtering
    print("\n🧪 TEST 2: Date range filtering (2024-10-01 to 2025-02-01)")
    try:
        result = connector.execute_safe_query("""
            SELECT 
                COUNT(*) as count,
                MIN(b."TransactionDate") as min_date,
                MAX(b."TransactionDate") as max_date
            FROM RAW.DBO_BILLINGTRANSACTION b
            INNER JOIN RAW.DBO_ORDERS ord ON CAST(ord."OrderNum" AS VARCHAR) = CAST(b."OrderId" AS VARCHAR)
            WHERE ord."OfficeNum" = '999'
            AND CAST(b."TransactionDate" as DATE) >= '2024-10-01'
            AND CAST(b."TransactionDate" as DATE) <= '2025-02-01'
        """)
        if not result.empty:
            row = result.iloc[0]
            print(f"✅ Found {row['count']:,} transactions in date range")
            print(f"   Date range: {row['min_date']} to {row['max_date']}")
        else:
            print("❌ No transactions in date range")
    except Exception as e:
        print(f"❌ Error: {str(e)[:80]}")
    
    # Test 3: Check full billing CTE
    print("\n🧪 TEST 3: Full billing CTE with all joins")
    try:
        result = connector.execute_safe_query("""
            SELECT COUNT(*) as count
            FROM RAW.DBO_BILLINGTRANSACTION b
            INNER JOIN RAW.DBO_ORDERS ord ON CAST(ord."OrderNum" AS VARCHAR) = CAST(b."OrderId" AS VARCHAR)
            LEFT JOIN RAW.DBO_BILLINGCLAIMLINEITEM li ON b."LineItemId" = li."LineItemId"
            LEFT JOIN RAW.DBO_BILLINGPAYMENT cp ON cp."PaymentId" = b."PaymentId"
            LEFT JOIN RAW.DBO_BILLINGLINEDETAILS ld ON ld."LineItemId" = li."LineItemId" AND ld."IsCurrent" = 1
            LEFT JOIN RAW.DBO_BILLINGCLAIM c ON li."ClaimId" = c."ClaimId"
            LEFT JOIN RAW.DBO_BILLINGCLAIMDATA cd ON c."ClaimId" = cd."ClaimDataId" AND cd."IsCurrent" = 1
            LEFT JOIN RAW.DBO_BILLINGCLAIMORDERS clord ON clord."ClaimID" = cd."ClaimDataId"
            LEFT JOIN RAW.DBO_INSCARRIER ic ON ic."ID" = cd."CarrierId" AND ic."IsPrepaidCarrier" = 0
            INNER JOIN RAW.DBO_PATIENT pat ON pat."ID" = ord."CustomerID"
            JOIN RAW.DBO_OFFICE ofc ON ofc."OfficeNum" = ord."OfficeNum"
            WHERE ord."OfficeNum" = '999'
            AND CAST(b."TransactionDate" as DATE) >= '2024-10-01'
            AND CAST(b."TransactionDate" as DATE) <= '2025-02-01'
        """)
        if not result.empty:
            count = result.iloc[0, 0]
            print(f"✅ Full billing CTE: {count:,} records")
        else:
            print("❌ No records from full billing CTE")
    except Exception as e:
        print(f"❌ Error: {str(e)[:80]}")
    
    # Test 4: Check POS transaction part
    print("\n🧪 TEST 4: POS transactions")
    try:
        result = connector.execute_safe_query("""
            SELECT COUNT(*) as count
            FROM RAW.DBO_POSTRANSACTION pt
            INNER JOIN RAW.DBO_ORDERS o ON o."OrderNum" = CAST(pt."OrderId" as VARCHAR)
            WHERE o."OfficeNum" = '999'
            AND CAST(pt."TransactionDate" as DATE) >= '2024-10-01'
            AND CAST(pt."TransactionDate" as DATE) <= '2025-02-01'
        """)
        if not result.empty:
            count = result.iloc[0, 0]
            print(f"✅ POS transactions: {count:,} records")
        else:
            print("❌ No POS transactions found")
    except Exception as e:
        print(f"❌ Error: {str(e)[:80]}")
    
    # Test 5: Check if HAVING clause is filtering out all data
    print("\n🧪 TEST 5: Test without HAVING clause")
    try:
        result = connector.execute_safe_query("""
            SELECT COUNT(*) as count
            FROM (
                SELECT 
                    'Billing' as source,
                    ord."OfficeNum" as LocationNum,
                    ofc."OfficeName" as LocationName,
                    COALESCE(cd."CarrierName", '') as InsuranceName,
                    COALESCE(cd."PlanName", '') as PlanName,
                    ord."OrderNum" as OrderId,
                    COALESCE(c."ClaimId", 0) as ClaimId,
                    CAST(b."TransactionDate" as DATE) as DateOfService,
                    COALESCE(b."InsDeltaAR", 0) as Billed
                FROM RAW.DBO_BILLINGTRANSACTION b
                INNER JOIN RAW.DBO_ORDERS ord ON CAST(ord."OrderNum" AS VARCHAR) = CAST(b."OrderId" AS VARCHAR)
                INNER JOIN RAW.DBO_PATIENT pat ON pat."ID" = ord."CustomerID"
                JOIN RAW.DBO_OFFICE ofc ON ofc."OfficeNum" = ord."OfficeNum"
                LEFT JOIN RAW.DBO_BILLINGCLAIMLINEITEM li ON b."LineItemId" = li."LineItemId"
                LEFT JOIN RAW.DBO_BILLINGCLAIM c ON li."ClaimId" = c."ClaimId"
                LEFT JOIN RAW.DBO_BILLINGCLAIMDATA cd ON c."ClaimId" = cd."ClaimDataId" AND cd."IsCurrent" = 1
                WHERE ord."OfficeNum" = '999'
                AND CAST(b."TransactionDate" as DATE) >= '2024-10-01'
                AND CAST(b."TransactionDate" as DATE) <= '2025-02-01'
            ) subquery
        """)
        if not result.empty:
            count = result.iloc[0, 0]
            print(f"✅ Without HAVING clause: {count:,} records")
        else:
            print("❌ No records even without HAVING clause")
    except Exception as e:
        print(f"❌ Error: {str(e)[:80]}")

def main():
    print("🚀 V1.3 QUERY TESTER")
    print("Testing why dashboard shows no data despite schema fixes")
    
    test_v13_query_step_by_step()
    
    print(f"\n🎯 DEBUGGING COMPLETE!")

if __name__ == "__main__":
    main()
