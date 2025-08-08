#!/usr/bin/env python3
"""
Analyze product relationships between InvoiceDetail and Item tables
for eyecare product analytics (frames, lenses, coatings, etc.)
"""

import sys
sys.path.append('src')
import pandas as pd
from connectors.robust_snowfall_connector import RobustSnowfallConnector

def analyze_product_relationships():
    print("🔍 Analyzing Product Relationships for Eyecare Analytics")
    print("=" * 60)
    
    connector = RobustSnowfallConnector()
    
    # 1. Explore InvoiceDetail structure
    print("\n📋 STEP 1: InvoiceDetail Table Structure")
    try:
        result = connector.execute_safe_query('SELECT * FROM RAW.DBO_INVOICEDET LIMIT 1')
        print(f"✅ Found {len(result.columns)} columns in DBO_INVOICEDET:")
        for i, col in enumerate(result.columns):
            print(f"  {i+1:2d}. {col}")
    except Exception as e:
        print(f"❌ Error exploring InvoiceDetail: {e}")
        return
    
    # 2. Sample InvoiceDetail records with product info
    print("\n📊 STEP 2: Sample InvoiceDetail Records with Product Info")
    try:
        query = '''
        SELECT 
            "ID", "InvoiceID", "ItemType", "ItemID", "IsLensItem", 
            "Quantity", "Price", "Amount", "LineNum"
        FROM RAW.DBO_INVOICEDET 
        WHERE "ItemID" IS NOT NULL 
        LIMIT 5
        '''
        result = connector.execute_safe_query(query)
        print(f"Found {len(result)} records with ItemID:")
        for i, row in result.iterrows():
            print(f"  Record {i+1}:")
            print(f"    ID: {row['ID']}, InvoiceID: {row['InvoiceID']}")
            print(f"    ItemType: {row['ItemType']}, ItemID: {row['ItemID']}")
            print(f"    IsLensItem: {row['IsLensItem']}, Qty: {row['Quantity']}")
            print(f"    Price: ${row['Price']}, Amount: ${row['Amount']}")
    except Exception as e:
        print(f"❌ Error getting InvoiceDetail samples: {e}")
    
    # 3. Check for Item tables
    print("\n🔍 STEP 3: Finding Item-related Tables")
    try:
        query = "SHOW TABLES IN RAW LIKE '%ITEM%'"
        result = connector.execute_safe_query(query)
        print(f"Found {len(result)} Item-related tables:")
        for _, row in result.iterrows():
            table_name = row['name']
            print(f"  • {table_name}")
    except Exception as e:
        print(f"❌ Error finding Item tables: {e}")
    
    # 4. Explore main Item table structure
    print("\n📋 STEP 4: Main Item Table Structure")
    try:
        # Try DBO_ITEM first
        result = connector.execute_safe_query('SELECT * FROM RAW.DBO_ITEM LIMIT 1')
        print(f"✅ Found {len(result.columns)} columns in DBO_ITEM:")
        for i, col in enumerate(result.columns):
            print(f"  {i+1:2d}. {col}")
    except Exception as e:
        print(f"❌ DBO_ITEM not accessible: {e}")
        
        # Try other item tables
        item_tables = ['DBO_ITEMTYPE', 'DBO_ITEMS', 'DBO_ITEMMASTER']
        for table in item_tables:
            try:
                result = connector.execute_safe_query(f'SELECT * FROM RAW.{table} LIMIT 1')
                print(f"✅ Found {len(result.columns)} columns in {table}:")
                for i, col in enumerate(result.columns):
                    print(f"  {i+1:2d}. {col}")
                break
            except:
                continue
    
    # 5. Sample Item records
    print("\n📊 STEP 5: Sample Item Records")
    try:
        result = connector.execute_safe_query('SELECT * FROM RAW.DBO_ITEM LIMIT 3')
        print(f"Sample Item records ({len(result)} found):")
        for i, row in result.iterrows():
            print(f"\n  Item Record {i+1}:")
            for col in result.columns:
                value = row[col]
                if pd.notna(value) and str(value).strip() != '':
                    print(f"    {col:20}: {value}")
    except Exception as e:
        print(f"❌ Error getting Item samples: {e}")
    
    # 6. Check ItemType table for product categories
    print("\n🏷️ STEP 6: ItemType Table (Product Categories)")
    try:
        result = connector.execute_safe_query('SELECT * FROM RAW.DBO_ITEMTYPE LIMIT 10')
        print(f"ItemType records ({len(result)} found):")
        for i, row in result.iterrows():
            print(f"  {row.get('ID', 'N/A'):3}: {row.get('Description', row.get('Name', 'N/A'))}")
    except Exception as e:
        print(f"❌ Error getting ItemType: {e}")
    
    # 7. Join analysis - InvoiceDetail to Item
    print("\n🔗 STEP 7: Join Analysis - InvoiceDetail to Item")
    try:
        query = '''
        SELECT 
            inv."ID" as InvoiceDetailID,
            inv."ItemType",
            inv."ItemID", 
            inv."IsLensItem",
            inv."Quantity",
            inv."Price",
            inv."Amount",
            item."Description" as ItemDescription,
            itemtype."Description" as ItemTypeDescription
        FROM RAW.DBO_INVOICEDET inv
        LEFT JOIN RAW.DBO_ITEM item ON inv."ItemID" = item."ID"
        LEFT JOIN RAW.DBO_ITEMTYPE itemtype ON inv."ItemType" = itemtype."ID"
        WHERE inv."ItemID" IS NOT NULL
        LIMIT 5
        '''
        result = connector.execute_safe_query(query)
        print(f"✅ Successful join! Found {len(result)} records:")
        for i, row in result.iterrows():
            print(f"\n  Join Record {i+1}:")
            print(f"    InvoiceDetailID: {row['INVOICEDETAILID']}")
            print(f"    ItemType: {row['ITEMTYPE']} ({row['ITEMTYPEDESCRIPTION']})")
            print(f"    ItemID: {row['ITEMID']} ({row['ITEMDESCRIPTION']})")
            print(f"    IsLensItem: {row['ISLENSITEM']}")
            print(f"    Qty: {row['QUANTITY']}, Price: ${row['PRICE']}, Amount: ${row['AMOUNT']}")
    except Exception as e:
        print(f"❌ Error in join analysis: {e}")
    
    # 8. Product category analysis
    print("\n📈 STEP 8: Product Category Analysis")
    try:
        query = '''
        SELECT 
            itemtype."Description" as ProductCategory,
            COUNT(*) as TransactionCount,
            SUM(inv."Quantity") as TotalQuantity,
            SUM(inv."Amount") as TotalRevenue,
            AVG(inv."Price") as AvgPrice
        FROM RAW.DBO_INVOICEDET inv
        LEFT JOIN RAW.DBO_ITEMTYPE itemtype ON inv."ItemType" = itemtype."ID"
        WHERE inv."ItemID" IS NOT NULL
        GROUP BY itemtype."Description"
        ORDER BY TotalRevenue DESC
        LIMIT 10
        '''
        result = connector.execute_safe_query(query)
        print(f"✅ Product Category Summary ({len(result)} categories):")
        print(f"{'Category':<20} {'Transactions':<12} {'Quantity':<10} {'Revenue':<12} {'Avg Price':<10}")
        print("-" * 70)
        for _, row in result.iterrows():
            category = str(row['PRODUCTCATEGORY'])[:18] if row['PRODUCTCATEGORY'] else 'Unknown'
            transactions = int(row['TRANSACTIONCOUNT']) if row['TRANSACTIONCOUNT'] else 0
            quantity = int(row['TOTALQUANTITY']) if row['TOTALQUANTITY'] else 0
            revenue = float(row['TOTALREVENUE']) if row['TOTALREVENUE'] else 0
            avg_price = float(row['AVGPRICE']) if row['AVGPRICE'] else 0
            print(f"{category:<20} {transactions:<12} {quantity:<10} ${revenue:<11,.0f} ${avg_price:<9.2f}")
    except Exception as e:
        print(f"❌ Error in product category analysis: {e}")
    
    print("\n🎯 ANALYSIS COMPLETE!")
    print("=" * 60)

if __name__ == "__main__":
    analyze_product_relationships()
