#!/usr/bin/env python3
"""
Missing Tables Analysis for Comprehensive Eyecare Analytics
Identifies what tables we have vs what we need based on sophisticated V1.3 query logic
"""

import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
project_root = Path(__file__).parent.parent.parent
load_dotenv(project_root / ".env")

class MissingTablesAnalyzer:
    """Analyze missing tables for comprehensive eyecare analytics"""
    
    def __init__(self):
        """Initialize analyzer with SSL-safe connection"""
        self.sf_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'role': os.getenv('SNOWFLAKE_ROLE'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': 'EYECARE_ANALYTICS',
            'schema': 'RAW'
        }
    
    def get_connection(self):
        """Get SSL-safe Snowflake connection"""
        return snowflake.connector.connect(**self.sf_params)
    
    def execute_safe_query(self, query: str, limit: int = 1000) -> pd.DataFrame:
        """Execute query with SSL-safe approach (limited results)"""
        try:
            # Add LIMIT to avoid S3 staging
            if "LIMIT" not in query.upper():
                query = f"{query} LIMIT {limit}"
            
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch limited data to avoid SSL issues
            data = cursor.fetchmany(limit)
            
            # Create DataFrame
            df = pd.DataFrame(data, columns=columns)
            
            cursor.close()
            conn.close()
            
            return df
            
        except Exception as e:
            print(f"Query error: {str(e)}")
            return pd.DataFrame()
    
    def get_current_tables(self):
        """Get list of current tables in Snowflake"""
        try:
            query = "SHOW TABLES"
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            
            # Fetch table names directly without S3 staging
            tables = []
            for row in cursor:
                tables.append(row[1])  # Table name is in index 1
            
            cursor.close()
            conn.close()
            
            return sorted(tables)
            
        except Exception as e:
            print(f"Error getting tables: {str(e)}")
            return []
    
    def get_required_tables_for_v13_analytics(self):
        """Define required tables based on user's sophisticated V1.3 query and business model"""
        
        # Core financial tables (from V1.3 query)
        financial_core = [
            'DBO_BILLINGTRANSACTION',      # Central billing hub
            'DBO_BILLINGCLAIMLINEITEM',    # Claim line items
            'DBO_BILLINGCLAIMDATA',        # Claim header data
            'DBO_BILLINGCLAIM',            # Billing claims
            'DBO_POSTRANSACTION',          # POS transactions
            'DBO_BILLINGTRANSACTIONTYPE',  # Transaction type descriptions
        ]
        
        # Invoice intersection model (central data hub)
        invoice_intersection = [
            'DBO_INVOICEDET',              # Invoice details (central hub)
            'DBO_INVOICEINSURANCEDET',     # Invoice insurance details
            'DBO_INVOICESUM',              # Invoice summaries
        ]
        
        # Insurance ecosystem
        insurance_ecosystem = [
            'DBO_INSCARRIER',              # Insurance carriers
            'DBO_INSPLAN',                 # Insurance plans
            'DBO_INSELIGIBILITY',          # Patient eligibility
            'DBO_PATIENTINSURANCE',        # Patient insurance
            'DBO_INSSCHEDULE',             # Insurance schedules
            'DBO_INSSCHEDULEMETHOD',       # Schedule methods
            'DBO_INSHCFA',                 # HCFA billing configuration
        ]
        
        # Organizational hierarchy (sales attribution)
        organizational_hierarchy = [
            'DBO_COMPANYINFO',             # Top-level company
            'DBO_OFFICE',                  # Office locations
            'DBO_EMPLOYEE',                # Employee data
            'DBO_EMPLOYEEROLE',            # Employee roles
            'DBO_EMPLOYEETYPE',            # Employee types
            'DBO_EMPLOYEECOMMISSION',      # Commission data
        ]
        
        # Core operational tables
        operational_core = [
            'DBO_PATIENT',                 # Patient information
            'DBO_ORDERS',                  # Orders
            'DBO_ITEM',                    # Products/services
            'DBO_ITEMTYPE',                # Item types
            'DBO_ADDRESS',                 # Address information
            'DBO_PHONE',                   # Phone numbers
        ]
        
        # Appointment and scheduling
        scheduling = [
            'DBO_APPSCH_APPOINTMENT',      # Appointments
            'DBO_APPSCHEDULE',             # Schedule data
        ]
        
        # Additional financial tables
        additional_financial = [
            'DBO_ORDERINSURANCE',          # Order insurance
            'DBO_STOCKORDERDETAIL',        # Stock order details
            'DBO_BILLINGCLAIMORDERS',      # Claim orders relationship
        ]
        
        return {
            'Financial Core': financial_core,
            'Invoice Intersection': invoice_intersection,
            'Insurance Ecosystem': insurance_ecosystem,
            'Organizational Hierarchy': organizational_hierarchy,
            'Operational Core': operational_core,
            'Scheduling': scheduling,
            'Additional Financial': additional_financial
        }
    
    def analyze_missing_tables(self):
        """Analyze what tables are missing for comprehensive analytics"""
        print("🔍 ANALYZING MISSING TABLES FOR COMPREHENSIVE EYECARE ANALYTICS")
        print("=" * 70)
        
        # Get current tables
        current_tables = self.get_current_tables()
        print(f"📊 Current tables in Snowflake: {len(current_tables)}")
        
        # Get required tables
        required_tables_by_category = self.get_required_tables_for_v13_analytics()
        
        # Analyze each category
        missing_summary = {}
        priority_missing = []
        
        for category, required_tables in required_tables_by_category.items():
            print(f"\n📋 {category.upper()} ANALYSIS:")
            print("-" * 50)
            
            present = []
            missing = []
            
            for table in required_tables:
                if table in current_tables:
                    present.append(table)
                    print(f"✅ {table}")
                else:
                    missing.append(table)
                    print(f"❌ {table} - MISSING")
            
            missing_summary[category] = {
                'present': len(present),
                'missing': len(missing),
                'missing_tables': missing,
                'present_tables': present
            }
            
            # Identify priority missing tables
            if category in ['Financial Core', 'Invoice Intersection', 'Insurance Ecosystem']:
                priority_missing.extend(missing)
        
        # Summary report
        print(f"\n🎯 MISSING TABLES PRIORITY ANALYSIS")
        print("=" * 70)
        
        total_required = sum(len(tables) for tables in required_tables_by_category.values())
        total_present = sum(info['present'] for info in missing_summary.values())
        total_missing = sum(info['missing'] for info in missing_summary.values())
        
        print(f"📊 Overall Status:")
        print(f"   • Total Required: {total_required}")
        print(f"   • Present: {total_present} ({total_present/total_required*100:.1f}%)")
        print(f"   • Missing: {total_missing} ({total_missing/total_required*100:.1f}%)")
        
        print(f"\n🚨 HIGH PRIORITY MISSING TABLES:")
        for table in priority_missing:
            print(f"   • {table}")
        
        print(f"\n📈 CATEGORY BREAKDOWN:")
        for category, info in missing_summary.items():
            total_cat = info['present'] + info['missing']
            completion = info['present'] / total_cat * 100 if total_cat > 0 else 0
            print(f"   • {category}: {completion:.1f}% complete ({info['present']}/{total_cat})")
        
        return {
            'current_tables': current_tables,
            'missing_summary': missing_summary,
            'priority_missing': priority_missing,
            'total_stats': {
                'required': total_required,
                'present': total_present,
                'missing': total_missing
            }
        }
    
    def get_table_row_counts(self, tables_list):
        """Get row counts for existing tables"""
        print(f"\n📊 TABLE ROW COUNTS:")
        print("-" * 50)
        
        for table in tables_list[:10]:  # Limit to first 10 to avoid SSL issues
            try:
                query = f"SELECT COUNT(*) as row_count FROM EYECARE_ANALYTICS.RAW.{table}"
                df = self.execute_safe_query(query)
                if not df.empty:
                    count = df.iloc[0]['ROW_COUNT']
                    print(f"   • {table}: {count:,} rows")
                else:
                    print(f"   • {table}: 0 rows")
            except Exception as e:
                print(f"   • {table}: Error getting count")

def main():
    """Main analysis function"""
    analyzer = MissingTablesAnalyzer()
    
    # Run comprehensive missing tables analysis
    results = analyzer.analyze_missing_tables()
    
    # Get row counts for existing tables
    if results['current_tables']:
        analyzer.get_table_row_counts(results['current_tables'])
    
    print(f"\n🎯 RECOMMENDATIONS:")
    print("=" * 70)
    print("1. Migrate high-priority missing tables first")
    print("2. Focus on Financial Core and Insurance Ecosystem tables")
    print("3. Organizational Hierarchy tables needed for sales attribution")
    print("4. Invoice Intersection model tables for comprehensive analytics")
    
    return results

if __name__ == "__main__":
    main()
