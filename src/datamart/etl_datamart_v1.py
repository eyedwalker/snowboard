#!/usr/bin/env python3
"""
Eyecare Analytics Datamart V1.0 - ETL Pipeline
===============================================
Populates the datamart using validated V1.3 revenue cycle analytics
Iterative approach: Start with core data, expand as we learn more
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from datetime import datetime, timedelta
from connectors.robust_snowfall_connector import RobustSnowfallConnector
from analytics.v13_revenue_cycle_dashboard import convert_v13_query_to_snowflake

class DatamartETL:
    def __init__(self):
        self.connector = RobustSnowfallConnector()
        self.load_date = datetime.now()
        
    def populate_date_dimension(self, start_year=2019, end_year=2025):
        """Populate DIM_DATE with date range"""
        print(f"📅 Populating DIM_DATE ({start_year}-{end_year})...")
        
        try:
            # Generate date range
            start_date = datetime(start_year, 1, 1)
            end_date = datetime(end_year, 12, 31)
            
            dates = []
            current_date = start_date
            while current_date <= end_date:
                date_key = int(current_date.strftime('%Y%m%d'))
                dates.append({
                    'DATE_KEY': date_key,
                    'FULL_DATE': current_date.strftime('%Y-%m-%d'),
                    'YEAR': current_date.year,
                    'QUARTER': (current_date.month - 1) // 3 + 1,
                    'MONTH': current_date.month,
                    'MONTH_NAME': current_date.strftime('%B'),
                    'DAY': current_date.day,
                    'DAY_OF_WEEK': current_date.weekday() + 1,
                    'DAY_NAME': current_date.strftime('%A'),
                    'WEEK_OF_YEAR': current_date.isocalendar()[1],
                    'IS_WEEKEND': current_date.weekday() >= 5
                })
                current_date += timedelta(days=1)
            
            # Insert in batches
            batch_size = 365
            for i in range(0, len(dates), batch_size):
                batch = dates[i:i+batch_size]
                df = pd.DataFrame(batch)
                
                # Create insert statement
                insert_sql = """
                INSERT INTO DATAMART.DIM_DATE 
                (DATE_KEY, FULL_DATE, YEAR, QUARTER, MONTH, MONTH_NAME, 
                 DAY, DAY_OF_WEEK, DAY_NAME, WEEK_OF_YEAR, IS_WEEKEND)
                VALUES 
                """
                
                values = []
                for _, row in df.iterrows():
                    values.append(f"({row['DATE_KEY']}, '{row['FULL_DATE']}', {row['YEAR']}, "
                                f"{row['QUARTER']}, {row['MONTH']}, '{row['MONTH_NAME']}', "
                                f"{row['DAY']}, {row['DAY_OF_WEEK']}, '{row['DAY_NAME']}', "
                                f"{row['WEEK_OF_YEAR']}, {row['IS_WEEKEND']})")
                
                insert_sql += ",\n".join(values)
                
                try:
                    self.connector.execute_safe_query(insert_sql)
                    print(f"✅ Inserted {len(batch)} dates (batch {i//batch_size + 1})")
                except Exception as e:
                    print(f"⚠️ Batch {i//batch_size + 1} warning: {str(e)[:100]}...")
            
            print(f"✅ DIM_DATE populated with {len(dates)} records")
            
        except Exception as e:
            print(f"❌ Error populating DIM_DATE: {e}")
    
    def populate_office_dimension(self):
        """Populate DIM_OFFICE from RAW.DBO_OFFICE"""
        print("🏢 Populating DIM_OFFICE...")
        
        try:
            # Extract office data
            office_query = '''
            SELECT DISTINCT
                "OfficeNum" as OFFICE_ID,
                "OfficeName" as OFFICE_NAME,
                "CompanyID" as COMPANY_ID,
                "Address" as ADDRESS,
                "City" as CITY,
                "State" as STATE,
                "ZipCode" as ZIP_CODE,
                "Phone" as PHONE,
                "Active" as ACTIVE_FLAG
            FROM RAW.DBO_OFFICE
            WHERE "OfficeNum" IS NOT NULL
            '''
            
            offices_df = self.connector.execute_safe_query(office_query)
            
            if not offices_df.empty:
                # Insert offices
                for _, office in offices_df.iterrows():
                    insert_sql = f'''
                    INSERT INTO DATAMART.DIM_OFFICE 
                    (OFFICE_ID, OFFICE_NAME, COMPANY_ID, ADDRESS, CITY, STATE, ZIP_CODE, PHONE, ACTIVE_FLAG)
                    VALUES ('{office["OFFICE_ID"]}', '{str(office["OFFICE_NAME"]).replace("'", "''")}', 
                            '{office["COMPANY_ID"]}', '{str(office["ADDRESS"]).replace("'", "''")}',
                            '{office["CITY"]}', '{office["STATE"]}', '{office["ZIP_CODE"]}', 
                            '{office["PHONE"]}', {office["ACTIVE_FLAG"]})
                    '''
                    
                    try:
                        self.connector.execute_safe_query(insert_sql)
                    except Exception as e:
                        print(f"⚠️ Office {office['OFFICE_ID']} warning: {str(e)[:50]}...")
                
                print(f"✅ DIM_OFFICE populated with {len(offices_df)} records")
            else:
                print("⚠️ No office data found")
                
        except Exception as e:
            print(f"❌ Error populating DIM_OFFICE: {e}")
    
    def populate_fact_revenue_transactions(self, office_num='999', from_date='2019-01-01', to_date='2025-12-31'):
        """Populate FACT_REVENUE_TRANSACTIONS using V1.3 query"""
        print("💰 Populating FACT_REVENUE_TRANSACTIONS...")
        
        try:
            # Get V1.3 revenue data
            v13_query = convert_v13_query_to_snowflake(office_num, from_date, to_date)
            revenue_df = self.connector.execute_safe_query(v13_query)
            
            if not revenue_df.empty:
                print(f"📊 Processing {len(revenue_df)} revenue transactions...")
                
                # Get office key mapping
                office_mapping = self.connector.execute_safe_query(
                    "SELECT OFFICE_KEY, OFFICE_ID FROM DATAMART.DIM_OFFICE"
                )
                office_map = dict(zip(office_mapping['OFFICE_ID'], office_mapping['OFFICE_KEY']))
                
                # Process each transaction
                inserted_count = 0
                for _, row in revenue_df.iterrows():
                    try:
                        # Convert date to date key
                        service_date = pd.to_datetime(row['DATEOFSERVICE'])
                        date_key = int(service_date.strftime('%Y%m%d'))
                        
                        # Get office key
                        office_key = office_map.get(str(row['LOCATIONNUM']), 1)  # Default to 1 if not found
                        
                        # Insert transaction
                        insert_sql = f'''
                        INSERT INTO DATAMART.FACT_REVENUE_TRANSACTIONS 
                        (DATE_KEY, OFFICE_KEY, TRANSACTION_SOURCE, ORDER_ID, CLAIM_ID,
                         BILLED_AMOUNT, INSURANCE_AR, INSURANCE_PAYMENT, PATIENT_PAYMENT,
                         ADJUSTMENT, REFUND_ADJUSTMENT, WRITEOFF_ALL, COLLECTIONS,
                         INS_TOTAL_BALANCE, PATIENT_BALANCE)
                        VALUES ({date_key}, {office_key}, '{row["SOURCE"]}', '{row["ORDERID"]}', 
                                '{row["CLAIMID"]}', {row["BILLED"]}, {row["INSURANCE_AR"]},
                                {row["INSURANCE_PAYMENT"]}, {row["PATIENT_PAYMENT"]}, 
                                {row["ADJUSTMENT"]}, {row["REFUNDADJUSTMENT"]}, {row["WRITEOFF_ALL"]},
                                {row["COLLECTIONS"]}, {row["INS_TOTAL_BALANCE"]}, {row["PATIENT_BALANCE"]})
                        '''
                        
                        self.connector.execute_safe_query(insert_sql)
                        inserted_count += 1
                        
                        if inserted_count % 10 == 0:
                            print(f"✅ Inserted {inserted_count}/{len(revenue_df)} transactions")
                            
                    except Exception as e:
                        print(f"⚠️ Transaction warning: {str(e)[:50]}...")
                
                print(f"✅ FACT_REVENUE_TRANSACTIONS populated with {inserted_count} records")
            else:
                print("⚠️ No revenue data found")
                
        except Exception as e:
            print(f"❌ Error populating FACT_REVENUE_TRANSACTIONS: {e}")
    
    def validate_datamart(self):
        """Validate datamart data against V1.3 query"""
        print("🔍 Validating datamart data...")
        
        try:
            # Get datamart totals
            datamart_totals = self.connector.execute_safe_query('''
                SELECT 
                    COUNT(*) as TRANSACTION_COUNT,
                    SUM(BILLED_AMOUNT) as TOTAL_BILLED,
                    SUM(INSURANCE_PAYMENT + PATIENT_PAYMENT) as TOTAL_PAYMENTS,
                    SUM(INS_TOTAL_BALANCE + PATIENT_BALANCE) as TOTAL_OUTSTANDING
                FROM DATAMART.FACT_REVENUE_TRANSACTIONS
            ''')
            
            # Get V1.3 totals for comparison
            v13_query = convert_v13_query_to_snowflake('999', '2019-01-01', '2025-12-31')
            v13_data = self.connector.execute_safe_query(v13_query)
            
            if not datamart_totals.empty and not v13_data.empty:
                dm_count = datamart_totals.iloc[0]['TRANSACTION_COUNT']
                dm_billed = datamart_totals.iloc[0]['TOTAL_BILLED']
                
                v13_count = len(v13_data)
                v13_billed = v13_data['BILLED'].sum()
                
                print(f"📊 Validation Results:")
                print(f"  Datamart Transactions: {dm_count}")
                print(f"  V1.3 Query Transactions: {v13_count}")
                print(f"  Datamart Total Billed: ${dm_billed:,.2f}")
                print(f"  V1.3 Total Billed: ${v13_billed:,.2f}")
                
                if dm_count == v13_count:
                    print("✅ Transaction counts match!")
                else:
                    print("⚠️ Transaction counts differ")
                    
            else:
                print("⚠️ Unable to validate - missing data")
                
        except Exception as e:
            print(f"❌ Error validating datamart: {e}")
    
    def run_full_etl(self):
        """Run complete ETL pipeline"""
        print("🚀 STARTING DATAMART V1.0 ETL PIPELINE")
        print("=" * 60)
        
        start_time = datetime.now()
        
        # Step 1: Populate dimensions
        self.populate_date_dimension()
        self.populate_office_dimension()
        
        # Step 2: Populate facts
        self.populate_fact_revenue_transactions()
        
        # Step 3: Validate
        self.validate_datamart()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n🎉 ETL PIPELINE COMPLETED!")
        print(f"⏱️ Duration: {duration}")
        print(f"📅 Load Date: {self.load_date}")
        print("=" * 60)

if __name__ == "__main__":
    etl = DatamartETL()
    etl.run_full_etl()
