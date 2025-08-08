#!/usr/bin/env python3
"""
Production Eyecare Datamart Builder
==================================
Enterprise-grade datamart with complete dimensional modeling
"""

import os
import pandas as pd
import snowflake.connector
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProductionDatamartBuilder:
    def __init__(self):
        self.load_config()
        self.connect_to_snowflake()
        
    def load_config(self):
        from dotenv import load_dotenv
        load_dotenv()
        
        self.config = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': 'ANALYTICS_DATAMART'
        }
        
    def connect_to_snowflake(self):
        try:
            self.conn = snowflake.connector.connect(
                account=self.config['account'],
                user=self.config['user'],
                password=self.config['password'],
                warehouse=self.config['warehouse'],
                database=self.config['database']
            )
            self.cursor = self.conn.cursor()
            logger.info("✅ Connected to Snowflake successfully")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {e}")
            raise
    
    def create_production_datamart(self):
        """Build complete production datamart"""
        logger.info("🚀 Building Production Eyecare Datamart...")
        
        # Create schema
        self.create_datamart_schema()
        
        # Create dimensions
        self.create_dimension_tables()
        
        # Create facts
        self.create_fact_tables()
        
        # Create views
        self.create_analytical_views()
        
        # Load initial data
        self.load_initial_data()
        
        logger.info("✅ Production datamart created successfully!")
    
    def create_datamart_schema(self):
        schema_sql = f"""
        CREATE SCHEMA IF NOT EXISTS {self.config['database']}.{self.config['schema']}
        COMMENT = 'Production Eyecare Analytics Datamart';
        """
        self.cursor.execute(schema_sql)
        self.cursor.execute(f"USE SCHEMA {self.config['database']}.{self.config['schema']}")
    
    def create_dimension_tables(self):
        """Create all dimension tables"""
        logger.info("📊 Creating dimension tables...")
        
        # DIM_DATE
        date_ddl = """
        CREATE OR REPLACE TABLE DIM_DATE (
            DATE_KEY INTEGER PRIMARY KEY,
            DATE_VALUE DATE NOT NULL,
            YEAR INTEGER,
            QUARTER INTEGER,
            MONTH INTEGER,
            DAY_OF_MONTH INTEGER,
            DAY_NAME VARCHAR(20),
            MONTH_NAME VARCHAR(20),
            IS_WEEKEND BOOLEAN,
            IS_HOLIDAY BOOLEAN,
            FISCAL_YEAR INTEGER,
            FISCAL_QUARTER INTEGER
        ) CLUSTER BY (DATE_VALUE);
        """
        
        # DIM_PATIENT
        patient_ddl = """
        CREATE OR REPLACE TABLE DIM_PATIENT (
            PATIENT_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
            PATIENT_ID INTEGER NOT NULL,
            FIRST_NAME VARCHAR(100),
            LAST_NAME VARCHAR(100),
            DATE_OF_BIRTH DATE,
            GENDER VARCHAR(10),
            CITY VARCHAR(100),
            STATE VARCHAR(50),
            ZIP_CODE VARCHAR(20),
            INSURANCE_PRIMARY VARCHAR(100),
            PATIENT_STATUS VARCHAR(50),
            REGISTRATION_DATE DATE,
            LIFETIME_VALUE DECIMAL(10,2) DEFAULT 0.00,
            EFFECTIVE_DATE DATE NOT NULL,
            EXPIRATION_DATE DATE,
            IS_CURRENT BOOLEAN DEFAULT TRUE
        ) CLUSTER BY (PATIENT_ID, IS_CURRENT);
        """
        
        # DIM_OFFICE
        office_ddl = """
        CREATE OR REPLACE TABLE DIM_OFFICE (
            OFFICE_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
            OFFICE_ID INTEGER NOT NULL,
            OFFICE_NAME VARCHAR(200),
            COMPANY_ID INTEGER,
            COMPANY_NAME VARCHAR(200),
            REGION VARCHAR(100),
            CITY VARCHAR(100),
            STATE VARCHAR(50),
            MANAGER_NAME VARCHAR(200),
            MONTHLY_TARGET DECIMAL(12,2),
            ANNUAL_TARGET DECIMAL(12,2),
            EFFECTIVE_DATE DATE NOT NULL,
            IS_CURRENT BOOLEAN DEFAULT TRUE
        ) CLUSTER BY (OFFICE_ID, IS_CURRENT);
        """
        
        # DIM_EMPLOYEE
        employee_ddl = """
        CREATE OR REPLACE TABLE DIM_EMPLOYEE (
            EMPLOYEE_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
            EMPLOYEE_ID INTEGER NOT NULL,
            FIRST_NAME VARCHAR(100),
            LAST_NAME VARCHAR(100),
            OFFICE_ID INTEGER,
            DEPARTMENT VARCHAR(100),
            POSITION_TITLE VARCHAR(200),
            COMMISSION_RATE DECIMAL(5,4),
            SALES_TARGET DECIMAL(12,2),
            EFFECTIVE_DATE DATE NOT NULL,
            IS_CURRENT BOOLEAN DEFAULT TRUE
        ) CLUSTER BY (EMPLOYEE_ID, IS_CURRENT);
        """
        
        # DIM_PRODUCT
        product_ddl = """
        CREATE OR REPLACE TABLE DIM_PRODUCT (
            PRODUCT_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
            ITEM_ID INTEGER NOT NULL,
            ITEM_TYPE_ID INTEGER,
            ITEM_NAME VARCHAR(500),
            CATEGORY VARCHAR(100),
            BRAND VARCHAR(200),
            RETAIL_PRICE DECIMAL(10,2),
            COST_PRICE DECIMAL(10,2),
            MARGIN_PERCENT DECIMAL(5,2),
            IS_ACTIVE BOOLEAN DEFAULT TRUE
        ) CLUSTER BY (ITEM_ID, ITEM_TYPE_ID);
        """
        
        # Execute DDL statements
        for table_name, ddl in [
            ('DIM_DATE', date_ddl),
            ('DIM_PATIENT', patient_ddl),
            ('DIM_OFFICE', office_ddl),
            ('DIM_EMPLOYEE', employee_ddl),
            ('DIM_PRODUCT', product_ddl)
        ]:
            logger.info(f"Creating {table_name}...")
            self.cursor.execute(ddl)
    
    def create_fact_tables(self):
        """Create fact tables"""
        logger.info("🎯 Creating fact tables...")
        
        # FACT_REVENUE_TRANSACTIONS
        revenue_ddl = """
        CREATE OR REPLACE TABLE FACT_REVENUE_TRANSACTIONS (
            TRANSACTION_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
            TRANSACTION_DATE_KEY INTEGER,
            PATIENT_KEY INTEGER,
            OFFICE_KEY INTEGER,
            EMPLOYEE_KEY INTEGER,
            PRODUCT_KEY INTEGER,
            TRANSACTION_ID INTEGER,
            ORDER_ID INTEGER,
            TRANSACTION_TYPE VARCHAR(100),
            QUANTITY DECIMAL(10,3),
            RETAIL_AMOUNT DECIMAL(12,2),
            BILLED_AMOUNT DECIMAL(12,2),
            PAID_AMOUNT DECIMAL(12,2),
            PATIENT_PAYMENT DECIMAL(12,2),
            INSURANCE_PAYMENT DECIMAL(12,2),
            ADJUSTMENT_AMOUNT DECIMAL(12,2),
            DISCOUNT_AMOUNT DECIMAL(12,2),
            NET_REVENUE DECIMAL(12,2),
            OUTSTANDING_BALANCE DECIMAL(12,2),
            COMMISSION_AMOUNT DECIMAL(12,2),
            IS_VOID BOOLEAN DEFAULT FALSE
        ) CLUSTER BY (TRANSACTION_DATE_KEY, OFFICE_KEY);
        """
        
        # FACT_PRODUCT_SALES
        sales_ddl = """
        CREATE OR REPLACE TABLE FACT_PRODUCT_SALES (
            SALES_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
            SALE_DATE_KEY INTEGER,
            PATIENT_KEY INTEGER,
            OFFICE_KEY INTEGER,
            EMPLOYEE_KEY INTEGER,
            PRODUCT_KEY INTEGER,
            QUANTITY_SOLD DECIMAL(10,3),
            UNIT_RETAIL_PRICE DECIMAL(10,2),
            UNIT_COST_PRICE DECIMAL(10,2),
            GROSS_SALES DECIMAL(12,2),
            DISCOUNT_AMOUNT DECIMAL(12,2),
            NET_SALES DECIMAL(12,2),
            COST_OF_GOODS DECIMAL(12,2),
            GROSS_PROFIT DECIMAL(12,2),
            GROSS_MARGIN_PERCENT DECIMAL(5,2)
        ) CLUSTER BY (SALE_DATE_KEY, PRODUCT_KEY);
        """
        
        # Execute fact table DDL
        for table_name, ddl in [
            ('FACT_REVENUE_TRANSACTIONS', revenue_ddl),
            ('FACT_PRODUCT_SALES', sales_ddl)
        ]:
            logger.info(f"Creating {table_name}...")
            self.cursor.execute(ddl)
    
    def create_analytical_views(self):
        """Create business-friendly analytical views"""
        logger.info("📈 Creating analytical views...")
        
        # Revenue Analytics View
        revenue_view = """
        CREATE OR REPLACE VIEW VW_REVENUE_ANALYTICS AS
        SELECT 
            d.DATE_VALUE,
            d.YEAR,
            d.MONTH,
            d.QUARTER,
            o.OFFICE_NAME,
            o.REGION,
            e.FIRST_NAME || ' ' || e.LAST_NAME AS EMPLOYEE_NAME,
            p.FIRST_NAME || ' ' || p.LAST_NAME AS PATIENT_NAME,
            pr.ITEM_NAME,
            pr.CATEGORY,
            f.TRANSACTION_TYPE,
            f.QUANTITY,
            f.RETAIL_AMOUNT,
            f.BILLED_AMOUNT,
            f.PAID_AMOUNT,
            f.NET_REVENUE,
            f.OUTSTANDING_BALANCE,
            f.COMMISSION_AMOUNT
        FROM FACT_REVENUE_TRANSACTIONS f
        JOIN DIM_DATE d ON f.TRANSACTION_DATE_KEY = d.DATE_KEY
        JOIN DIM_OFFICE o ON f.OFFICE_KEY = o.OFFICE_KEY
        JOIN DIM_EMPLOYEE e ON f.EMPLOYEE_KEY = e.EMPLOYEE_KEY
        JOIN DIM_PATIENT p ON f.PATIENT_KEY = p.PATIENT_KEY
        JOIN DIM_PRODUCT pr ON f.PRODUCT_KEY = pr.PRODUCT_KEY
        WHERE f.IS_VOID = FALSE;
        """
        
        # Executive Summary View
        executive_view = """
        CREATE OR REPLACE VIEW VW_EXECUTIVE_SUMMARY AS
        SELECT 
            d.YEAR,
            d.QUARTER,
            o.REGION,
            COUNT(DISTINCT f.PATIENT_KEY) as UNIQUE_PATIENTS,
            COUNT(f.TRANSACTION_KEY) as TOTAL_TRANSACTIONS,
            SUM(f.RETAIL_AMOUNT) as GROSS_REVENUE,
            SUM(f.NET_REVENUE) as NET_REVENUE,
            SUM(f.OUTSTANDING_BALANCE) as TOTAL_AR,
            AVG(f.NET_REVENUE) as AVG_TRANSACTION_VALUE
        FROM FACT_REVENUE_TRANSACTIONS f
        JOIN DIM_DATE d ON f.TRANSACTION_DATE_KEY = d.DATE_KEY
        JOIN DIM_OFFICE o ON f.OFFICE_KEY = o.OFFICE_KEY
        WHERE f.IS_VOID = FALSE
        GROUP BY d.YEAR, d.QUARTER, o.REGION;
        """
        
        # Execute view creation
        for view_name, sql in [
            ('VW_REVENUE_ANALYTICS', revenue_view),
            ('VW_EXECUTIVE_SUMMARY', executive_view)
        ]:
            logger.info(f"Creating {view_name}...")
            self.cursor.execute(sql)
    
    def load_initial_data(self):
        """Load initial data from RAW tables"""
        logger.info("📥 Loading initial data...")
        
        # Load date dimension
        self.load_date_dimension()
        
        # Load other dimensions from RAW data
        self.load_dimensions_from_raw()
        
        # Load fact data
        self.load_facts_from_raw()
    
    def load_date_dimension(self):
        """Load date dimension with business calendar"""
        date_sql = """
        INSERT INTO DIM_DATE (DATE_KEY, DATE_VALUE, YEAR, QUARTER, MONTH, DAY_OF_MONTH, 
                             DAY_NAME, MONTH_NAME, IS_WEEKEND, IS_HOLIDAY, FISCAL_YEAR, FISCAL_QUARTER)
        WITH date_range AS (
            SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY NULL) - 1, '2020-01-01') as date_val
            FROM TABLE(GENERATOR(ROWCOUNT => 2557))
        )
        SELECT 
            TO_NUMBER(TO_CHAR(date_val, 'YYYYMMDD')) as DATE_KEY,
            date_val as DATE_VALUE,
            YEAR(date_val) as YEAR,
            QUARTER(date_val) as QUARTER,
            MONTH(date_val) as MONTH,
            DAY(date_val) as DAY_OF_MONTH,
            DAYNAME(date_val) as DAY_NAME,
            MONTHNAME(date_val) as MONTH_NAME,
            CASE WHEN DAYOFWEEK(date_val) IN (1,7) THEN TRUE ELSE FALSE END as IS_WEEKEND,
            FALSE as IS_HOLIDAY,
            CASE WHEN MONTH(date_val) >= 7 THEN YEAR(date_val) + 1 ELSE YEAR(date_val) END as FISCAL_YEAR,
            CASE WHEN MONTH(date_val) IN (7,8,9) THEN 1
                 WHEN MONTH(date_val) IN (10,11,12) THEN 2
                 WHEN MONTH(date_val) IN (1,2,3) THEN 3
                 ELSE 4 END as FISCAL_QUARTER
        FROM date_range;
        """
        
        self.cursor.execute(date_sql)
        logger.info("✅ Date dimension loaded")
    
    def load_dimensions_from_raw(self):
        """Load dimension tables from RAW data"""
        
        # Load patients
        patient_sql = """
        INSERT INTO DIM_PATIENT (PATIENT_ID, FIRST_NAME, LAST_NAME, DATE_OF_BIRTH, 
                                GENDER, CITY, STATE, ZIP_CODE, PATIENT_STATUS, 
                                REGISTRATION_DATE, EFFECTIVE_DATE, IS_CURRENT)
        SELECT DISTINCT
            "PatientID" as PATIENT_ID,
            "FirstName" as FIRST_NAME,
            "LastName" as LAST_NAME,
            "DOB" as DATE_OF_BIRTH,
            "Gender" as GENDER,
            "City" as CITY,
            "State" as STATE,
            "Zip" as ZIP_CODE,
            'Active' as PATIENT_STATUS,
            "CreatedDate" as REGISTRATION_DATE,
            CURRENT_DATE() as EFFECTIVE_DATE,
            TRUE as IS_CURRENT
        FROM RAW.DBO_PATIENT
        WHERE "PatientID" IS NOT NULL;
        """
        
        # Load offices
        office_sql = """
        INSERT INTO DIM_OFFICE (OFFICE_ID, OFFICE_NAME, COMPANY_ID, CITY, STATE, 
                               EFFECTIVE_DATE, IS_CURRENT)
        SELECT DISTINCT
            "OfficeID" as OFFICE_ID,
            "OfficeName" as OFFICE_NAME,
            "CompanyID" as COMPANY_ID,
            "City" as CITY,
            "State" as STATE,
            CURRENT_DATE() as EFFECTIVE_DATE,
            TRUE as IS_CURRENT
        FROM RAW.DBO_OFFICE
        WHERE "OfficeID" IS NOT NULL;
        """
        
        # Execute dimension loads
        for sql_name, sql in [
            ('Patients', patient_sql),
            ('Offices', office_sql)
        ]:
            try:
                self.cursor.execute(sql)
                logger.info(f"✅ {sql_name} dimension loaded")
            except Exception as e:
                logger.warning(f"⚠️ Could not load {sql_name}: {e}")
    
    def load_facts_from_raw(self):
        """Load fact tables from RAW data"""
        
        # Load revenue transactions from POS data
        revenue_sql = """
        INSERT INTO FACT_REVENUE_TRANSACTIONS (
            TRANSACTION_DATE_KEY, PATIENT_KEY, OFFICE_KEY, TRANSACTION_ID,
            TRANSACTION_TYPE, RETAIL_AMOUNT, PAID_AMOUNT, NET_REVENUE
        )
        SELECT 
            TO_NUMBER(TO_CHAR(pos."TransactionDate", 'YYYYMMDD')) as TRANSACTION_DATE_KEY,
            COALESCE(p.PATIENT_KEY, -1) as PATIENT_KEY,
            COALESCE(o.OFFICE_KEY, -1) as OFFICE_KEY,
            pos."TransactionId" as TRANSACTION_ID,
            'POS Transaction' as TRANSACTION_TYPE,
            pos."Amount" as RETAIL_AMOUNT,
            pos."Amount" as PAID_AMOUNT,
            pos."Amount" as NET_REVENUE
        FROM RAW.DBO_POSTRANSACTION pos
        LEFT JOIN DIM_PATIENT p ON pos."PatientID" = p.PATIENT_ID AND p.IS_CURRENT = TRUE
        LEFT JOIN DIM_OFFICE o ON pos."OfficeID" = o.OFFICE_ID AND o.IS_CURRENT = TRUE
        WHERE pos."TransactionDate" IS NOT NULL
        AND pos."Amount" IS NOT NULL;
        """
        
        try:
            self.cursor.execute(revenue_sql)
            logger.info("✅ Revenue facts loaded")
        except Exception as e:
            logger.warning(f"⚠️ Could not load revenue facts: {e}")
    
    def get_datamart_summary(self):
        """Get summary of datamart contents"""
        summary_sql = """
        SELECT 
            'DIM_DATE' as TABLE_NAME,
            COUNT(*) as ROW_COUNT
        FROM DIM_DATE
        UNION ALL
        SELECT 
            'DIM_PATIENT' as TABLE_NAME,
            COUNT(*) as ROW_COUNT
        FROM DIM_PATIENT
        UNION ALL
        SELECT 
            'DIM_OFFICE' as TABLE_NAME,
            COUNT(*) as ROW_COUNT
        FROM DIM_OFFICE
        UNION ALL
        SELECT 
            'FACT_REVENUE_TRANSACTIONS' as TABLE_NAME,
            COUNT(*) as ROW_COUNT
        FROM FACT_REVENUE_TRANSACTIONS;
        """
        
        result = self.cursor.execute(summary_sql)
        return result.fetchall()

if __name__ == "__main__":
    builder = ProductionDatamartBuilder()
    builder.create_production_datamart()
    
    # Get summary
    summary = builder.get_datamart_summary()
    print("\n📊 Datamart Summary:")
    for table, count in summary:
        print(f"  {table}: {count:,} rows")
