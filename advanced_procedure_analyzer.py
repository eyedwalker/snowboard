#!/usr/bin/env python3
"""
Advanced Stored Procedure Deep Analysis System
============================================
Extracts deeper insights from stored procedures:
- Parameter analysis and data flow
- Table dependencies and relationships  
- Business logic patterns and calculations
- Cross-procedure workflows and chains
- Hidden business rules and validations
"""

import pymssql
import os
from dotenv import load_dotenv
import pandas as pd
import json
import re
from collections import defaultdict, Counter
from datetime import datetime

load_dotenv()

class AdvancedProcedureAnalyzer:
    def __init__(self):
        self.connection = None
        self.procedures_data = {}
        self.insights = {
            'parameter_patterns': {},
            'table_dependencies': {},
            'business_calculations': {},
            'workflow_chains': {},
            'validation_rules': {},
            'integration_patterns': {},
            'performance_insights': {}
        }
    
    def connect_database(self):
        """Connect to SQL Server"""
        try:
            self.connection = pymssql.connect(
                server=os.getenv('SOURCE_DB_HOST'),
                user=os.getenv('SOURCE_DB_USER'),
                password=os.getenv('SOURCE_DB_PASSWORD'),
                database=os.getenv('SOURCE_DB_DATABASE'),
                port=int(os.getenv('SOURCE_DB_PORT', '1433')),
                timeout=30
            )
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def analyze_procedure_parameters(self):
        """Deep analysis of procedure parameters"""
        print("🔍 ANALYZING PROCEDURE PARAMETERS...")
        
        query = """
        SELECT 
            p.name AS procedure_name,
            par.name AS parameter_name,
            t.name AS data_type,
            par.max_length,
            par.is_output,
            par.has_default_value,
            par.default_value
        FROM sys.procedures p
        INNER JOIN sys.parameters par ON p.object_id = par.object_id
        INNER JOIN sys.types t ON par.user_type_id = t.user_type_id
        WHERE p.is_ms_shipped = 0
        ORDER BY p.name, par.parameter_id
        """
        
        df = pd.read_sql(query, self.connection)
        
        # Analyze parameter patterns
        param_patterns = defaultdict(list)
        for _, row in df.iterrows():
            param_name = row['parameter_name'].lower()
            
            # Identify common parameter types
            if 'patient' in param_name:
                param_patterns['patient_operations'].append(row['procedure_name'])
            elif 'office' in param_name:
                param_patterns['office_operations'].append(row['procedure_name'])
            elif 'date' in param_name:
                param_patterns['date_operations'].append(row['procedure_name'])
            elif 'id' in param_name:
                param_patterns['id_operations'].append(row['procedure_name'])
        
        self.insights['parameter_patterns'] = dict(param_patterns)
        print(f"📊 Analyzed parameters for {df['procedure_name'].nunique()} procedures")
        return df
    
    def analyze_table_dependencies(self):
        """Extract table dependencies from procedure definitions"""
        print("🔗 ANALYZING TABLE DEPENDENCIES...")
        
        query = """
        SELECT 
            p.name AS procedure_name,
            m.definition
        FROM sys.procedures p
        INNER JOIN sys.sql_modules m ON p.object_id = m.object_id
        WHERE p.is_ms_shipped = 0 AND m.definition IS NOT NULL
        """
        
        df = pd.read_sql(query, self.connection)
        
        # Extract table references
        table_deps = defaultdict(set)
        for _, row in df.iterrows():
            proc_name = row['procedure_name']
            definition = row['definition'].upper() if row['definition'] else ''
            
            # Find table references (simplified pattern)
            table_patterns = re.findall(r'FROM\s+(\w+)|JOIN\s+(\w+)|UPDATE\s+(\w+)|INSERT\s+INTO\s+(\w+)', definition)
            
            for match in table_patterns:
                for table in match:
                    if table and len(table) > 2:
                        table_deps[proc_name].add(table)
        
        # Convert sets to lists for JSON serialization
        self.insights['table_dependencies'] = {k: list(v) for k, v in table_deps.items()}
        print(f"📊 Analyzed table dependencies for {len(table_deps)} procedures")
        return df
    
    def extract_business_calculations(self):
        """Extract business calculation patterns"""
        print("🧮 EXTRACTING BUSINESS CALCULATIONS...")
        
        query = """
        SELECT 
            p.name AS procedure_name,
            m.definition
        FROM sys.procedures p
        INNER JOIN sys.sql_modules m ON p.object_id = m.object_id
        WHERE p.is_ms_shipped = 0 
        AND m.definition IS NOT NULL
        AND (UPPER(m.definition) LIKE '%CALCULATE%' 
             OR UPPER(m.definition) LIKE '%SUM(%'
             OR UPPER(m.definition) LIKE '%COUNT(%'
             OR UPPER(m.definition) LIKE '%AVG(%'
             OR UPPER(m.definition) LIKE '%TOTAL%'
             OR UPPER(m.definition) LIKE '%BALANCE%')
        """
        
        df = pd.read_sql(query, self.connection)
        
        calculations = {}
        for _, row in df.iterrows():
            proc_name = row['procedure_name']
            definition = row['definition'].upper() if row['definition'] else ''
            
            calc_patterns = []
            if 'BALANCE' in definition:
                calc_patterns.append('Balance Calculations')
            if 'SUM(' in definition:
                calc_patterns.append('Summation Logic')
            if 'TOTAL' in definition:
                calc_patterns.append('Total Calculations')
            if 'COUNT(' in definition:
                calc_patterns.append('Count Operations')
            if 'AVG(' in definition:
                calc_patterns.append('Average Calculations')
            
            if calc_patterns:
                calculations[proc_name] = calc_patterns
        
        self.insights['business_calculations'] = calculations
        print(f"📊 Found business calculations in {len(calculations)} procedures")
        return df
    
    def identify_workflow_chains(self):
        """Identify procedures that call other procedures"""
        print("🔄 IDENTIFYING WORKFLOW CHAINS...")
        
        query = """
        SELECT 
            p.name AS procedure_name,
            m.definition
        FROM sys.procedures p
        INNER JOIN sys.sql_modules m ON p.object_id = m.object_id
        WHERE p.is_ms_shipped = 0 
        AND m.definition IS NOT NULL
        AND UPPER(m.definition) LIKE '%EXEC%'
        """
        
        df = pd.read_sql(query, self.connection)
        
        workflow_chains = {}
        for _, row in df.iterrows():
            proc_name = row['procedure_name']
            definition = row['definition'] if row['definition'] else ''
            
            # Find EXEC statements (simplified)
            exec_patterns = re.findall(r'EXEC\s+(\w+)', definition.upper())
            
            if exec_patterns:
                workflow_chains[proc_name] = list(set(exec_patterns))
        
        self.insights['workflow_chains'] = workflow_chains
        print(f"📊 Found workflow chains in {len(workflow_chains)} procedures")
        return df
    
    def analyze_validation_rules(self):
        """Extract validation and business rules"""
        print("✅ ANALYZING VALIDATION RULES...")
        
        query = """
        SELECT 
            p.name AS procedure_name,
            m.definition
        FROM sys.procedures p
        INNER JOIN sys.sql_modules m ON p.object_id = m.object_id
        WHERE p.is_ms_shipped = 0 
        AND m.definition IS NOT NULL
        AND (UPPER(m.definition) LIKE '%IF%'
             OR UPPER(m.definition) LIKE '%CASE%'
             OR UPPER(m.definition) LIKE '%WHEN%'
             OR UPPER(m.definition) LIKE '%VALIDATE%'
             OR UPPER(m.definition) LIKE '%CHECK%')
        """
        
        df = pd.read_sql(query, self.connection)
        
        validations = {}
        for _, row in df.iterrows():
            proc_name = row['procedure_name']
            definition = row['definition'].upper() if row['definition'] else ''
            
            validation_types = []
            if 'IF' in definition and 'THEN' in definition:
                validation_types.append('Conditional Logic')
            if 'CASE WHEN' in definition:
                validation_types.append('Case-based Validation')
            if 'VALIDATE' in definition:
                validation_types.append('Explicit Validation')
            if 'CHECK' in definition:
                validation_types.append('Data Checks')
            
            if validation_types:
                validations[proc_name] = validation_types
        
        self.insights['validation_rules'] = validations
        print(f"📊 Found validation rules in {len(validations)} procedures")
        return df
    
    def create_advanced_dashboard(self):
        """Create advanced analysis dashboard"""
        dashboard_code = '''
import streamlit as st
import json
import pandas as pd

st.set_page_config(page_title="Advanced Procedure Analysis", layout="wide")
st.title("🔍 Advanced Stored Procedure Analysis")

@st.cache_data
def load_insights():
    try:
        with open('docs/advanced_procedure_insights.json', 'r') as f:
            return json.load(f)
    except:
        return {}

insights = load_insights()

if insights:
    tab1, tab2, tab3, tab4 = st.tabs(["Parameters", "Dependencies", "Calculations", "Workflows"])
    
    with tab1:
        st.header("Parameter Patterns")
        if 'parameter_patterns' in insights:
            for pattern, procedures in insights['parameter_patterns'].items():
                st.subheader(f"{pattern.replace('_', ' ').title()}")
                st.write(f"**{len(procedures)} procedures**")
                with st.expander("View Procedures"):
                    for proc in procedures[:20]:
                        st.write(f"• {proc}")
    
    with tab2:
        st.header("Table Dependencies")
        if 'table_dependencies' in insights:
            st.write(f"**{len(insights['table_dependencies'])} procedures** have table dependencies")
            
            # Most referenced tables
            all_tables = []
            for tables in insights['table_dependencies'].values():
                all_tables.extend(tables)
            
            table_counts = pd.Series(all_tables).value_counts().head(20)
            st.subheader("Most Referenced Tables")
            st.bar_chart(table_counts)
    
    with tab3:
        st.header("Business Calculations")
        if 'business_calculations' in insights:
            calc_types = {}
            for proc, calcs in insights['business_calculations'].items():
                for calc in calcs:
                    calc_types[calc] = calc_types.get(calc, 0) + 1
            
            st.subheader("Calculation Types")
            df = pd.DataFrame(list(calc_types.items()), columns=['Type', 'Count'])
            st.bar_chart(df.set_index('Type'))
    
    with tab4:
        st.header("Workflow Chains")
        if 'workflow_chains' in insights:
            st.write(f"**{len(insights['workflow_chains'])} procedures** call other procedures")
            
            for proc, called_procs in list(insights['workflow_chains'].items())[:10]:
                st.subheader(f"{proc}")
                st.write("Calls:")
                for called in called_procs:
                    st.write(f"• {called}")

else:
    st.error("No insights found. Run the analyzer first.")
        '''
        
        with open('advanced_procedure_dashboard.py', 'w') as f:
            f.write(dashboard_code)
    
    def run_analysis(self):
        """Run complete advanced analysis"""
        print("🚀 STARTING ADVANCED PROCEDURE ANALYSIS")
        print("=" * 50)
        
        if not self.connect_database():
            return False
        
        try:
            # Run all analyses
            self.analyze_procedure_parameters()
            self.analyze_table_dependencies()
            self.extract_business_calculations()
            self.identify_workflow_chains()
            self.analyze_validation_rules()
            
            # Save insights
            os.makedirs('docs', exist_ok=True)
            with open('docs/advanced_procedure_insights.json', 'w') as f:
                json.dump(self.insights, f, indent=2, default=str)
            
            # Create dashboard
            self.create_advanced_dashboard()
            
            print("\n🎉 ADVANCED ANALYSIS COMPLETE!")
            print("📊 Results saved to: docs/advanced_procedure_insights.json")
            print("🚀 Dashboard: advanced_procedure_dashboard.py")
            
            return True
            
        except Exception as e:
            print(f"❌ Analysis failed: {e}")
            return False
        
        finally:
            if self.connection:
                self.connection.close()

def main():
    analyzer = AdvancedProcedureAnalyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main()
