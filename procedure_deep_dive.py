#!/usr/bin/env python3
"""
Comprehensive Stored Procedure Deep Dive Analysis
===============================================
Extracts maximum insights from stored procedures using multiple approaches:
- Procedure metadata and naming patterns
- Parameter analysis and signatures
- Dependencies and relationships
- Business domain classification
- Execution frequency and performance patterns
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

class ProcedureDeepDive:
    def __init__(self):
        self.connection = None
        self.deep_insights = {
            'metadata': {'analysis_date': datetime.now().isoformat()},
            'procedure_inventory': {},
            'naming_patterns': {},
            'parameter_analysis': {},
            'business_domains': {},
            'complexity_analysis': {},
            'dependency_networks': {},
            'execution_insights': {},
            'business_intelligence': {}
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
    
    def analyze_procedure_inventory(self):
        """Complete inventory of all procedures with metadata"""
        print("📋 BUILDING COMPLETE PROCEDURE INVENTORY...")
        
        query = """
        SELECT 
            SCHEMA_NAME(p.schema_id) AS schema_name,
            p.name AS procedure_name,
            p.type_desc,
            p.create_date,
            p.modify_date,
            DATEDIFF(day, p.create_date, p.modify_date) as days_between_create_modify,
            CASE 
                WHEN m.definition IS NOT NULL THEN 'Has Source'
                ELSE 'No Source Access'
            END as source_availability,
            CASE 
                WHEN m.definition IS NOT NULL THEN LEN(m.definition)
                ELSE 0
            END as definition_length
        FROM sys.procedures p
        LEFT JOIN sys.sql_modules m ON p.object_id = m.object_id
        WHERE p.is_ms_shipped = 0
        ORDER BY p.create_date DESC
        """
        
        df = pd.read_sql(query, self.connection)
        
        # Analyze creation patterns
        df['create_year'] = pd.to_datetime(df['create_date']).dt.year
        df['modify_year'] = pd.to_datetime(df['modify_date']).dt.year
        
        inventory = {
            'total_procedures': len(df),
            'with_source_code': len(df[df['source_availability'] == 'Has Source']),
            'without_source_code': len(df[df['source_availability'] == 'No Source Access']),
            'creation_timeline': df['create_year'].value_counts().to_dict(),
            'modification_timeline': df['modify_year'].value_counts().to_dict(),
            'schemas': df['schema_name'].value_counts().to_dict(),
            'avg_definition_length': df[df['definition_length'] > 0]['definition_length'].mean(),
            'complexity_distribution': {
                'simple': len(df[df['definition_length'].between(0, 1000)]),
                'medium': len(df[df['definition_length'].between(1001, 5000)]),
                'complex': len(df[df['definition_length'] > 5000])
            }
        }
        
        self.deep_insights['procedure_inventory'] = inventory
        print(f"📊 Inventoried {len(df)} procedures across {df['schema_name'].nunique()} schemas")
        return df
    
    def analyze_naming_patterns(self):
        """Deep analysis of procedure naming conventions"""
        print("🏷️ ANALYZING NAMING PATTERNS...")
        
        query = """
        SELECT name AS procedure_name
        FROM sys.procedures 
        WHERE is_ms_shipped = 0
        ORDER BY name
        """
        
        df = pd.read_sql(query, self.connection)
        
        # Extract naming patterns
        patterns = {
            'prefixes': defaultdict(int),
            'suffixes': defaultdict(int),
            'business_domains': defaultdict(list),
            'action_verbs': defaultdict(list),
            'entity_focus': defaultdict(list)
        }
        
        for proc_name in df['procedure_name']:
            name_lower = proc_name.lower()
            
            # Analyze prefixes (first part before underscore)
            if '_' in proc_name:
                prefix = proc_name.split('_')[0]
                patterns['prefixes'][prefix] += 1
                
                # Analyze suffixes (last part after underscore)
                suffix = proc_name.split('_')[-1]
                patterns['suffixes'][suffix] += 1
            
            # Business domain classification
            if any(word in name_lower for word in ['patient', 'clinical', 'exam', 'diagnosis']):
                patterns['business_domains']['clinical'].append(proc_name)
            elif any(word in name_lower for word in ['billing', 'invoice', 'payment', 'financial', 'gl']):
                patterns['business_domains']['financial'].append(proc_name)
            elif any(word in name_lower for word in ['inventory', 'stock', 'item', 'product']):
                patterns['business_domains']['inventory'].append(proc_name)
            elif any(word in name_lower for word in ['insurance', 'carrier', 'claim', 'benefit']):
                patterns['business_domains']['insurance'].append(proc_name)
            elif any(word in name_lower for word in ['schedule', 'appointment', 'calendar']):
                patterns['business_domains']['scheduling'].append(proc_name)
            elif any(word in name_lower for word in ['employee', 'user', 'security', 'role']):
                patterns['business_domains']['administrative'].append(proc_name)
            
            # Action verb analysis
            if any(word in name_lower for word in ['get', 'select', 'retrieve', 'find']):
                patterns['action_verbs']['read'].append(proc_name)
            elif any(word in name_lower for word in ['insert', 'create', 'add', 'new']):
                patterns['action_verbs']['create'].append(proc_name)
            elif any(word in name_lower for word in ['update', 'modify', 'change', 'edit']):
                patterns['action_verbs']['update'].append(proc_name)
            elif any(word in name_lower for word in ['delete', 'remove', 'drop']):
                patterns['action_verbs']['delete'].append(proc_name)
            elif any(word in name_lower for word in ['process', 'execute', 'run', 'perform']):
                patterns['action_verbs']['process'].append(proc_name)
        
        # Convert to serializable format
        self.deep_insights['naming_patterns'] = {
            'prefixes': dict(patterns['prefixes']),
            'suffixes': dict(patterns['suffixes']),
            'business_domains': {k: len(v) for k, v in patterns['business_domains'].items()},
            'action_verbs': {k: len(v) for k, v in patterns['action_verbs'].items()},
            'domain_procedures': {k: v[:10] for k, v in patterns['business_domains'].items()}  # Top 10 examples
        }
        
        print(f"📊 Analyzed naming patterns across {len(df)} procedures")
        return df
    
    def analyze_parameters_comprehensive(self):
        """Comprehensive parameter analysis"""
        print("🔧 COMPREHENSIVE PARAMETER ANALYSIS...")
        
        query = """
        SELECT 
            p.name AS procedure_name,
            par.name AS parameter_name,
            t.name AS data_type,
            par.max_length,
            par.precision,
            par.scale,
            par.is_output,
            par.has_default_value,
            par.default_value,
            par.parameter_id
        FROM sys.procedures p
        INNER JOIN sys.parameters par ON p.object_id = par.object_id
        INNER JOIN sys.types t ON par.user_type_id = t.user_type_id
        WHERE p.is_ms_shipped = 0
        ORDER BY p.name, par.parameter_id
        """
        
        df = pd.read_sql(query, self.connection)
        
        if len(df) == 0:
            print("⚠️ No parameters found - procedures may not have parameters or access is restricted")
            self.deep_insights['parameter_analysis'] = {'status': 'no_parameters_found'}
            return df
        
        # Parameter analysis
        param_insights = {
            'total_parameters': len(df),
            'procedures_with_params': df['procedure_name'].nunique(),
            'avg_params_per_procedure': len(df) / df['procedure_name'].nunique(),
            'data_type_distribution': df['data_type'].value_counts().to_dict(),
            'output_parameters': len(df[df['is_output'] == True]),
            'parameters_with_defaults': len(df[df['has_default_value'] == True]),
            'common_parameter_names': df['parameter_name'].value_counts().head(20).to_dict(),
            'parameter_patterns': self._analyze_parameter_patterns(df)
        }
        
        self.deep_insights['parameter_analysis'] = param_insights
        print(f"📊 Analyzed {len(df)} parameters across {df['procedure_name'].nunique()} procedures")
        return df
    
    def _analyze_parameter_patterns(self, df):
        """Analyze parameter naming patterns"""
        patterns = defaultdict(list)
        
        for _, row in df.iterrows():
            param_name = row['parameter_name'].lower()
            proc_name = row['procedure_name']
            
            if 'id' in param_name:
                patterns['id_parameters'].append(f"{proc_name}.{row['parameter_name']}")
            if 'date' in param_name:
                patterns['date_parameters'].append(f"{proc_name}.{row['parameter_name']}")
            if 'patient' in param_name:
                patterns['patient_parameters'].append(f"{proc_name}.{row['parameter_name']}")
            if 'office' in param_name:
                patterns['office_parameters'].append(f"{proc_name}.{row['parameter_name']}")
        
        return {k: len(v) for k, v in patterns.items()}
    
    def analyze_business_intelligence_opportunities(self):
        """Identify BI and analytics opportunities"""
        print("💡 IDENTIFYING BUSINESS INTELLIGENCE OPPORTUNITIES...")
        
        # Based on naming patterns, identify key analytical opportunities
        bi_opportunities = {
            'patient_analytics': {
                'description': 'Patient lifecycle and outcome analytics',
                'procedures': [proc for proc in self.deep_insights.get('naming_patterns', {}).get('domain_procedures', {}).get('clinical', [])],
                'potential_kpis': ['Patient satisfaction', 'Treatment outcomes', 'Visit frequency', 'Revenue per patient']
            },
            'financial_analytics': {
                'description': 'Revenue cycle and financial performance',
                'procedures': [proc for proc in self.deep_insights.get('naming_patterns', {}).get('domain_procedures', {}).get('financial', [])],
                'potential_kpis': ['Revenue trends', 'Payment cycles', 'Outstanding AR', 'Profitability by service']
            },
            'operational_analytics': {
                'description': 'Operational efficiency and resource utilization',
                'procedures': [proc for proc in self.deep_insights.get('naming_patterns', {}).get('domain_procedures', {}).get('scheduling', [])],
                'potential_kpis': ['Appointment utilization', 'Staff productivity', 'Resource allocation', 'Wait times']
            },
            'inventory_analytics': {
                'description': 'Inventory management and product performance',
                'procedures': [proc for proc in self.deep_insights.get('naming_patterns', {}).get('domain_procedures', {}).get('inventory', [])],
                'potential_kpis': ['Inventory turnover', 'Product profitability', 'Stock levels', 'Supplier performance']
            }
        }
        
        self.deep_insights['business_intelligence'] = bi_opportunities
        print("📊 Identified 4 major BI opportunity areas")
    
    def create_comprehensive_dashboard(self):
        """Create comprehensive analysis dashboard"""
        print("📊 CREATING COMPREHENSIVE DASHBOARD...")
        
        dashboard_code = '''
import streamlit as st
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Procedure Deep Dive", layout="wide", page_icon="🔍")
st.title("🔍 Stored Procedure Deep Dive Analysis")

@st.cache_data
def load_insights():
    try:
        with open('docs/procedure_deep_insights.json', 'r') as f:
            return json.load(f)
    except:
        return {}

insights = load_insights()

if insights:
    # Overview metrics
    st.header("📊 Overview")
    
    if 'procedure_inventory' in insights:
        inv = insights['procedure_inventory']
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Procedures", inv.get('total_procedures', 0))
        with col2:
            st.metric("With Source Code", inv.get('with_source_code', 0))
        with col3:
            st.metric("Schemas", len(inv.get('schemas', {})))
        with col4:
            st.metric("Avg Definition Length", f"{inv.get('avg_definition_length', 0):.0f}")
        
        # Complexity distribution
        if 'complexity_distribution' in inv:
            st.subheader("Complexity Distribution")
            complexity_data = inv['complexity_distribution']
            fig = px.pie(
                values=list(complexity_data.values()),
                names=list(complexity_data.keys()),
                title="Procedure Complexity"
            )
            st.plotly_chart(fig)
    
    # Business domains
    st.header("🏢 Business Domain Analysis")
    
    if 'naming_patterns' in insights:
        patterns = insights['naming_patterns']
        
        if 'business_domains' in patterns:
            domains_df = pd.DataFrame(
                list(patterns['business_domains'].items()),
                columns=['Domain', 'Count']
            )
            
            fig = px.bar(
                domains_df,
                x='Domain',
                y='Count',
                title="Procedures by Business Domain"
            )
            st.plotly_chart(fig)
            
            # Show example procedures for each domain
            if 'domain_procedures' in patterns:
                st.subheader("Example Procedures by Domain")
                for domain, procedures in patterns['domain_procedures'].items():
                    if procedures:
                        with st.expander(f"{domain.title()} ({len(procedures)} examples)"):
                            for proc in procedures:
                                st.write(f"• {proc}")
    
    # Action verb analysis
    if 'naming_patterns' in insights and 'action_verbs' in insights['naming_patterns']:
        st.header("⚡ Action Verb Analysis")
        
        actions = insights['naming_patterns']['action_verbs']
        actions_df = pd.DataFrame(
            list(actions.items()),
            columns=['Action', 'Count']
        )
        
        fig = px.bar(
            actions_df,
            x='Action',
            y='Count',
            title="Procedures by Action Type"
        )
        st.plotly_chart(fig)
    
    # Parameter analysis
    if 'parameter_analysis' in insights:
        st.header("🔧 Parameter Analysis")
        
        params = insights['parameter_analysis']
        
        if 'status' not in params:  # If we have actual parameter data
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Total Parameters", params.get('total_parameters', 0))
                st.metric("Procedures with Params", params.get('procedures_with_params', 0))
            
            with col2:
                st.metric("Avg Params per Procedure", f"{params.get('avg_params_per_procedure', 0):.1f}")
                st.metric("Output Parameters", params.get('output_parameters', 0))
            
            # Data type distribution
            if 'data_type_distribution' in params:
                st.subheader("Parameter Data Types")
                types_df = pd.DataFrame(
                    list(params['data_type_distribution'].items()),
                    columns=['Data Type', 'Count']
                )
                fig = px.bar(types_df, x='Data Type', y='Count')
                st.plotly_chart(fig)
        else:
            st.warning("Parameter analysis unavailable - access may be restricted")
    
    # Business Intelligence Opportunities
    if 'business_intelligence' in insights:
        st.header("💡 Business Intelligence Opportunities")
        
        bi_ops = insights['business_intelligence']
        
        for opportunity, details in bi_ops.items():
            with st.expander(f"{opportunity.replace('_', ' ').title()}"):
                st.write(f"**Description:** {details.get('description', 'N/A')}")
                
                if 'potential_kpis' in details:
                    st.write("**Potential KPIs:**")
                    for kpi in details['potential_kpis']:
                        st.write(f"• {kpi}")
                
                if 'procedures' in details and details['procedures']:
                    st.write(f"**Related Procedures ({len(details['procedures'])}):**")
                    for proc in details['procedures'][:5]:  # Show first 5
                        st.write(f"• {proc}")

else:
    st.error("No insights found. Run the deep dive analysis first.")
        '''
        
        with open('procedure_deep_dive_dashboard.py', 'w') as f:
            f.write(dashboard_code)
        
        print("✅ Dashboard created: procedure_deep_dive_dashboard.py")
    
    def run_deep_analysis(self):
        """Run complete deep dive analysis"""
        print("🚀 STARTING COMPREHENSIVE PROCEDURE DEEP DIVE")
        print("=" * 60)
        
        if not self.connect_database():
            return False
        
        try:
            # Run all analyses
            self.analyze_procedure_inventory()
            self.analyze_naming_patterns()
            self.analyze_parameters_comprehensive()
            self.analyze_business_intelligence_opportunities()
            
            # Save insights
            os.makedirs('docs', exist_ok=True)
            with open('docs/procedure_deep_insights.json', 'w') as f:
                json.dump(self.deep_insights, f, indent=2, default=str)
            
            # Create dashboard
            self.create_comprehensive_dashboard()
            
            print("\n🎉 DEEP DIVE ANALYSIS COMPLETE!")
            print("=" * 60)
            print("📊 Insights saved to: docs/procedure_deep_insights.json")
            print("🚀 Dashboard: procedure_deep_dive_dashboard.py")
            print("\n💡 KEY FINDINGS:")
            
            # Print key insights
            if 'procedure_inventory' in self.deep_insights:
                inv = self.deep_insights['procedure_inventory']
                print(f"  • {inv.get('total_procedures', 0)} total procedures analyzed")
                print(f"  • {inv.get('with_source_code', 0)} have accessible source code")
                print(f"  • {len(inv.get('schemas', {}))} schemas identified")
            
            if 'naming_patterns' in self.deep_insights:
                domains = self.deep_insights['naming_patterns'].get('business_domains', {})
                print(f"  • {len(domains)} business domains identified")
                for domain, count in domains.items():
                    print(f"    - {domain.title()}: {count} procedures")
            
            return True
            
        except Exception as e:
            print(f"❌ Analysis failed: {e}")
            return False
        
        finally:
            if self.connection:
                self.connection.close()

def main():
    analyzer = ProcedureDeepDive()
    analyzer.run_deep_analysis()

if __name__ == "__main__":
    main()
