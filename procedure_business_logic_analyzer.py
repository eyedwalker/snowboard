#!/usr/bin/env python3
"""
Stored Procedure Business Logic Analyzer
=======================================
Analyzes the actual stored procedure content to understand:
- Business calculations and formulas
- Data transformations and workflows
- Integration patterns and external systems
- Financial logic and revenue calculations
- Clinical workflows and patient management
"""

import pandas as pd
import re
import json
from collections import defaultdict, Counter
import os

class ProcedureBusinessLogicAnalyzer:
    def __init__(self):
        self.procedures_df = None
        self.business_insights = {
            'financial_calculations': {},
            'clinical_workflows': {},
            'integration_patterns': {},
            'data_transformations': {},
            'business_rules': {},
            'performance_patterns': {},
            'key_procedures': {}
        }
    
    def load_procedure_data(self):
        """Load the stored procedure data with definitions"""
        try:
            self.procedures_df = pd.read_csv('docs/stored_procedures_sqlalchemy.csv')
            print(f"📋 Loaded {len(self.procedures_df)} stored procedures with definitions")
            
            # Filter procedures with actual content
            procedures_with_content = self.procedures_df[
                (self.procedures_df['source_availability'] == 'Has Source') & 
                (self.procedures_df['definition_length'] > 100)
            ]
            
            print(f"🔍 {len(procedures_with_content)} procedures have substantial business logic")
            return True
            
        except Exception as e:
            print(f"❌ Error loading procedure data: {e}")
            return False
    
    def analyze_financial_calculations(self):
        """Analyze financial calculation patterns"""
        print("💰 ANALYZING FINANCIAL CALCULATIONS...")
        
        financial_keywords = [
            'payment', 'billing', 'invoice', 'balance', 'amount', 'total', 
            'revenue', 'commission', 'discount', 'tax', 'copay', 'deductible'
        ]
        
        financial_procedures = []
        calculation_patterns = defaultdict(list)
        
        for _, row in self.procedures_df.iterrows():
            proc_name = row['procedure_name']
            definition = str(row['definition_preview']).lower() if pd.notna(row['definition_preview']) else ''
            
            if any(keyword in definition for keyword in financial_keywords):
                financial_procedures.append({
                    'name': proc_name,
                    'definition_length': row['definition_length'],
                    'preview': row['definition_preview'][:200] if pd.notna(row['definition_preview']) else ''
                })
                
                # Extract specific calculation patterns
                if 'sum(' in definition or 'total' in definition:
                    calculation_patterns['summation'].append(proc_name)
                if 'balance' in definition:
                    calculation_patterns['balance_calculation'].append(proc_name)
                if 'commission' in definition:
                    calculation_patterns['commission_calculation'].append(proc_name)
                if 'discount' in definition:
                    calculation_patterns['discount_calculation'].append(proc_name)
        
        self.business_insights['financial_calculations'] = {
            'total_financial_procedures': len(financial_procedures),
            'key_procedures': financial_procedures[:10],  # Top 10
            'calculation_patterns': dict(calculation_patterns),
            'business_value': 'Critical revenue cycle and financial operations'
        }
        
        print(f"📊 Found {len(financial_procedures)} financial procedures with calculation logic")
    
    def analyze_clinical_workflows(self):
        """Analyze clinical workflow patterns"""
        print("🏥 ANALYZING CLINICAL WORKFLOWS...")
        
        clinical_keywords = [
            'patient', 'exam', 'diagnosis', 'prescription', 'appointment',
            'clinical', 'medical', 'treatment', 'provider', 'doctor'
        ]
        
        clinical_procedures = []
        workflow_patterns = defaultdict(list)
        
        for _, row in self.procedures_df.iterrows():
            proc_name = row['procedure_name']
            definition = str(row['definition_preview']).lower() if pd.notna(row['definition_preview']) else ''
            
            if any(keyword in definition for keyword in clinical_keywords):
                clinical_procedures.append({
                    'name': proc_name,
                    'definition_length': row['definition_length'],
                    'preview': row['definition_preview'][:200] if pd.notna(row['definition_preview']) else ''
                })
                
                # Extract workflow patterns
                if 'appointment' in definition:
                    workflow_patterns['appointment_management'].append(proc_name)
                if 'exam' in definition:
                    workflow_patterns['examination_workflow'].append(proc_name)
                if 'prescription' in definition:
                    workflow_patterns['prescription_management'].append(proc_name)
                if 'patient' in definition and 'create' in definition:
                    workflow_patterns['patient_registration'].append(proc_name)
        
        self.business_insights['clinical_workflows'] = {
            'total_clinical_procedures': len(clinical_procedures),
            'key_procedures': clinical_procedures[:10],
            'workflow_patterns': dict(workflow_patterns),
            'business_value': 'Patient care and clinical operations'
        }
        
        print(f"📊 Found {len(clinical_procedures)} clinical workflow procedures")
    
    def analyze_integration_patterns(self):
        """Analyze external system integration patterns"""
        print("🔌 ANALYZING INTEGRATION PATTERNS...")
        
        integration_keywords = [
            'edi', 'xml', 'json', 'api', 'import', 'export', 'interface',
            'external', 'third', 'integration', 'sync', 'transmit'
        ]
        
        integration_procedures = []
        integration_types = defaultdict(list)
        
        for _, row in self.procedures_df.iterrows():
            proc_name = row['procedure_name']
            definition = str(row['definition_preview']).lower() if pd.notna(row['definition_preview']) else ''
            
            if any(keyword in definition for keyword in integration_keywords):
                integration_procedures.append({
                    'name': proc_name,
                    'definition_length': row['definition_length'],
                    'preview': row['definition_preview'][:200] if pd.notna(row['definition_preview']) else ''
                })
                
                # Categorize integration types
                if 'edi' in definition:
                    integration_types['edi_processing'].append(proc_name)
                if 'import' in definition:
                    integration_types['data_import'].append(proc_name)
                if 'export' in definition:
                    integration_types['data_export'].append(proc_name)
                if 'sync' in definition:
                    integration_types['data_synchronization'].append(proc_name)
        
        self.business_insights['integration_patterns'] = {
            'total_integration_procedures': len(integration_procedures),
            'key_procedures': integration_procedures[:10],
            'integration_types': dict(integration_types),
            'business_value': 'External system connectivity and data exchange'
        }
        
        print(f"📊 Found {len(integration_procedures)} integration procedures")
    
    def analyze_complex_business_rules(self):
        """Analyze complex business rules and validations"""
        print("📋 ANALYZING BUSINESS RULES...")
        
        rule_keywords = [
            'case when', 'if', 'else', 'validate', 'check', 'rule',
            'condition', 'criteria', 'requirement', 'constraint'
        ]
        
        rule_procedures = []
        rule_patterns = defaultdict(list)
        
        for _, row in self.procedures_df.iterrows():
            proc_name = row['procedure_name']
            definition = str(row['definition_preview']).lower() if pd.notna(row['definition_preview']) else ''
            
            if any(keyword in definition for keyword in rule_keywords):
                rule_procedures.append({
                    'name': proc_name,
                    'definition_length': row['definition_length'],
                    'complexity': 'High' if row['definition_length'] > 5000 else 'Medium',
                    'preview': row['definition_preview'][:200] if pd.notna(row['definition_preview']) else ''
                })
                
                # Categorize rule types
                if 'case when' in definition:
                    rule_patterns['conditional_logic'].append(proc_name)
                if 'validate' in definition:
                    rule_patterns['validation_rules'].append(proc_name)
                if 'check' in definition:
                    rule_patterns['data_checks'].append(proc_name)
        
        self.business_insights['business_rules'] = {
            'total_rule_procedures': len(rule_procedures),
            'key_procedures': rule_procedures[:10],
            'rule_patterns': dict(rule_patterns),
            'business_value': 'Data validation and business logic enforcement'
        }
        
        print(f"📊 Found {len(rule_procedures)} procedures with complex business rules")
    
    def identify_key_procedures(self):
        """Identify the most important procedures based on complexity and business impact"""
        print("⭐ IDENTIFYING KEY PROCEDURES...")
        
        # Sort by definition length (complexity) and recent modification
        key_procedures = []
        
        for _, row in self.procedures_df.iterrows():
            if row['source_availability'] == 'Has Source' and row['definition_length'] > 1000:
                
                # Calculate importance score
                importance_score = 0
                proc_name = row['procedure_name'].lower()
                definition = str(row['definition_preview']).lower() if pd.notna(row['definition_preview']) else ''
                
                # Business impact scoring
                if any(word in proc_name for word in ['payment', 'billing', 'revenue']):
                    importance_score += 10  # Financial operations
                if any(word in proc_name for word in ['patient', 'clinical', 'exam']):
                    importance_score += 8   # Clinical operations
                if any(word in proc_name for word in ['schedule', 'appointment']):
                    importance_score += 6   # Operational efficiency
                if any(word in proc_name for word in ['inventory', 'stock']):
                    importance_score += 5   # Inventory management
                
                # Complexity scoring
                if row['definition_length'] > 10000:
                    importance_score += 5
                elif row['definition_length'] > 5000:
                    importance_score += 3
                
                key_procedures.append({
                    'name': row['procedure_name'],
                    'importance_score': importance_score,
                    'definition_length': row['definition_length'],
                    'last_modified': row['modify_date'],
                    'business_area': self._categorize_business_area(proc_name),
                    'preview': row['definition_preview'][:300] if pd.notna(row['definition_preview']) else ''
                })
        
        # Sort by importance score
        key_procedures.sort(key=lambda x: x['importance_score'], reverse=True)
        
        self.business_insights['key_procedures'] = {
            'top_20_procedures': key_procedures[:20],
            'total_analyzed': len(key_procedures),
            'scoring_criteria': 'Business impact + complexity + recent modifications'
        }
        
        print(f"⭐ Identified top {min(20, len(key_procedures))} key procedures")
    
    def _categorize_business_area(self, proc_name):
        """Categorize procedure by business area"""
        if any(word in proc_name for word in ['payment', 'billing', 'invoice', 'revenue']):
            return 'Financial Operations'
        elif any(word in proc_name for word in ['patient', 'clinical', 'exam', 'diagnosis']):
            return 'Clinical Operations'
        elif any(word in proc_name for word in ['schedule', 'appointment', 'calendar']):
            return 'Scheduling & Operations'
        elif any(word in proc_name for word in ['inventory', 'stock', 'item']):
            return 'Inventory Management'
        elif any(word in proc_name for word in ['insurance', 'claim', 'carrier']):
            return 'Insurance Processing'
        else:
            return 'Other Operations'
    
    def create_business_logic_dashboard(self):
        """Create dashboard for business logic insights"""
        print("📊 CREATING BUSINESS LOGIC DASHBOARD...")
        
        dashboard_code = '''
import streamlit as st
import json
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Business Logic Analysis", layout="wide", page_icon="🧠")
st.title("🧠 Stored Procedure Business Logic Analysis")
st.markdown("*Deep analysis of actual business logic from 610+ stored procedures*")

@st.cache_data
def load_business_insights():
    try:
        with open('docs/business_logic_insights.json', 'r') as f:
            return json.load(f)
    except:
        return {}

insights = load_business_insights()

if insights:
    # Overview metrics
    st.header("📊 Business Logic Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        financial_count = insights.get('financial_calculations', {}).get('total_financial_procedures', 0)
        st.metric("Financial Procedures", financial_count)
    
    with col2:
        clinical_count = insights.get('clinical_workflows', {}).get('total_clinical_procedures', 0)
        st.metric("Clinical Procedures", clinical_count)
    
    with col3:
        integration_count = insights.get('integration_patterns', {}).get('total_integration_procedures', 0)
        st.metric("Integration Procedures", integration_count)
    
    with col4:
        rules_count = insights.get('business_rules', {}).get('total_rule_procedures', 0)
        st.metric("Business Rule Procedures", rules_count)
    
    # Key Procedures
    st.header("⭐ Most Important Procedures")
    
    if 'key_procedures' in insights:
        key_procs = insights['key_procedures'].get('top_20_procedures', [])
        
        if key_procs:
            # Create DataFrame for visualization
            df = pd.DataFrame(key_procs)
            
            # Business area distribution
            if 'business_area' in df.columns:
                area_counts = df['business_area'].value_counts()
                fig = px.pie(
                    values=area_counts.values,
                    names=area_counts.index,
                    title="Key Procedures by Business Area"
                )
                st.plotly_chart(fig)
            
            # Top procedures table
            st.subheader("Top 10 Most Important Procedures")
            display_df = df[['name', 'business_area', 'importance_score', 'definition_length']].head(10)
            st.dataframe(display_df, use_container_width=True)
            
            # Detailed view
            st.subheader("Procedure Details")
            selected_proc = st.selectbox("Select a procedure to view details:", 
                                       [proc['name'] for proc in key_procs[:10]])
            
            if selected_proc:
                proc_details = next((p for p in key_procs if p['name'] == selected_proc), None)
                if proc_details:
                    st.write(f"**Business Area:** {proc_details.get('business_area', 'N/A')}")
                    st.write(f"**Importance Score:** {proc_details.get('importance_score', 0)}")
                    st.write(f"**Definition Length:** {proc_details.get('definition_length', 0):,} characters")
                    st.write(f"**Last Modified:** {proc_details.get('last_modified', 'N/A')}")
                    
                    if proc_details.get('preview'):
                        st.subheader("Code Preview")
                        st.code(proc_details['preview'], language='sql')
    
    # Financial Analysis
    st.header("💰 Financial Calculation Analysis")
    
    if 'financial_calculations' in insights:
        fin_data = insights['financial_calculations']
        
        st.write(f"**Total Financial Procedures:** {fin_data.get('total_financial_procedures', 0)}")
        st.write(f"**Business Value:** {fin_data.get('business_value', 'N/A')}")
        
        if 'calculation_patterns' in fin_data:
            patterns = fin_data['calculation_patterns']
            if patterns:
                st.subheader("Financial Calculation Patterns")
                for pattern, procedures in patterns.items():
                    st.write(f"**{pattern.replace('_', ' ').title()}:** {len(procedures)} procedures")
                    with st.expander(f"View {pattern} procedures"):
                        for proc in procedures[:10]:
                            st.write(f"• {proc}")
        
        if 'key_procedures' in fin_data:
            st.subheader("Key Financial Procedures")
            for proc in fin_data['key_procedures'][:5]:
                with st.expander(proc['name']):
                    st.write(f"**Length:** {proc['definition_length']:,} characters")
                    if proc.get('preview'):
                        st.code(proc['preview'], language='sql')
    
    # Clinical Analysis
    st.header("🏥 Clinical Workflow Analysis")
    
    if 'clinical_workflows' in insights:
        clinical_data = insights['clinical_workflows']
        
        st.write(f"**Total Clinical Procedures:** {clinical_data.get('total_clinical_procedures', 0)}")
        st.write(f"**Business Value:** {clinical_data.get('business_value', 'N/A')}")
        
        if 'workflow_patterns' in clinical_data:
            patterns = clinical_data['workflow_patterns']
            if patterns:
                st.subheader("Clinical Workflow Patterns")
                for pattern, procedures in patterns.items():
                    st.write(f"**{pattern.replace('_', ' ').title()}:** {len(procedures)} procedures")

else:
    st.error("No business logic insights found. Run the analyzer first.")
        '''
        
        with open('business_logic_dashboard.py', 'w') as f:
            f.write(dashboard_code)
        
        print("✅ Business logic dashboard created")
    
    def run_comprehensive_analysis(self):
        """Run complete business logic analysis"""
        print("🧠 STARTING COMPREHENSIVE BUSINESS LOGIC ANALYSIS")
        print("=" * 70)
        
        if not self.load_procedure_data():
            return False
        
        try:
            # Run all analyses
            self.analyze_financial_calculations()
            self.analyze_clinical_workflows()
            self.analyze_integration_patterns()
            self.analyze_complex_business_rules()
            self.identify_key_procedures()
            
            # Save insights
            os.makedirs('docs', exist_ok=True)
            with open('docs/business_logic_insights.json', 'w') as f:
                json.dump(self.business_insights, f, indent=2, default=str)
            
            # Create dashboard
            self.create_business_logic_dashboard()
            
            print("\n🎉 BUSINESS LOGIC ANALYSIS COMPLETE!")
            print("=" * 70)
            print("🧠 Insights saved to: docs/business_logic_insights.json")
            print("🚀 Dashboard: business_logic_dashboard.py")
            
            # Print summary
            print("\n💡 KEY BUSINESS LOGIC DISCOVERED:")
            print(f"  • {self.business_insights['financial_calculations']['total_financial_procedures']} financial calculation procedures")
            print(f"  • {self.business_insights['clinical_workflows']['total_clinical_procedures']} clinical workflow procedures")
            print(f"  • {self.business_insights['integration_patterns']['total_integration_procedures']} integration procedures")
            print(f"  • {self.business_insights['business_rules']['total_rule_procedures']} business rule procedures")
            print(f"  • {len(self.business_insights['key_procedures']['top_20_procedures'])} key procedures identified")
            
            return True
            
        except Exception as e:
            print(f"❌ Business logic analysis failed: {e}")
            return False

def main():
    analyzer = ProcedureBusinessLogicAnalyzer()
    analyzer.run_comprehensive_analysis()

if __name__ == "__main__":
    main()
