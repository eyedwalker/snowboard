#!/usr/bin/env python3
"""
Comprehensive Database Discovery Toolkit
=======================================
Deep dive analysis of SQL Server database to extract:
- Stored procedures and functions
- Views and their dependencies
- Triggers and business logic
- Complex relationships and workflows
- Business rules embedded in code
"""

import pymssql
import os
from dotenv import load_dotenv
import pandas as pd
import json
from collections import defaultdict

# Load environment variables
load_dotenv()

class ComprehensiveDBDiscovery:
    def __init__(self):
        self.connection = None
        self.discoveries = {
            'stored_procedures': [],
            'functions': [],
            'views': [],
            'triggers': [],
            'complex_relationships': [],
            'business_logic': [],
            'workflow_patterns': []
        }
    
    def connect(self):
        """Connect to SQL Server database"""
        try:
            self.connection = pymssql.connect(
                server=os.getenv('SOURCE_DB_HOST', '10.154.10.204'),
                user=os.getenv('SOURCE_DB_USER', 'sa'),
                password=os.getenv('SOURCE_DB_PASSWORD'),
                database=os.getenv('SOURCE_DB_DATABASE', 'blink_dev1'),
                port=int(os.getenv('SOURCE_DB_PORT', '1433')),
                timeout=30
            )
            print("✅ Connected to SQL Server database")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def discover_stored_procedures(self):
        """Discover all stored procedures and their metadata"""
        print("\n🔍 DISCOVERING STORED PROCEDURES...")
        
        query = """
        SELECT 
            SCHEMA_NAME(p.schema_id) AS schema_name,
            p.name AS procedure_name,
            p.type_desc,
            p.create_date,
            p.modify_date,
            m.definition
        FROM sys.procedures p
        LEFT JOIN sys.sql_modules m ON p.object_id = m.object_id
        WHERE p.is_ms_shipped = 0
        ORDER BY p.name
        """
        
        try:
            df = pd.read_sql(query, self.connection)
            self.discoveries['stored_procedures'] = df.to_dict('records')
            print(f"📊 Found {len(df)} stored procedures")
            
            # Analyze procedure patterns
            self._analyze_procedure_patterns(df)
            
            return df
        except Exception as e:
            print(f"❌ Error discovering procedures: {e}")
            return pd.DataFrame()
    
    def discover_functions(self):
        """Discover all user-defined functions"""
        print("\n🔍 DISCOVERING FUNCTIONS...")
        
        query = """
        SELECT 
            SCHEMA_NAME(f.schema_id) AS schema_name,
            f.name AS function_name,
            f.type_desc,
            f.create_date,
            f.modify_date,
            m.definition
        FROM sys.objects f
        LEFT JOIN sys.sql_modules m ON f.object_id = m.object_id
        WHERE f.type IN ('FN', 'IF', 'TF') -- Scalar, Inline Table, Table-valued
        AND f.is_ms_shipped = 0
        ORDER BY f.name
        """
        
        try:
            df = pd.read_sql(query, self.connection)
            self.discoveries['functions'] = df.to_dict('records')
            print(f"📊 Found {len(df)} functions")
            return df
        except Exception as e:
            print(f"❌ Error discovering functions: {e}")
            return pd.DataFrame()
    
    def discover_views(self):
        """Discover all views and their dependencies"""
        print("\n🔍 DISCOVERING VIEWS...")
        
        query = """
        SELECT 
            SCHEMA_NAME(v.schema_id) AS schema_name,
            v.name AS view_name,
            v.create_date,
            v.modify_date,
            m.definition
        FROM sys.views v
        LEFT JOIN sys.sql_modules m ON v.object_id = m.object_id
        WHERE v.is_ms_shipped = 0
        ORDER BY v.name
        """
        
        try:
            df = pd.read_sql(query, self.connection)
            self.discoveries['views'] = df.to_dict('records')
            print(f"📊 Found {len(df)} views")
            return df
        except Exception as e:
            print(f"❌ Error discovering views: {e}")
            return pd.DataFrame()
    
    def discover_triggers(self):
        """Discover all triggers and their business logic"""
        print("\n🔍 DISCOVERING TRIGGERS...")
        
        query = """
        SELECT 
            SCHEMA_NAME(t.schema_id) AS schema_name,
            OBJECT_NAME(t.parent_id) AS table_name,
            t.name AS trigger_name,
            t.type_desc,
            t.create_date,
            t.modify_date,
            m.definition
        FROM sys.triggers t
        LEFT JOIN sys.sql_modules m ON t.object_id = m.object_id
        WHERE t.is_ms_shipped = 0
        ORDER BY OBJECT_NAME(t.parent_id), t.name
        """
        
        try:
            df = pd.read_sql(query, self.connection)
            self.discoveries['triggers'] = df.to_dict('records')
            print(f"📊 Found {len(df)} triggers")
            return df
        except Exception as e:
            print(f"❌ Error discovering triggers: {e}")
            return pd.DataFrame()
    
    def analyze_business_logic_patterns(self):
        """Analyze stored procedures for business logic patterns"""
        print("\n🧠 ANALYZING BUSINESS LOGIC PATTERNS...")
        
        patterns = {
            'revenue_cycle': ['invoice', 'billing', 'payment', 'pos', 'transaction'],
            'insurance': ['insurance', 'carrier', 'plan', 'eligibility', 'claim'],
            'clinical': ['exam', 'patient', 'appointment', 'prescription'],
            'inventory': ['inventory', 'stock', 'item', 'product'],
            'financial': ['gl', 'accounting', 'revenue', 'ar', 'payment'],
            'reporting': ['report', 'summary', 'analytics', 'kpi']
        }
        
        business_logic = defaultdict(list)
        
        for proc in self.discoveries['stored_procedures']:
            proc_name = proc['procedure_name'].lower()
            definition = (proc['definition'] or '').lower()
            
            for category, keywords in patterns.items():
                if any(keyword in proc_name or keyword in definition for keyword in keywords):
                    business_logic[category].append({
                        'name': proc['procedure_name'],
                        'schema': proc['schema_name'],
                        'category': category,
                        'definition_snippet': (proc['definition'] or '')[:500]
                    })
        
        self.discoveries['business_logic'] = dict(business_logic)
        
        print("📈 BUSINESS LOGIC CATEGORIES FOUND:")
        for category, procs in business_logic.items():
            print(f"  • {category.upper()}: {len(procs)} procedures")
    
    def discover_complex_relationships(self):
        """Discover complex relationships through procedure analysis"""
        print("\n🔗 DISCOVERING COMPLEX RELATIONSHIPS...")
        
        # Analyze procedures that reference multiple key tables
        key_tables = ['patient', 'orders', 'invoice', 'item', 'insurance', 'billing']
        complex_procs = []
        
        for proc in self.discoveries['stored_procedures']:
            definition = (proc['definition'] or '').lower()
            referenced_tables = [table for table in key_tables if table in definition]
            
            if len(referenced_tables) >= 3:  # Procedures touching 3+ key tables
                complex_procs.append({
                    'procedure': proc['procedure_name'],
                    'schema': proc['schema_name'],
                    'referenced_tables': referenced_tables,
                    'complexity_score': len(referenced_tables)
                })
        
        self.discoveries['complex_relationships'] = sorted(
            complex_procs, 
            key=lambda x: x['complexity_score'], 
            reverse=True
        )
        
        print(f"📊 Found {len(complex_procs)} complex procedures")
    
    def _analyze_procedure_patterns(self, df):
        """Analyze naming patterns and categories"""
        patterns = defaultdict(int)
        
        for proc_name in df['procedure_name']:
            # Extract prefixes (common naming conventions)
            if '_' in proc_name:
                prefix = proc_name.split('_')[0]
                patterns[f"prefix_{prefix}"] += 1
            
            # Look for common suffixes
            if proc_name.endswith('_Insert'):
                patterns['suffix_Insert'] += 1
            elif proc_name.endswith('_Update'):
                patterns['suffix_Update'] += 1
            elif proc_name.endswith('_Delete'):
                patterns['suffix_Delete'] += 1
            elif proc_name.endswith('_Select'):
                patterns['suffix_Select'] += 1
        
        print("📊 PROCEDURE NAMING PATTERNS:")
        for pattern, count in sorted(patterns.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  • {pattern}: {count}")
    
    def generate_comprehensive_report(self):
        """Generate comprehensive discovery report"""
        print("\n📋 GENERATING COMPREHENSIVE REPORT...")
        
        report = {
            'summary': {
                'stored_procedures': len(self.discoveries['stored_procedures']),
                'functions': len(self.discoveries['functions']),
                'views': len(self.discoveries['views']),
                'triggers': len(self.discoveries['triggers']),
                'business_logic_categories': len(self.discoveries['business_logic']),
                'complex_procedures': len(self.discoveries['complex_relationships'])
            },
            'discoveries': self.discoveries
        }
        
        # Save detailed report
        with open('docs/comprehensive_db_discovery_report.json', 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print("✅ Comprehensive report saved to docs/comprehensive_db_discovery_report.json")
        return report
    
    def run_full_discovery(self):
        """Run complete database discovery process"""
        print("🚀 STARTING COMPREHENSIVE DATABASE DISCOVERY")
        print("=" * 60)
        
        if not self.connect():
            return None
        
        try:
            # Discover all database objects
            self.discover_stored_procedures()
            self.discover_functions()
            self.discover_views()
            self.discover_triggers()
            
            # Analyze patterns and relationships
            self.analyze_business_logic_patterns()
            self.discover_complex_relationships()
            
            # Generate comprehensive report
            report = self.generate_comprehensive_report()
            
            print("\n🎉 DISCOVERY COMPLETE!")
            print("=" * 60)
            print("📊 SUMMARY:")
            for key, value in report['summary'].items():
                print(f"  • {key.replace('_', ' ').title()}: {value}")
            
            return report
            
        except Exception as e:
            print(f"❌ Discovery failed: {e}")
            return None
        
        finally:
            if self.connection:
                self.connection.close()

def main():
    discovery = ComprehensiveDBDiscovery()
    report = discovery.run_full_discovery()
    
    if report:
        print("\n🎯 KEY INSIGHTS:")
        print("• Stored procedures contain embedded business logic")
        print("• Functions may reveal calculation methods")
        print("• Views show common data access patterns")
        print("• Triggers reveal automated business rules")
        print("• Complex procedures show workflow patterns")

if __name__ == "__main__":
    main()
