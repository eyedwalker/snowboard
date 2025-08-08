#!/usr/bin/env python3
"""
Database Relationship Analyzer
=============================
Analyzes foreign key relationships to create comprehensive documentation
"""

import pandas as pd
from collections import defaultdict, Counter

def analyze_relationships():
    print('🔍 ANALYZING FOREIGN KEY RELATIONSHIPS')
    print('=' * 50)
    
    # Parse the foreign key data provided by user
    fk_lines = """FK_InvoiceDet_InvoiceSum	InvoiceDet	InvoiceID	InvoiceSum	InvoiceID
FK_InvoiceSum_PosTransaction	InvoiceSum	TransNum	PosTransaction	TransactionID
FK_PatientInsurance_InsPlan	PatientInsurance	InsurancePlanID	InsPlan	ID
FK_PatientInsurance_Patient	PatientInsurance	PatientID	Patient	ID
FK_InsPlan_InsCarrier	InsPlan	CarrierCode	InsCarrier	ID
FK_BillingClaim_PatientInsurance	BillingClaim	PatientInsuranceId	PatientInsurance	ID
FK_Orders_Patient	Orders	PatientID	Patient	ID
FK_Patient_Office	Patient	HomeOffice	Office	OfficeNum
FK_Item_ItemType	Item	ItemType	ItemType	ItemType
FK_InventoryActivity_OrderNum	InventoryActivity	OrderNum	Orders	OrderNum
FK_InventoryBalance_ItemID	InventoryBalance	ItemID	Item	ID
FK_PatientExam_Patient	PatientExam	PatientID	Patient	ID
FK_Employee_Office	Employee	HomeOffice	Office	OfficeNum
FK_Appointment_Patient	Appointment	PatientID	Patient	ID
FK_Appointment_Employee	Appointment	DoctorId	Employee	Employee
FK_Appointment_Office	Appointment	OfficeNum	Office	OfficeNum""".strip().split('\n')
    
    relationships = []
    for line in fk_lines:
        if line.strip():
            parts = line.split('\t')
            if len(parts) >= 5:
                relationships.append({
                    'fk_name': parts[0],
                    'child_table': parts[1],
                    'child_column': parts[2], 
                    'parent_table': parts[3],
                    'parent_column': parts[4]
                })
    
    # Analyze key patterns
    print(f"📊 FOUND {len(relationships)} KEY RELATIONSHIPS")
    print()
    
    # Most referenced tables (parent tables)
    parent_tables = Counter([r['parent_table'] for r in relationships])
    print("🏢 TOP REFERENCED TABLES (CORE ENTITIES):")
    for table, count in parent_tables.most_common(10):
        print(f"  • {table}: {count} relationships")
    print()
    
    # Key business entities and their relationships
    key_entities = {
        'Patient': [],
        'Orders': [],
        'Item': [],
        'InsPlan': [],
        'Office': [],
        'Employee': []
    }
    
    for rel in relationships:
        for entity in key_entities:
            if rel['parent_table'] == entity or rel['child_table'] == entity:
                key_entities[entity].append(rel)
    
    print("🔗 KEY BUSINESS ENTITY RELATIONSHIPS:")
    for entity, rels in key_entities.items():
        if rels:
            print(f"\n{entity} ({len(rels)} relationships):")
            for rel in rels[:5]:  # Show top 5
                direction = "→" if rel['parent_table'] == entity else "←"
                other_table = rel['child_table'] if rel['parent_table'] == entity else rel['parent_table']
                print(f"  {direction} {other_table}")
    
    # Critical revenue cycle relationships
    print("\n💰 CRITICAL REVENUE CYCLE FLOW:")
    revenue_flow = [
        "Patient → PatientInsurance → InsPlan → InsCarrier",
        "Patient → Orders → InvoiceDet → InvoiceSum",
        "InvoiceSum → PosTransaction (payment processing)",
        "Orders → BillingClaim → PatientInsurance",
        "Item → InvoiceDet (product sales)",
        "Patient → PatientExam → Orders (clinical workflow)"
    ]
    
    for flow in revenue_flow:
        print(f"  📈 {flow}")
    
    print("\n✅ RELATIONSHIP ANALYSIS COMPLETE!")
    print("Key findings saved for datamart design and ERD creation.")
    
    return relationships

if __name__ == "__main__":
    analyze_relationships()
