#!/usr/bin/env python3
"""
Comprehensive fix for V1.3 dashboard column name case mismatches.
The query returns uppercase column names but dashboard code uses mixed case.
"""

import re

# Read the current dashboard file
dashboard_file = '/Users/daviwa2@vsp.com/Analytics and Insights/CascadeProjects/snowflake-eyecare-platform/src/analytics/v13_revenue_cycle_dashboard.py'

with open(dashboard_file, 'r') as f:
    content = f.read()

# Define column name mappings (mixed case -> UPPERCASE)
column_mappings = {
    # Financial columns
    "'Billed'": "'BILLED'",
    "'Insurance_Payment'": "'INSURANCE_PAYMENT'",
    "'Patient_Payment'": "'PATIENT_PAYMENT'",
    "'Ins_Total_Balance'": "'INS_TOTAL_BALANCE'",
    "'Patient_Balance'": "'PATIENT_BALANCE'",
    "'Insurance_AR'": "'INSURANCE_AR'",
    "'Adjustment'": "'ADJUSTMENT'",
    "'RefundAdjustment'": "'REFUNDADJUSTMENT'",
    "'WriteOff_All'": "'WRITEOFF_ALL'",
    "'Collections'": "'COLLECTIONS'",
    
    # Other columns that might be mixed case
    "'LocationName'": "'LOCATIONNAME'",
    "'OrderId'": "'ORDERID'",
    "'DateOfService'": "'DATEOFSERVICE'",
    "'InsuranceName'": "'INSURANCENAME'",
    "'PlanName'": "'PLANNAME'",
    "'ClaimId'": "'CLAIMID'",
    "'OrderStatus'": "'ORDERSTATUS'",
    "'ClaimStatus'": "'CLAIMSTATUS'",
    "'ClaimNotes'": "'CLAIMNOTES'",
    "'LocationNum'": "'LOCATIONNUM'",
    "'source'": "'SOURCE'",
    
    # Bracket notation
    "['Billed']": "['BILLED']",
    "['Insurance_Payment']": "['INSURANCE_PAYMENT']",
    "['Patient_Payment']": "['PATIENT_PAYMENT']",
    "['Ins_Total_Balance']": "['INS_TOTAL_BALANCE']",
    "['Patient_Balance']": "['PATIENT_BALANCE']",
    "['Insurance_AR']": "['INSURANCE_AR']",
    "['Adjustment']": "['ADJUSTMENT']",
    "['RefundAdjustment']": "['REFUNDADJUSTMENT']",
    "['WriteOff_All']": "['WRITEOFF_ALL']",
    "['Collections']": "['COLLECTIONS']",
    "['LocationName']": "['LOCATIONNAME']",
    "['OrderId']": "['ORDERID']",
    "['DateOfService']": "['DATEOFSERVICE']",
    "['source']": "['SOURCE']",
}

print("🔧 Fixing V1.3 dashboard column name case mismatches...")
print(f"📁 File: {dashboard_file}")

# Apply all mappings
original_content = content
for old_name, new_name in column_mappings.items():
    if old_name in content:
        content = content.replace(old_name, new_name)
        print(f"✅ Fixed: {old_name} -> {new_name}")

# Check if any changes were made
if content != original_content:
    # Write the fixed content back
    with open(dashboard_file, 'w') as f:
        f.write(content)
    print(f"\n🎉 Successfully fixed column name case mismatches!")
    print(f"📊 Dashboard should now work with uppercase column names from V1.3 query")
else:
    print(f"\n✅ No column name fixes needed - file already correct")

print(f"\n🚀 Ready to test updated V1.3 dashboard!")
