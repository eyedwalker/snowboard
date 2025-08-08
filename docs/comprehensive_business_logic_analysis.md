# Comprehensive Business Logic Analysis
## Eyecare Database Stored Procedures

*Generated: 2025-08-08 08:21:39*

---

## Executive Summary

This analysis examined **610 stored procedures** to extract business logic across five critical domains:

- **Invoice/Billing Operations**: 482 procedures
- **Item/Product Management**: 167 procedures  
- **Employee Operations**: 116 procedures
- **Insurance Processing**: 103 procedures
- **Claims Management**: 75 procedures

---

## 🧾 Invoice & Billing Business Logic

### Key Business Patterns

#### Balance Calculations
- **19 procedures**
  - `GetPatientStatementReport`
  - `LoadOldSystemBalanceByPatient`
  - `SP_OrderAgingFilterByOfficeIds`
  - `LoadOldSystemCustomerCreditByPatient`
  - `LoadPatientOldSystemBalance`

#### Payment Processing
- **23 procedures**
  - `GetMonthlyAppliedPaymentSummary`
  - `SalesPaymentByVisionPlan`
  - `GetPOSPaymentDetailforMiscandCredit`
  - `GetPOSPaymentDetailbyTransactionDateforMailCheckandCC`
  - `GetMemberPayments`

### Key Business Formulas
- Balance = Charges - Payments
- AR = Outstanding Balances
- Revenue = Collected Amounts


---

## 📦 Item & Product Business Logic

### Key Business Patterns

#### Inventory Management
- **14 procedures**
  - `GetInventoryStatus`
  - `InventoryValuationMethodChange`
  - `GetInventoryValuationItem`
  - `SP_ProductMovementDetails`
  - `SetCLStockItemID`


---

## 🔄 Cross-Domain Workflows

### Revenue Cycle Workflow
Patient Visit → Clinical Exam → Product Orders → Invoice Generation → Insurance Claims → Payment Collection

### Product Fulfillment Workflow  
Customer Order → Inventory Check → Product Dispensing → Invoice Creation → Payment Processing

### Insurance Processing Workflow
Eligibility Verification → Benefit Authorization → Claim Submission → Adjudication → Payment Posting

---

## 🧮 Key Business Formulas

### Financial Calculations
- Outstanding Balance = Billed Amount - (Payments + Adjustments + Write-offs)
- Patient Balance = Total Charges - Insurance Payments - Patient Payments
- Commission = Sales Amount * Commission Rate
- Discount Amount = Retail Price * Discount Percentage

### Insurance Calculations
- Patient Responsibility = Deductible + Copay + Coinsurance
- Insurance Allowable = Retail Price - Insurance Discount
- Coverage Amount = Benefit Maximum - Used Benefits
- Claim Amount = Allowable Amount - Patient Responsibility


---

## 💡 Analytics & Datamart Implications

### Recommended Fact Tables
1. **FACT_REVENUE_TRANSACTIONS** - Invoice, payment, and AR data
2. **FACT_PRODUCT_SALES** - Item sales and inventory movements  
3. **FACT_INSURANCE_CLAIMS** - Claims processing and payments
4. **FACT_EMPLOYEE_PERFORMANCE** - Commission and productivity metrics

### Key Dimensions
- **DIM_PATIENT** - Patient demographics and history
- **DIM_PRODUCT** - Items, frames, lenses, contacts
- **DIM_INSURANCE** - Carriers, plans, benefits
- **DIM_EMPLOYEE** - Providers, staff, roles
- **DIM_OFFICE** - Locations and organizational hierarchy

### Critical KPIs
- Revenue per Patient Visit
- Insurance Collection Rate  
- Product Profitability
- Employee Productivity
- Claims Approval Rate
- Days Sales Outstanding
- Inventory Turnover

---

*This analysis provides the foundation for advanced eyecare analytics and business intelligence.*
