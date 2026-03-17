# Encompass Analytics Data Guide — Agent Reference

This document is the authoritative reference for the Encompass Assist analytics agent. It describes the Snowflake data warehouse schema, naming conventions, common query patterns, and pre-built report views.

## Database: DEV_ANALYTICS

### Schemas

| Schema | Purpose | Views |
|--------|---------|-------|
| **DATAWAREHOUSE** | Clean business views of Encompass practice data — one view per entity | 118 views |
| **DATAMART** | Pre-aggregated fact/dimension tables for dashboards and reports | 18 views |

### Critical Naming Conventions

- **All identifiers are case-sensitive** — always double-quote: `"InvoiceSummary"`, `"Amount"`, `"LastOperationDateTime"`
- **Schema prefix required**: `DATAWAREHOUSE."InvoiceSummary"` or `DATAMART."DashboardMartFact_Billing"`
- **Soft deletes**: Most tables have `"DELETED_FLAG"` (values: `'Y'` or `'N'`). Always filter `WHERE "DELETED_FLAG" = 'N'` unless looking at deleted records.
- **`_Customer` column**: Internal tenant/company identifier — present in most tables. For multi-office practices, use `"OfficeId"` or `"OfficeKey"` to filter by location.
- **Amounts are stored as TEXT in some views** — always CAST: `CAST("Amount" AS FLOAT)`, `CAST("TotalAmount" AS FLOAT)`, `CAST("Quantity" AS FLOAT)`
- **Date columns**: `"LastOperationDateTime"`, `"InvoiceDate"`, `"Date"`, `"OrderDate"`, `"ServiceDate"`, `"BirthDate"` — all `TIMESTAMP_NTZ`
- **Gender**: Column is `"Sex"` with values `'M'` and `'F'` — NOT `Gender`
- **Names**: `"FirstName"`, `"LastName"` — NOT `Name`
- **IDs**: Primary keys are `"ID"` (auto-increment), business keys use entity names: `"PatientId"`, `"InvoiceSummaryId"`, `"OfficeId"`

---

## Core Tables (DATAWAREHOUSE)

### Patient Data
| View | Purpose | Key Columns |
|------|---------|-------------|
| `Patient` | Patient demographics | ID, PatientId, FirstName, LastName, Sex, BirthDate, OfficeId, HomeOfficeKey |
| `PatientAddress` | Patient addresses | PatientId, Line1, City, State, ZipCode |
| `PatientPhone` | Phone numbers | PatientId, PhoneNumber, PhoneType |
| `PatientInsurance` | Insurance links | PatientId, CarrierId, PlanId, InsuredId |
| `PatientExam` | Exam records | PatientId, ExamDate, ProviderId |
| `PatientRecall` | Recall/follow-up | PatientId, RecallDate, RecallType |

### Sales & Revenue
| View | Purpose | Key Columns |
|------|---------|-------------|
| `InvoiceSummary` | Invoice headers | InvoiceSummaryId, PatientId, TotalAmount, InvoiceDate, OfficeId, Status |
| `InvoiceDetail` | Invoice line items | InvoiceSummaryId, Amount, Quantity, ItemDescription, LastOperationDateTime |
| `InvoiceInsuranceDetail` | Insurance payments on invoices | InvoiceSummaryId, InsuranceAmount, PatientAmount |

### Orders
| View | Purpose | Key Columns |
|------|---------|-------------|
| `Order` | Lab/product orders | OrderId, PatientId, OrderDate, Status, OfficeId, DerivedStatus |
| `OrderExamDetail` | Exam details on orders | OrderId, SPH, CYL, AXIS, ADD (Rx data) |
| `OrderInsurance` | Insurance on orders | OrderId, CarrierId, AuthorizationNumber |
| `OrderType` | Order type lookup | Value (e.g., "Eyeglasses", "Contact Lenses") |

### Appointments
| View | Purpose | Key Columns |
|------|---------|-------------|
| `ScheduledAppointment` | Appointment records | Date, StartTime, Duration, IsCanceled, IsConfirmed, IsDeleted, PatientId, OfficeId, ProviderId |
| `ScheduledAppointmentType` | Appointment types | Value (e.g., "Comprehensive Eye Exam") |

### Products & Inventory
| View | Purpose | Key Columns |
|------|---------|-------------|
| `Item` | Product catalog | ItemId, Description, UnitPrice, ItemTypeId |
| `ItemType` | Product categories | Value (e.g., "Frame", "Lens", "Contact Lens") |
| `ItemFrame` | Frame details | Brand, Model, Color, Size, Material |
| `ItemEGLens` | Eyeglass lens details | Material, Type, Coating |
| `ItemCL` | Contact lens details | Manufacturer, Brand, BaseCurve, Diameter |

### Billing & AR
| View | Purpose | Key Columns |
|------|---------|-------------|
| `BillingTransaction` | All billing transactions | Amount, TransactionDate, TransactionTypeId, PatientId, OfficeId |
| `BillingClaim` | Insurance claims | ServiceDate, BillingDate, PatientId, ExternalClaimNumber |
| `BillingPayment` | Payments received | Amount, PaymentDate, PaymentType |
| `BillingClaimLineItem` | Claim line details | ChargeAmount, PaidAmount, CPTCode |

### Insurance
| View | Purpose | Key Columns |
|------|---------|-------------|
| `Carrier` | Insurance carriers | Name, CarrierId |
| `Plan` | Insurance plans | PlanName, CarrierId |
| `Eligibility` | Patient eligibility | PatientId, CarrierId, EffectiveDate, TermDate |
| `EraRemittance` | Electronic remittance | ClaimId, PaidAmount, AdjustmentAmount |

### Reference
| View | Purpose | Key Columns |
|------|---------|-------------|
| `Office` | Practice office locations | OfficeName, OfficeNumber, OfficeKey |
| `Employee` | Staff members | FirstName, LastName, EmployeeType, IsProvider |
| `CompanyInfo` | Company/practice info | CompanyName, TaxId |

---

## Pre-Built Report Views (DATAMART)

These views contain complex business logic already validated by the analytics team. **Use these when possible instead of writing raw SQL against DATAWAREHOUSE tables.**

### Accounts Receivable
| View | Purpose |
|------|---------|
| `ARMartFct_Billing` | AR aging by order — insurance vs patient balances, days outstanding, by carrier/plan |
| `ARMartFct_MonthlyMeasuresBilling` | Monthly AR trends — 12-month rolling window of insurance/patient balances |
| `ARMartFct_MonthlyMeasuresPOS` | Monthly POS balance trends |
| `ARMartFct_POSBalanceDetails` | POS balance details by order/patient |

### Dashboard Facts
| View | Purpose |
|------|---------|
| `DashboardMartFact_Billing` | Billing dashboard — revenue, collections, adjustments by office/provider |
| `DashboardMartFact_NetCollections` | Net collections ratio (collections / charges) |
| `DashboardMartFact_PatientDemographics` | Patient counts, demographics, new vs returning |
| `DashboardMartFact_SalesByProviderAndStaff` | Sales performance by provider and staff member |
| `DashboardMartFact_BillingClaimAging` | Claim aging buckets (0-30, 31-60, 61-90, 90+ days) |
| `DashboardMartFact_AccountsReceivable_Dev` | AR summary by office |

### Sales & Marketing
| View | Purpose |
|------|---------|
| `DashboardMartDim_CombinedInvoiceDetail` | Combined invoice detail with patient demographics — the go-to view for sales analysis |
| `DashboardPatientMarketingDim_SalesRevenue` | Sales revenue with patient marketing dimensions |
| `DashboardMartFact_Appointment` | Appointment metrics — show rates, cancellations, utilization |

### Clinical
| View | Purpose |
|------|---------|
| `DashboardMartDim_DiagnosisCode` | Diagnosis code lookup with descriptions |

---

## Common Query Patterns

### Revenue by Month (Last 12 Months)
```sql
SELECT
  DATE_TRUNC('month', "LastOperationDateTime") AS "Month",
  COUNT(DISTINCT "InvoiceSummaryId") AS "Invoices",
  SUM(CAST("Amount" AS FLOAT)) AS "Revenue",
  SUM(CAST("Quantity" AS FLOAT)) AS "Units"
FROM DATAWAREHOUSE."InvoiceDetail"
WHERE "DELETED_FLAG" = 'N'
  AND "LastOperationDateTime" >= DATEADD('month', -12, CURRENT_DATE)
GROUP BY DATE_TRUNC('month', "LastOperationDateTime")
ORDER BY "Month" ASC
```

### Top Offices by Revenue
```sql
SELECT
  o."OfficeName",
  SUM(CAST(id."Amount" AS FLOAT)) AS "Revenue",
  COUNT(DISTINCT id."InvoiceSummaryId") AS "Invoices"
FROM DATAWAREHOUSE."InvoiceDetail" id
JOIN DATAWAREHOUSE."InvoiceSummary" isu ON id."InvoiceSummaryId" = isu."InvoiceSummaryId"
  AND id."_Customer" = isu."_Customer"
JOIN DATAWAREHOUSE."Office" o ON isu."OfficeId" = o."OfficeId"
  AND isu."_Customer" = o."_Customer"
WHERE id."DELETED_FLAG" = 'N'
  AND id."LastOperationDateTime" >= DATEADD('month', -12, CURRENT_DATE)
GROUP BY o."OfficeName"
ORDER BY "Revenue" DESC
LIMIT 10
```

### Patient Demographics
```sql
SELECT
  "Sex",
  COUNT(*) AS "Count",
  ROUND(AVG(DATEDIFF('year', "BirthDate", CURRENT_DATE)), 1) AS "AvgAge"
FROM DATAWAREHOUSE."Patient"
WHERE "DELETED_FLAG" = 'N'
  AND "Sex" IN ('M', 'F')
GROUP BY "Sex"
```

### Appointment Utilization
```sql
SELECT
  DATE_TRUNC('month', "Date") AS "Month",
  COUNT(*) AS "TotalAppts",
  SUM(CASE WHEN "IsCanceled" = 'Y' THEN 1 ELSE 0 END) AS "Canceled",
  SUM(CASE WHEN "IsConfirmed" = 'Y' AND "IsCanceled" = 'N' THEN 1 ELSE 0 END) AS "Completed",
  ROUND(100.0 * SUM(CASE WHEN "IsCanceled" = 'Y' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS "CancelRate%"
FROM DATAWAREHOUSE."ScheduledAppointment"
WHERE "IsDeleted" = 'N'
  AND "Date" >= DATEADD('month', -12, CURRENT_DATE)
GROUP BY DATE_TRUNC('month', "Date")
ORDER BY "Month" ASC
```

### Net Collections (Using DATAMART)
```sql
SELECT *
FROM DATAMART."DashboardMartFact_NetCollections"
WHERE "DELETED_FLAG" = 'N'
ORDER BY "TransactionDate" DESC
LIMIT 100
```

### AR Aging (Using DATAMART)
```sql
SELECT
  "OfficeName",
  SUM("ClaimTotalBalance") AS "TotalAR",
  SUM(CASE WHEN "ClaimBalanceDaysOutstanding" <= 30 THEN "ClaimTotalBalance" ELSE 0 END) AS "0-30 Days",
  SUM(CASE WHEN "ClaimBalanceDaysOutstanding" BETWEEN 31 AND 60 THEN "ClaimTotalBalance" ELSE 0 END) AS "31-60 Days",
  SUM(CASE WHEN "ClaimBalanceDaysOutstanding" BETWEEN 61 AND 90 THEN "ClaimTotalBalance" ELSE 0 END) AS "61-90 Days",
  SUM(CASE WHEN "ClaimBalanceDaysOutstanding" > 90 THEN "ClaimTotalBalance" ELSE 0 END) AS "90+ Days"
FROM DATAMART."ARMartFct_Billing"
GROUP BY "OfficeName"
ORDER BY "TotalAR" DESC
```

### Sales by Provider
```sql
SELECT *
FROM DATAMART."DashboardMartFact_SalesByProviderAndStaff"
ORDER BY "TotalSales" DESC
LIMIT 20
```

---

## Join Patterns

### InvoiceDetail → InvoiceSummary → Patient
```sql
FROM DATAWAREHOUSE."InvoiceDetail" id
JOIN DATAWAREHOUSE."InvoiceSummary" isu
  ON id."InvoiceSummaryId" = isu."InvoiceSummaryId"
  AND id."_Customer" = isu."_Customer"
JOIN DATAWAREHOUSE."Patient" p
  ON isu."PatientId" = p."PatientId"
  AND isu."_Customer" = p."_Customer"
```

### Order → Patient → Office
```sql
FROM DATAWAREHOUSE."Order" o
JOIN DATAWAREHOUSE."Patient" p
  ON o."PatientId" = p."PatientId"
  AND o."_Customer" = p."_Customer"
JOIN DATAWAREHOUSE."Office" ofc
  ON o."OfficeId" = ofc."OfficeId"
  AND o."_Customer" = ofc."_Customer"
```

### BillingClaim → BillingClaimData → Carrier
```sql
FROM DATAWAREHOUSE."BillingClaim" bc
JOIN DATAWAREHOUSE."BillingClaimData" bcd
  ON bc."BillingClaimId" = bcd."BillingClaimId"
  AND bc."_Customer" = bcd."_Customer"
```

**IMPORTANT**: Always join on BOTH the business key AND `"_Customer"` to ensure multi-tenant isolation.

---

## Common Pitfalls

1. **Forgetting to quote identifiers** — `SELECT Amount` fails; use `SELECT "Amount"`
2. **Not casting amounts** — `"Amount"` is TEXT in some views; always `CAST("Amount" AS FLOAT)`
3. **Missing DELETED_FLAG filter** — Soft-deleted records will inflate counts
4. **Wrong date column** — `"InvoiceDate"` is on InvoiceSummary; `"LastOperationDateTime"` is on InvoiceDetail
5. **Missing _Customer join** — Cross-tenant data contamination if you only join on business key
6. **Using LIMIT without ORDER BY** — Results are non-deterministic without explicit ordering
7. **Gender = "Sex"** — The column is `"Sex"`, not `"Gender"`, values are `'M'` and `'F'`
