# Eyecare Analytics Data Dictionary V2.0
## Comprehensive Business Logic & Database Documentation

*Last Updated: 2025-08-08*  
*Version: 2.0 - Enhanced with Stored Procedure Business Logic Analysis*

---

## Executive Summary

This comprehensive data dictionary documents the complete eyecare database ecosystem, including:
- **818 tables** with detailed relationships and business context
- **610 stored procedures** with extracted business logic
- **2,092 parameters** across all procedures
- **105 functions** for business calculations
- **207 views** for data access patterns

### Key Business Logic Discoveries

Through deep analysis of stored procedure source code, we've identified:

- **482 Invoice/Billing procedures** - Complete revenue cycle management
- **167 Item/Product procedures** - Inventory and catalog operations  
- **116 Employee procedures** - Staff management and performance tracking
- **103 Insurance procedures** - Coverage and benefit processing
- **75 Claims procedures** - EDI and adjudication workflows

---

## 🧾 Invoice & Billing Domain

### Business Overview
The invoice and billing domain represents the **financial heartbeat** of the eyecare operation, managing the complete revenue cycle from service delivery to payment collection.

### Core Tables
- **InvoiceSum** - Invoice header with totals and summary information
- **InvoiceDet** - Line-item details for products and services
- **PosTransaction** - Point-of-sale payment transactions
- **BillingTransaction** - Comprehensive billing transaction log
- **BillingClaimLineItem** - Individual claim line items

### Key Business Logic (482 Procedures)

#### Balance Calculations
- **Outstanding Balance Formula**: `Billed Amount - (Payments + Adjustments + Write-offs)`
- **Patient Balance Formula**: `Total Charges - Insurance Payments - Patient Payments`
- **AR Aging Logic**: Categorizes outstanding balances by age (0-30, 31-60, 61-90, 90+ days)

#### Payment Processing Workflows
- **Multi-tender Support**: Cash, credit card, check, insurance payments
- **Split Payment Logic**: Automatic allocation between insurance and patient responsibility
- **Refund Processing**: Complex refund calculations with tax implications

#### Key Procedures
- `GetMonthlyAppliedPaymentSummary` - Monthly payment analytics with office/doctor filtering
- `CalcPOSTransactionSummaryByDayCloseDate` - Daily POS reconciliation
- `SP_OrderAgingFilterByOfficeIds` - Accounts receivable aging analysis
- `ProcessBillingTransaction` - Core billing transaction processing
- `CalculatePatientBalance` - Patient balance calculations

### Financial KPIs & Metrics
- **Days Sales Outstanding (DSO)**: `Accounts Receivable / (Revenue / Days)`
- **Collection Rate**: `Collections / Net Charges`
- **Revenue per Visit**: `Total Revenue / Number of Visits`
- **Payment Method Distribution**: Analysis of payment preferences
- **Write-off Analysis**: Categorization and trending of write-offs

---

## 📦 Item & Product Domain

### Business Overview
The item and product domain manages the complete product lifecycle from catalog management to inventory control, supporting frames, lenses, contacts, and accessories.

### Core Tables
- **Item** - Master product catalog with specifications
- **ItemType** - Product categorization and hierarchy
- **InvoiceDet** - Product sales transactions
- **InventoryBalance** - Current stock levels by location
- **ItemFrame** - Frame-specific attributes (style, color, size)
- **ItemEGLens** - Eyeglass lens specifications
- **ItemContact** - Contact lens parameters

### Key Business Logic (167 Procedures)

#### Inventory Management
- **Reorder Point Formula**: `(Lead Time × Average Usage) + Safety Stock`
- **Inventory Value Calculation**: `Quantity on Hand × Unit Cost`
- **Turnover Ratio**: `Cost of Goods Sold / Average Inventory`

#### Pricing Logic
- **Markup Calculation**: `(Retail Price - Cost) / Cost × 100`
- **Discount Application**: Multi-tier discount structures
- **Insurance Pricing**: Carrier-specific pricing schedules

#### Product Configuration
- **Frame Compatibility**: Lens-frame compatibility matrices
- **Prescription Validation**: Complex prescription parameter validation
- **Product Bundling**: Package pricing for complete eyewear solutions

#### Key Procedures
- `FrameSearch_Get` - Advanced frame search with filtering
- `SaveExistingPriceBulkFrameSetup` - Bulk pricing updates
- `LoadSpexFrameWithPriceRounding` - Frame pricing with rounding rules
- `Lookups_Accessories` - Accessory catalog management
- `UpdateInventoryBalance` - Real-time inventory updates

### Product Analytics KPIs
- **Inventory Turnover**: Product movement efficiency
- **Product Profitability**: Margin analysis by category
- **Stock-out Frequency**: Availability metrics
- **Supplier Performance**: Lead time and quality metrics

---

## 👥 Employee Domain

### Business Overview
The employee domain encompasses staff management, performance tracking, commission calculations, and security access control across the organization.

### Core Tables
- **Employee** - Staff master data with roles and attributes
- **Office** - Location assignments and hierarchy
- **CompanyInfo** - Corporate structure and organization
- **PosTransaction** - Sales attribution by employee
- **Appointment** - Provider scheduling and assignments

### Key Business Logic (116 Procedures)

#### Commission Calculations
- **Commission Formula**: `Sales Amount × Commission Rate`
- **Tiered Commission**: Progressive rates based on performance thresholds
- **Product-Specific Rates**: Different commission rates by product category

#### Performance Tracking
- **Productivity Score**: `Revenue Generated / Hours Worked`
- **Patient Satisfaction**: Weighted scoring from multiple feedback sources
- **Goal Achievement**: Target vs. actual performance tracking

#### User Management & Security
- **Role-Based Access**: Hierarchical permission structures
- **Multi-Location Access**: Cross-office user privileges
- **Audit Trails**: Comprehensive user activity logging

#### Key Procedures
- `CreateUsers` - Comprehensive user setup for new companies
- `Scheduler_UserByUserCredentials_Get` - Authentication and authorization
- `Scheduler_ResourcesAll_Get` - Provider resource management
- `GetEmployeeCommission` - Commission calculation engine
- `UpdateEmployeePerformance` - Performance metrics updates

### Employee Analytics KPIs
- **Revenue per Employee**: Individual and team productivity
- **Commission Accuracy**: Payment processing efficiency
- **Schedule Utilization**: Provider time optimization
- **Training Completion**: Compliance and development tracking

---

## 🛡️ Insurance Domain

### Business Overview
The insurance domain manages carrier relationships, benefit verification, eligibility checking, and coverage analysis to optimize reimbursement and patient experience.

### Core Tables
- **InsCarrier** - Insurance carrier master data
- **InsPlan** - Insurance plan details and benefits
- **PatientInsurance** - Patient coverage information
- **InsEligibility** - Benefit eligibility and limits
- **InsSchedule** - Reimbursement schedules and rates

### Key Business Logic (103 Procedures)

#### Eligibility & Benefits
- **Coverage Verification**: Real-time eligibility checking
- **Benefit Calculation**: `Benefit Maximum - Used Benefits`
- **Patient Responsibility**: `Deductible + Copay + Coinsurance`

#### Reimbursement Logic
- **Allowable Amount**: `Retail Price - Insurance Discount`
- **Coverage Percentage**: Variable by service type and plan
- **Annual Maximums**: Benefit year tracking and limits

#### Carrier Management
- **Contract Terms**: Reimbursement rates and schedules
- **Performance Metrics**: Payment timing and accuracy
- **Network Status**: In-network vs. out-of-network processing

#### Key Procedures
- `SetupVspMedicaid` - VSP Medicaid plan configuration
- `ProcessInsuranceEligibility` - Eligibility verification workflow
- `CalculatePatientResponsibility` - Patient cost calculations
- `UpdateInsuranceBenefits` - Benefit utilization tracking
- `CarrierPerformanceAnalysis` - Carrier relationship metrics

### Insurance Analytics KPIs
- **Authorization Success Rate**: Prior auth approval percentage
- **Average Days to Payment**: Carrier payment timing
- **Denial Rate Analysis**: Claim rejection patterns
- **Benefit Utilization**: Usage vs. available benefits

---

## 📋 Claims Domain

### Business Overview
The claims domain handles the complete claims lifecycle from submission through adjudication to payment posting, including EDI processing and denial management.

### Core Tables
- **BillingClaim** - Claim header information
- **BillingClaimData** - Detailed claim data and status
- **BillingClaimLineItem** - Individual service line items
- **EDI835HeaderInfo** - Electronic remittance advice
- **BillingPayment** - Claim payment postings

### Key Business Logic (75 Procedures)

#### Claims Processing
- **Claim Amount Calculation**: `Allowable Amount - Patient Responsibility`
- **Adjudication Logic**: Complex business rules for claim processing
- **Payment Posting**: `Approved Amount - Adjustments`

#### EDI Integration
- **837 Claim Submission**: Electronic claim transmission
- **835 Payment Processing**: Electronic remittance processing
- **999 Acknowledgment**: Transmission acknowledgment handling

#### Denial Management
- **Denial Categorization**: Systematic denial reason classification
- **Appeal Workflows**: Automated appeal generation and tracking
- **Resubmission Logic**: Intelligent claim resubmission rules

#### Key Procedures
- `Era.MatchClaims` - EDI 835 claim matching and processing
- `SubmitInsuranceClaims` - Batch claim submission workflow
- `ProcessClaimDenials` - Denial management and appeals
- `PostInsurancePayments` - Payment allocation and posting
- `ClaimStatusTracking` - Real-time claim status monitoring

### Claims Analytics KPIs
- **First-Pass Approval Rate**: Clean claim percentage
- **Average Days in AR**: Claim processing time
- **Denial Rate by Reason**: Systematic denial analysis
- **Collection Efficiency**: Payment realization rates

---

## 🔄 Cross-Domain Business Workflows

### Revenue Cycle Workflow
**Patient Visit → Clinical Exam → Product Orders → Invoice Generation → Insurance Claims → Payment Collection**

**Key Integration Points:**
- Patient registration triggers insurance verification
- Clinical exam creates billable services
- Product dispensing generates inventory transactions
- Invoice creation initiates claims processing
- Payment posting updates AR and commission calculations

### Product Fulfillment Workflow
**Customer Order → Inventory Check → Product Dispensing → Invoice Creation → Payment Processing**

**Key Integration Points:**
- Order validation against inventory levels
- Prescription verification and product compatibility
- Real-time inventory updates during dispensing
- Automated invoice generation with proper coding
- Integrated payment processing with commission tracking

### Insurance Processing Workflow
**Eligibility Verification → Benefit Authorization → Claim Submission → Adjudication → Payment Posting**

**Key Integration Points:**
- Real-time eligibility checking during scheduling
- Automated prior authorization workflows
- Electronic claim submission with validation
- Automated payment posting and reconciliation
- Exception handling for denials and appeals

---

## 🧮 Critical Business Formulas

### Financial Calculations
- **Outstanding Balance**: `Billed Amount - (Payments + Adjustments + Write-offs)`
- **Patient Balance**: `Total Charges - Insurance Payments - Patient Payments`
- **Commission**: `Sales Amount × Commission Rate`
- **Discount Amount**: `Retail Price × Discount Percentage`
- **Days Sales Outstanding**: `Accounts Receivable / (Revenue / Days)`

### Insurance Calculations
- **Patient Responsibility**: `Deductible + Copay + Coinsurance`
- **Insurance Allowable**: `Retail Price - Insurance Discount`
- **Coverage Amount**: `Benefit Maximum - Used Benefits`
- **Claim Amount**: `Allowable Amount - Patient Responsibility`

### Inventory Calculations
- **Reorder Point**: `(Lead Time × Average Usage) + Safety Stock`
- **Inventory Value**: `Quantity on Hand × Unit Cost`
- **Turnover Ratio**: `Cost of Goods Sold / Average Inventory`
- **Stock Level**: `Received + Adjustments - Dispensed`

### Operational Metrics
- **Utilization Rate**: `Scheduled Hours / Available Hours`
- **Revenue per Visit**: `Total Revenue / Number of Visits`
- **Collection Rate**: `Collections / Net Charges`
- **Productivity Score**: `Revenue Generated / Hours Worked`

---

## 📊 Analytics & Datamart Implications

### Recommended Fact Tables

#### FACT_REVENUE_TRANSACTIONS
**Grain**: One row per financial transaction
**Measures**: Amount, Tax, Discount, Net_Amount, Outstanding_Balance
**Dimensions**: Date, Patient, Office, Insurance, Payment_Method, Transaction_Type
**Source Procedures**: 482 invoice/billing procedures

#### FACT_PRODUCT_SALES
**Grain**: One row per product sale
**Measures**: Quantity, Unit_Cost, Retail_Price, Discount, Net_Revenue
**Dimensions**: Date, Product, Office, Employee, Customer, Promotion
**Source Procedures**: 167 item/product procedures

#### FACT_INSURANCE_CLAIMS
**Grain**: One row per claim line item
**Measures**: Billed_Amount, Allowed_Amount, Paid_Amount, Patient_Responsibility
**Dimensions**: Date, Patient, Provider, Insurance, Service, Claim_Status
**Source Procedures**: 75 claims procedures + 103 insurance procedures

#### FACT_EMPLOYEE_PERFORMANCE
**Grain**: One row per employee per period
**Measures**: Revenue_Generated, Commission_Earned, Hours_Worked, Patients_Seen
**Dimensions**: Date, Employee, Office, Performance_Period, Goal_Type
**Source Procedures**: 116 employee procedures

### Enhanced Dimensions

#### DIM_PATIENT (SCD Type 2)
**Attributes**: Demographics, Insurance_Info, Clinical_History, Payment_Preferences
**Calculated Fields**: Lifetime_Value, Risk_Score, Loyalty_Segment, Visit_Frequency

#### DIM_PRODUCT (SCD Type 2)
**Attributes**: Category, Brand, Specifications, Cost, Retail_Price
**Calculated Fields**: Profitability, Turnover_Rate, Popularity_Score, Margin_Percentage

#### DIM_INSURANCE (SCD Type 2)
**Attributes**: Carrier, Plan_Type, Benefits, Reimbursement_Rates
**Calculated Fields**: Performance_Score, Payment_Speed, Approval_Rate, Network_Status

#### DIM_EMPLOYEE (SCD Type 2)
**Attributes**: Role, Specialties, Schedule_Preferences, Performance_Metrics
**Calculated Fields**: Productivity_Score, Commission_Rate, Patient_Satisfaction, Goal_Achievement

---

## 🎯 Key Performance Indicators (KPIs)

### Financial KPIs
- **Revenue Growth**: Month-over-month and year-over-year trends
- **Collection Efficiency**: Percentage of billed amounts collected
- **Days Sales Outstanding**: Average collection period
- **Bad Debt Rate**: Uncollectible accounts percentage
- **Payment Method Mix**: Distribution of payment types

### Operational KPIs
- **Patient Satisfaction**: Composite satisfaction scores
- **Appointment Utilization**: Scheduled vs. available time slots
- **Provider Productivity**: Revenue per provider hour
- **Inventory Turnover**: Product movement efficiency
- **Schedule Optimization**: Wait times and resource allocation

### Clinical KPIs
- **Patient Outcomes**: Treatment success rates and follow-up compliance
- **Prescription Accuracy**: Error rates and corrections
- **Referral Patterns**: Internal and external referral tracking
- **Quality Metrics**: Clinical quality indicators and benchmarks

### Insurance KPIs
- **Authorization Success**: Prior authorization approval rates
- **Claim Approval Rate**: First-pass claim success percentage
- **Carrier Performance**: Payment timing and accuracy by carrier
- **Benefit Utilization**: Usage patterns and optimization opportunities

---

## 🔧 Technical Implementation Notes

### Database Architecture
- **Source System**: SQL Server with 818 tables
- **Target Platform**: Snowflake data warehouse
- **ETL Approach**: Incremental loading with change data capture
- **Data Quality**: Comprehensive validation using stored procedure business rules

### Performance Considerations
- **Indexing Strategy**: Based on frequently-used procedure join patterns
- **Partitioning**: Date-based partitioning for large fact tables
- **Aggregation**: Pre-calculated summaries for common reporting needs
- **Caching**: Materialized views for complex cross-domain queries

### Security & Compliance
- **Data Masking**: PHI protection in non-production environments
- **Access Control**: Role-based security aligned with business functions
- **Audit Trails**: Comprehensive logging of data access and modifications
- **Compliance**: HIPAA-compliant data handling and storage

---

## 📈 Future Enhancements

### Advanced Analytics Opportunities
- **Predictive Modeling**: Patient lifetime value and churn prediction
- **Machine Learning**: Inventory optimization and demand forecasting
- **Real-time Analytics**: Live dashboards and alerting systems
- **AI-Powered Insights**: Natural language query and automated insights

### Integration Expansions
- **External Data Sources**: Market data, demographic information, weather patterns
- **IoT Integration**: Equipment monitoring and maintenance scheduling
- **Mobile Applications**: Patient engagement and self-service capabilities
- **API Development**: Third-party integrations and data sharing

---

*This comprehensive data dictionary serves as the definitive guide for eyecare analytics development, combining traditional data modeling with deep business logic understanding extracted from 610+ stored procedures.*

**Document Status**: Complete and ready for implementation  
**Next Review**: Quarterly or upon significant system changes  
**Maintained By**: Analytics Team with Business Stakeholder Review
