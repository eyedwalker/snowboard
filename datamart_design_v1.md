# Eyecare Analytics Datamart Design V1.0

## 🎯 Design Philosophy: Iterative & Expandable

**Start with solid foundation → Iterate and expand as we learn more**

This V1.0 datamart focuses on the **validated, working components** from our V1.3 revenue cycle analytics, with clear expansion points for future enhancements.

---

## 🏗️ V1.0 Core Architecture

### **📊 Fact Tables**

#### **1. FACT_REVENUE_TRANSACTIONS**
*Primary fact table based on validated V1.3 query*

**Source:** V1.3 query combining Billing + POS transactions
**Grain:** One row per transaction line item
**Key Measures:**
- `BILLED_AMOUNT`
- `INSURANCE_AR`
- `INSURANCE_PAYMENT` 
- `PATIENT_PAYMENT`
- `ADJUSTMENT`
- `WRITEOFF_ALL`
- `COLLECTIONS`
- `INS_TOTAL_BALANCE`
- `PATIENT_BALANCE`

**Foreign Keys:**
- `DATE_KEY` → DIM_DATE
- `OFFICE_KEY` → DIM_OFFICE  
- `PATIENT_KEY` → DIM_PATIENT
- `INSURANCE_KEY` → DIM_INSURANCE
- `ORDER_KEY` → DIM_ORDER
- `ITEM_KEY` → DIM_ITEM (V1.1 expansion)

---

### **📋 Dimension Tables**

#### **1. DIM_DATE**
*Standard date dimension*
- `DATE_KEY` (YYYYMMDD)
- `FULL_DATE`, `YEAR`, `QUARTER`, `MONTH`, `DAY`
- `FISCAL_YEAR`, `FISCAL_QUARTER` (if applicable)
- `IS_WEEKEND`, `IS_HOLIDAY`

#### **2. DIM_OFFICE**
*Office/location dimension*
**Source:** DBO_OFFICE
- `OFFICE_KEY` (surrogate)
- `OFFICE_ID` (natural key)
- `OFFICE_NAME`
- `COMPANY_ID`
- `ADDRESS`, `CITY`, `STATE`, `ZIP`
- `PHONE`, `MANAGER`
- `ACTIVE_FLAG`

#### **3. DIM_PATIENT**
*Patient dimension (de-identified)*
**Source:** DBO_PATIENT
- `PATIENT_KEY` (surrogate)
- `PATIENT_ID` (natural key)
- `AGE_GROUP` (instead of DOB for privacy)
- `GENDER`
- `ZIP_CODE` (for geographic analysis)
- `FIRST_VISIT_DATE`
- `LAST_VISIT_DATE`
- `ACTIVE_FLAG`

#### **4. DIM_INSURANCE**
*Insurance plan and carrier dimension*
**Source:** DBO_INSCARRIER, DBO_INSPLAN
- `INSURANCE_KEY` (surrogate)
- `CARRIER_ID`, `CARRIER_NAME`
- `PLAN_ID`, `PLAN_NAME`
- `PLAN_TYPE`
- `ACTIVE_FLAG`

#### **5. DIM_ORDER**
*Order header dimension*
**Source:** DBO_ORDERS
- `ORDER_KEY` (surrogate)
- `ORDER_ID` (natural key)
- `ORDER_DATE`
- `ORDER_STATUS`
- `ORDER_TYPE`
- `TOTAL_AMOUNT`

---

## 🚀 V1.0 Implementation Plan

### **Phase 1: Core Infrastructure (Week 1)**
1. **Create DATAMART schema** in Snowflake
2. **Build dimension tables** with SCD Type 1
3. **Create date dimension** with 5-year range
4. **Implement basic ETL framework**

### **Phase 2: Fact Table Implementation (Week 2)**
1. **Adapt V1.3 query** for fact table population
2. **Create FACT_REVENUE_TRANSACTIONS** table
3. **Build initial ETL pipeline** (daily refresh)
4. **Data quality validation** using V1.3 totals

### **Phase 3: Basic Analytics Layer (Week 3)**
1. **Create analytical views** for common KPIs
2. **Build basic dashboard** using datamart
3. **Performance optimization** (indexes, clustering)
4. **User acceptance testing**

---

## 📈 V1.1+ Expansion Roadmap

### **V1.1: Product Analytics** 
*Add when ready to implement product relationships*
- **DIM_ITEM** (ItemType, ItemID relationships)
- **DIM_ITEM_CATEGORY** (ItemType descriptions)
- **FACT_PRODUCT_SALES** (InvoiceDetail-based)

### **V1.2: Promotion Analytics**
*Add when promotion relationships are validated*
- **DIM_PROMOTION** (promotion details)
- **DIM_DISCOUNT_TYPE** (discount categories)
- **FACT_PROMOTION_USAGE** (promotion effectiveness)

### **V1.3: Advanced Product Details**
*Add specialized product tables when needed*
- **DIM_FRAME_SPECS** (frame measurements, brands)
- **DIM_LENS_SPECS** (lens materials, coatings)
- **DIM_EXAM_DETAILS** (exam types, procedures)

### **V1.4: Employee & Sales Attribution**
*Add when employee analytics are prioritized*
- **DIM_EMPLOYEE** (sales attribution)
- **FACT_EMPLOYEE_PERFORMANCE** (sales by employee)

---

## 🔧 Technical Specifications

### **ETL Strategy**
- **Incremental loading** based on transaction dates
- **SCD Type 1** for dimensions (overwrite changes)
- **Data validation** against V1.3 query totals
- **Error handling** and logging

### **Performance Optimization**
- **Clustering keys** on date and office
- **Materialized views** for common aggregations
- **Query result caching** enabled
- **Automatic statistics** collection

### **Data Quality**
- **Primary key constraints** on all tables
- **Foreign key validation** in ETL
- **Data profiling** and monitoring
- **Reconciliation** with source systems

---

## 📊 Initial KPIs & Analytics

### **Revenue Analytics**
- Total billed, payments, outstanding A/R
- Insurance vs Patient revenue breakdown
- Collection rates and aging analysis
- Office performance comparisons

### **Operational Analytics**
- Transaction volume trends
- Average transaction values
- Payment method analysis
- Geographic performance

### **Financial Analytics**
- Outstanding balances by aging buckets
- Write-off analysis and trends
- Insurance vs patient payment patterns
- Revenue forecasting (basic)

---

## 🎯 Success Criteria for V1.0

1. **Data Accuracy:** Datamart totals match V1.3 query results
2. **Performance:** Sub-second response for standard reports
3. **Usability:** Business users can create basic reports
4. **Reliability:** 99%+ ETL success rate
5. **Scalability:** Ready for V1.1 product analytics expansion

---

## 📝 Next Steps

1. **Review and approve** V1.0 design
2. **Create Snowflake schemas** and tables
3. **Build initial ETL pipeline**
4. **Validate against V1.3 results**
5. **Plan V1.1 product analytics** expansion

---

*This iterative approach ensures we deliver value quickly while building a foundation for comprehensive eyecare analytics.*
