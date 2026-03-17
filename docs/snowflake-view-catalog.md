# DEV_ANALYTICS View Catalog

**Extracted:** 2026-03-17 12:06

**Total Views:** 136


---

## Schema: `DATAWAREHOUSE`

*118 views*

### `DATAWAREHOUSE.Address`

**Description:** View providing location or office site data.

**Columns (15):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `AddressId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Line1` | `TEXT` |
| `Line2` | `TEXT` |
| `City` | `TEXT` |
| `State` | `TEXT` |
| `ZipCode` | `TEXT` |
| `IsPrimary` | `TEXT` |
| `AddressTypeId` | `NUMBER` |
| `AddressType_AddressType` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.AddressType`

**Description:** View providing location or office site data.

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `AddressTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Value` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.BillingActivityReason`

**Description:** View providing financial or billing data.

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `BillingActivityReasonId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `BillingClaimLineItemId` | `NUMBER` |
| `BillingClaimLineItem_BillingClaimLineItem` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.BillingAdjustmentType`

**Description:** View providing financial or billing data.

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `BillingAdjustmentTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.BillingClaim`

**Description:** View providing claims or invoice transactions; financial or billing data.

**Columns (14):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `BillingClaimId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `ServiceDate` | `TIMESTAMP_NTZ` |
| `BillingDate` | `TIMESTAMP_NTZ` |
| `CompanyInfoId` | `TEXT` |
| `CompanyInfo_CompanyInfo` | `NUMBER` |
| `PatientId` | `NUMBER` |
| `Patient_Patient` | `NUMBER` |
| `ExternalClaimNumber` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.BillingClaimData`

**Description:** View providing claims or invoice transactions; financial or billing data.

**Columns (33):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `BillingClaimDataId` | `NUMBER` |
| `BillingClaimId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `OfficeKey` | `TEXT` |
| `AuthorizationNumber` | `TEXT` |
| `CarrierName` | `TEXT` |
| `PlanName` | `TEXT` |
| `InsuredId` | `TEXT` |
| `InsuredFirstName` | `TEXT` |
| `InsuredLastName` | `TEXT` |
| `InsuredAddress` | `TEXT` |
| `InsuredCity` | `TEXT` |
| `InsuredState` | `TEXT` |
| `InsuredZip` | `TEXT` |
| `PatientFirstName` | `TEXT` |
| `PatientLastName` | `TEXT` |
| `RenderingProviderFirstName` | `TEXT` |
| `RenderingProviderLastName` | `TEXT` |
| `IsCurrent` | `TEXT` |
| `PatientBirthDate` | `TEXT` |
| `Office_Office` | `NUMBER` |
| `CarrierKey` | `TEXT` |
| `Carrier_Carrier` | `NUMBER` |
| `PlanId` | `NUMBER` |
| `Plan_Plan` | `NUMBER` |
| `BillingClaim_BillingClaim` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RenderingProviderNPI` | `TEXT` |
| `RenderingProviderNumber` | `TEXT` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.BillingClaimLineItem`

**Description:** View providing claims or invoice transactions; product or item catalog data; financial or billing data.

**Columns (18):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `BillingClaimLineItemId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `ItemTypeId` | `NUMBER` |
| `BillingClaimId` | `NUMBER` |
| `CurrentState` | `TEXT` |
| `Description` | `TEXT` |
| `ItemType_ItemType` | `NUMBER` |
| `BillingClaim_BillingClaim` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `Item_Item` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RetailAmount` | `NUMBER` |
| `PaidAmount` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.BillingClaimOrder`

**Description:** View providing claims or invoice transactions; financial or billing data; order transaction data.

**Columns (11):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `BillingClaimOrderId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `BillingClaimId` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `BillingClaim_BillingClaim` | `NUMBER` |
| `Orders_Order` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.BillingLabFeeTransaction`

**Description:** View providing financial or billing data; fee schedule or charge data; general ledger or transaction data; lab order data.

**Columns (16):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `BillingLabFeeTransactionId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `BillingClaimLineItemId` | `NUMBER` |
| `BillingPaymentId` | `NUMBER` |
| `CreatedDateTimeUtc` | `TIMESTAMP_NTZ` |
| `Amount` | `NUMBER` |
| `BillingTransactionTypeId` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `BillingTransactionType_BillingTransactionType` | `NUMBER` |
| `BillingClaimLineItem_BillingClaimLineItem` | `NUMBER` |
| `BillingPayment_BillingPayment` | `NUMBER` |
| `CreatedDateTime` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.BillingLineDetail`

**Description:** View providing financial or billing data; general ledger or transaction data.

**Columns (17):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `BillingLineDetailId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `BillingClaimId` | `NUMBER` |
| `BillingClaimLineItemId` | `NUMBER` |
| `ProcedureCode` | `TEXT` |
| `IsCurrent` | `TEXT` |
| `BillingClaim_BillingClaim` | `NUMBER` |
| `BillingClaimLineItem_BillingClaimLineItem` | `NUMBER` |
| `ServiceDateTime` | `TIMESTAMP_NTZ` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `Quantity` | `NUMBER` |
| `ProcedureCodeDescription` | `TEXT` |
| `ChargeAmount` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.BillingLineItemCurrentAR`

**Description:** View providing product or item catalog data; financial or billing data; general ledger or transaction data.

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `BillingClaimLineItemId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `InsuranceAR` | `NUMBER` |
| `PatientAR` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.BillingPayment`

**Description:** View providing financial or billing data.

**Columns (24):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `BillingPaymentId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `CarrierKey` | `TEXT` |
| `PatientId` | `NUMBER` |
| `BillingPaymentTypeId` | `NUMBER` |
| `Amount` | `NUMBER` |
| `Number` | `TEXT` |
| `Date` | `TIMESTAMP_NTZ` |
| `DepositDate` | `TIMESTAMP_NTZ` |
| `NumberOfSections` | `NUMBER` |
| `OutstandingAmount` | `NUMBER` |
| `IsDeleted` | `TEXT` |
| `CompanyInfoId` | `TEXT` |
| `IsCommitted` | `TEXT` |
| `IsEra` | `TEXT` |
| `CompanyInfo_CompanyInfo` | `NUMBER` |
| `Patient_Patient` | `NUMBER` |
| `Carrier_Carrier` | `NUMBER` |
| `BillingPaymentType_BillingPaymentType` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.BillingPaymentSection`

**Description:** View providing financial or billing data.

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `BillingPaymentSectionId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `BillingPaymentId` | `NUMBER` |
| `BillingPayment_BillingPayment` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.BillingPaymentType`

**Description:** View providing financial or billing data.

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `BillingPaymentTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Value` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.BillingTransaction`

**Description:** View providing financial or billing data; general ledger or transaction data.

**Columns (33):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `BillingTransactionId` | `NUMBER` |
| `BillingClaimLineItemId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `BillingTransactionTypeId` | `NUMBER` |
| `BillingPaymentId` | `NUMBER` |
| `Amount` | `NUMBER` |
| `DateTime` | `TIMESTAMP_NTZ` |
| `InsuranceAR` | `NUMBER` |
| `PatientAR` | `NUMBER` |
| `InsuranceARDelta` | `NUMBER` |
| `PatientARDelta` | `NUMBER` |
| `BillingTransactionType_BillingTransactionType` | `NUMBER` |
| `BillingPayment_BillingPayment` | `NUMBER` |
| `Order_Order` | `NUMBER` |
| `MonthCloseId` | `NUMBER` |
| `WorkflowActivityId` | `NUMBER` |
| `WorkflowActivity_WorkflowActivity` | `NUMBER` |
| `BillingClaimLineItem_BillingClaimLineItem` | `NUMBER` |
| `BillingClaimId` | `NUMBER` |
| `BillingWriteoffReasonId` | `NUMBER` |
| `BillingAdjustmentTypeId` | `NUMBER` |
| `BillingWriteoffReason_BillingWriteoffReason` | `NUMBER` |
| `BillingAdjustmentType_BillingAdjustmentType` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `PatientInsurancePayment` | `NUMBER` |
| `CarrierPayment` | `NUMBER` |
| `CarrierCredit` | `NUMBER` |
| `PatientCredit` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.BillingTransactionType`

**Description:** View providing financial or billing data; general ledger or transaction data.

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `BillingTransactionTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `IsGLTransaction` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.BillingWriteoffReason`

**Description:** View providing financial or billing data.

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `BillingWriteoffReasonId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Code` | `TEXT` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.CLHardRX`

**Description:** View providing prescription or refraction data.

**Columns (13):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CLHardRXId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `DoctorEmployeeId` | `NUMBER` |
| `Employee_Employee` | `NUMBER` |
| `Order_Order` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `OutsideDoctorId` | `NUMBER` |
| `OutsideDoctor_OutsideDoctor` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.CLManufacturer`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CLManufacturerId` | `TEXT` |
| `_Customer` | `NUMBER` |
| `VendorName` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.CLMedicallyNecessaryRX`

**Description:** View providing prescription or refraction data.

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CLMedicallyNecessaryRXId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `Order_Order` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.CLOrder`

**Description:** View providing order transaction data.

**Columns (14):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `DispenseTypeId` | `NUMBER` |
| `DispenseNote` | `TEXT` |
| `CLSupplySourceId` | `NUMBER` |
| `SupplierId` | `NUMBER` |
| `Supplier_Supplier` | `NUMBER` |
| `DispenseType_DispenseType` | `NUMBER` |
| `CLSupplySource_CLSupplySource` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.CLPower`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (11):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CLPowerId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `CLStyleId` | `NUMBER` |
| `Base` | `NUMBER` |
| `Diameter` | `NUMBER` |
| `CLStyle_CLStyle` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.CLSoftRX`

**Description:** View providing prescription or refraction data.

**Columns (14):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CLSoftRXId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `DoctorEmployeeId` | `NUMBER` |
| `Employee_Employee` | `NUMBER` |
| `Order_Order` | `NUMBER` |
| `PatientExamId` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `OutsideDoctorId` | `NUMBER` |
| `OutsideDoctor_OutsideDoctor` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.CLStyle`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (15):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CLStyleId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `CLManufacturerId` | `TEXT` |
| `Style` | `TEXT` |
| `IsHard` | `TEXT` |
| `CLTypeId` | `NUMBER` |
| `CLManufacturer_CLManufacturer` | `NUMBER` |
| `CLType_CLType` | `NUMBER` |
| `CLStyleTypeId` | `NUMBER` |
| `CLStyleType_CLStyleType` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.CLStyleType`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CLStyleTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Value` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.CLSupplierManufacturer`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (12):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CLSupplierManufacturerId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `CLManufacturerId` | `TEXT` |
| `SupplierId` | `NUMBER` |
| `SupplierManufacturer` | `TEXT` |
| `CLManufacturer_CLManufacturer` | `NUMBER` |
| `Supplier_Supplier` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.CLSupplySource`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CLSupplySourceId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `CLSupplySourceSurrogateId` | `NUMBER` |
| `Name` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.CLType`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CLTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Value` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.CPTCode`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (10):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CPTCodeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Code` | `TEXT` |
| `Description` | `TEXT` |
| `IsActive` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Carrier`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CarrierKey` | `TEXT` |
| `_Customer` | `NUMBER` |
| `Name` | `TEXT` |
| `IsPrepaid` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.CatElementLU`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (10):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CatElementLUId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Value` | `TEXT` |
| `Type` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |
| `Ordinal` | `NUMBER` |

### `DATAWAREHOUSE.CompanyInfo`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CompanyInfoId` | `TEXT` |
| `_Customer` | `NUMBER` |
| `Name` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.CompanyItemType`

**Description:** View providing product or item catalog data.

**Columns (12):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CompanyItemTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `CompanyInfoId` | `TEXT` |
| `SalesCategory` | `NUMBER` |
| `CompanyInfo_CompanyInfo` | `NUMBER` |
| `ItemTypeId` | `NUMBER` |
| `ItemType_ItemType` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Date`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (26):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `DATE` | `DATE` |
| `DATE_YYYY_MM_DD` | `TEXT` |
| `DATE_YYYYMMDD` | `TEXT` |
| `YEAR` | `NUMBER` |
| `YEAR_STR` | `TEXT` |
| `YEAR_STR2` | `TEXT` |
| `MONTH` | `NUMBER` |
| `MONTH_STR2` | `TEXT` |
| `MONTH_NAME` | `TEXT` |
| `MONTH_NAME3` | `TEXT` |
| `DAY_IN_MONTH` | `NUMBER` |
| `DAY_IN_MONTH_STR2` | `TEXT` |
| `DAY_IN_YEAR` | `NUMBER` |
| `DAY_IN_WEEK` | `NUMBER` |
| `DAY_IN_WEEK_NAME` | `TEXT` |
| `DAY_IN_WEEK_STR3` | `TEXT` |
| `QTR` | `NUMBER` |
| `YYYYQQ` | `TEXT` |
| `YYQQ` | `TEXT` |
| `WEEK_IN_YEAR` | `NUMBER` |
| `IS_CURRENT_DATE` | `NUMBER` |
| `MONTH_END` | `DATE` |
| `MONTH_START` | `DATE` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.DayClose`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (10):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `DayCloseId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `OfficeKey` | `TEXT` |
| `TransactionDate` | `TIMESTAMP_NTZ` |
| `Office_Office` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.DayCloseDetail`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (12):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `DayCloseDetailId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `DayCloseId` | `NUMBER` |
| `PaymentTypeId` | `NUMBER` |
| `Computed` | `NUMBER` |
| `DayClose_DayClose` | `NUMBER` |
| `PaymentType_PaymentType` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.DiagnosisCategory`

**Description:** View providing diagnosis or clinical data.

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `DiagnosisCategoryId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.DiagnosisCode`

**Description:** View providing diagnosis or clinical data.

**Columns (11):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `DiagnosisCodeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Name` | `TEXT` |
| `Description` | `TEXT` |
| `DiagnosisCategoryId` | `NUMBER` |
| `DiagnosisCategory_DiagnosisCategory` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.DiscountType`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (17):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `DiscountTypeId` | `NUMBER` |
| `CompanyInfoId` | `TEXT` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `ViewOrder` | `NUMBER` |
| `IsActive` | `TEXT` |
| `StartDate` | `TIMESTAMP_NTZ` |
| `EndDate` | `TIMESTAMP_NTZ` |
| `IsCoupon` | `TEXT` |
| `LabCode1` | `TEXT` |
| `LabCode2` | `TEXT` |
| `SystemUseCode` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.DispenseType`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CompanyInfoId` | `TEXT` |
| `DispenseTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.EGFrame`

**Description:** View providing product or item catalog data; contains financial measures.

**Columns (15):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `AMeas` | `NUMBER` |
| `BMeas` | `NUMBER` |
| `EDMeas` | `NUMBER` |
| `DBLMeas` | `NUMBER` |
| `Temple` | `NUMBER` |
| `FrameEdgeTypeId` | `TEXT` |
| `RetailPrice` | `NUMBER` |
| `FrameEdgeType_FrameEdgeType` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.EGOrder`

**Description:** View providing order transaction data.

**Columns (17):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `IsUncut` | `TEXT` |
| `DispenseTypeId` | `NUMBER` |
| `DispenseNote` | `TEXT` |
| `IsMakeFrame` | `TEXT` |
| `IsMakeRLens` | `TEXT` |
| `IsMakeLLens` | `TEXT` |
| `IsMakeExtra` | `TEXT` |
| `IsSafety` | `TEXT` |
| `IsManualLabOrder` | `TEXT` |
| `DispenseType_DispenseType` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.EGRX`

**Description:** View providing prescription or refraction data; contains financial measures.

**Columns (49):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `EGRXId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `Eye` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `FPD` | `NUMBER` |
| `NPD` | `NUMBER` |
| `Sphere` | `NUMBER` |
| `Cylinder` | `NUMBER` |
| `Axis` | `NUMBER` |
| `Prism1` | `NUMBER` |
| `PrismDir1` | `TEXT` |
| `Dir1` | `NUMBER` |
| `Prism2` | `NUMBER` |
| `PrismDir2` | `TEXT` |
| `Dir2` | `NUMBER` |
| `ResultPrism` | `NUMBER` |
| `ResultAngle` | `NUMBER` |
| `Base` | `NUMBER` |
| `OCHeight` | `NUMBER` |
| `AddPower1` | `NUMBER` |
| `AddPower2` | `NUMBER` |
| `SegHeight` | `NUMBER` |
| `Thick` | `NUMBER` |
| `IsBalance` | `TEXT` |
| `IsSlabOff` | `TEXT` |
| `DateTime` | `TIMESTAMP_NTZ` |
| `DoctorEmployeeId` | `NUMBER` |
| `PatientExamId` | `NUMBER` |
| `OutsideDoctorId` | `NUMBER` |
| `EGRXTypeId` | `NUMBER` |
| `FrameWrap` | `NUMBER` |
| `Vertex` | `NUMBER` |
| `PantoscopicTilt` | `NUMBER` |
| `EGRXKey` | `TEXT` |
| `NonFormularyLensDescription` | `TEXT` |
| `NonFormularyLensPrice` | `NUMBER` |
| `StyleRetailPrice` | `NUMBER` |
| `ColorRetailPrice` | `NUMBER` |
| `Item_Item` | `NUMBER` |
| `Doctor_Employee` | `NUMBER` |
| `Order_Order` | `NUMBER` |
| `EGRXType_EGRXType` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `OutsideDoctor_OutsideDoctor` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.EGRXType`

**Description:** View providing prescription or refraction data.

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `EGRXTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Value` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Eligibility`

**Description:** View providing authorization or eligibility data.

**Columns (14):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `EligibilityId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `CarrierKey` | `TEXT` |
| `PlanId` | `NUMBER` |
| `AuthorizationNumber` | `TEXT` |
| `AuthorizationDateTime` | `TIMESTAMP_NTZ` |
| `AuthorizationExpireDate` | `TIMESTAMP_NTZ` |
| `PlanPlan` | `NUMBER` |
| `Carrier_Carrier` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Employee`

**Description:** View providing user or staff data.

**Columns (19):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `EmployeeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `CompanyInfoId` | `TEXT` |
| `LastName` | `TEXT` |
| `FirstName` | `TEXT` |
| `EmployeeTypeId` | `NUMBER` |
| `CompanyInfo_CompanyInfo` | `NUMBER` |
| `EmployeeType_EmployeeType` | `NUMBER` |
| `IsActive` | `TEXT` |
| `IsAllowedToBeScheduled` | `TEXT` |
| `UserId` | `NUMBER` |
| `User_User` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `NPI` | `TEXT` |
| `FullName` | `TEXT` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.EmployeeType`

**Description:** View providing user or staff data.

**Columns (7):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `EmployeeTypeId` | `NUMBER` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.EraAdjustment`

**Description:** View providing contains financial measures.

**Columns (12):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `EraAdjustmentId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `GroupCode` | `TEXT` |
| `ReasonCode` | `TEXT` |
| `EraLineItemId` | `NUMBER` |
| `Amount` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `EraLineItem_EraLineItem` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.EraClaim`

**Description:** View providing claims or invoice transactions.

**Columns (11):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `EraClaimId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `BillingClaimId` | `NUMBER` |
| `EraRemittanceId` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `BillingClaim_BillingClaim` | `NUMBER` |
| `EraRemittance_EraRemittance` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.EraLineItem`

**Description:** View providing product or item catalog data.

**Columns (11):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `EraLineItemId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `ProcedureCode` | `TEXT` |
| `EraClaimId` | `NUMBER` |
| `ParentEraLineItemId` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `EraClaim_EraClaim` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.EraRemittance`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `EraRemittanceId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `BillingPaymentId` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `BillingPayment_BillingPayment` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.FrameCollection`

**Description:** View providing product or item catalog data.

**Columns (12):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `FrameCollectionId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `VendorId` | `NUMBER` |
| `ManufacturerId` | `NUMBER` |
| `Manufacturer_Manufacturer` | `NUMBER` |
| `Vendor_Vendor` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.FrameEdgeType`

**Description:** View providing product or item catalog data.

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `FrameEdgeTypeId` | `TEXT` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `IsGroovedRimless` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.FrameStyle`

**Description:** View providing product or item catalog data.

**Columns (12):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `FrameStyleId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Model` | `TEXT` |
| `Description` | `TEXT` |
| `FrameCollectionId` | `NUMBER` |
| `IsMedicaid` | `TEXT` |
| `FrameCollection_FrameCollection` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.ICare_EnabledCompany`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (13):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ICare_EnabledCompanyId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Name` | `TEXT` |
| `OfficeKey` | `TEXT` |
| `OfficeKeyUid` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `LastOperation` | `TEXT` |
| `LastOperationDateTime` | `TIMESTAMP_NTZ` |
| `Office_Office` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.InsuranceProcedureCode`

**Description:** View providing insurance plan or coverage data.

**Columns (11):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `InsuranceProcedureCodeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Code` | `TEXT` |
| `ItemId` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `Item_Item` | `NUMBER` |
| `ItemGroupId` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.InvoiceDetail`

**Description:** View providing claims or invoice transactions; contains financial measures; includes demographic attributes.

**Columns (40):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `InvoiceDetailId` | `NUMBER` |
| `InvoiceSummaryId` | `NUMBER` |
| `LineNumber` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `Quantity` | `NUMBER` |
| `Price` | `NUMBER` |
| `DiscountTypeId` | `NUMBER` |
| `Tax` | `NUMBER` |
| `Discount` | `NUMBER` |
| `LineDiscount` | `NUMBER` |
| `PackageDiscount` | `NUMBER` |
| `Amount` | `NUMBER` |
| `CouponNumber` | `TEXT` |
| `IsPreAppointment` | `TEXT` |
| `GiftCardNumber` | `TEXT` |
| `IsLensItem` | `TEXT` |
| `IsCalculateLineDiscountByPercent` | `TEXT` |
| `StockOrderNumber` | `NUMBER` |
| `UnitCost` | `NUMBER` |
| `ValuationCost` | `NUMBER` |
| `InvoiceDetailKey` | `TEXT` |
| `IsBillToInsurance` | `TEXT` |
| `TaxedPriceType` | `NUMBER` |
| `IsAddToInventory` | `TEXT` |
| `PromotionDiscount` | `NUMBER` |
| `PromotionId` | `NUMBER` |
| `ItemType_ItemType` | `NUMBER` |
| `Item_Item` | `NUMBER` |
| `DiscountType_DiscountType` | `NUMBER` |
| `Order_Order` | `NUMBER` |
| `InvoiceSummary_InvoiceSummary` | `NUMBER` |
| `ItemTypeId` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `LastOperationDateTime` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.InvoiceInsuranceDetail`

**Description:** View providing claims or invoice transactions; insurance plan or coverage data.

**Columns (16):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `InvoiceInsuranceDetailId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `InvoiceDetailId` | `NUMBER` |
| `Allowance` | `NUMBER` |
| `Copay` | `NUMBER` |
| `Receivable` | `NUMBER` |
| `InsuranceDiscount` | `NUMBER` |
| `IsPrimary` | `TEXT` |
| `OrderInsuranceId` | `NUMBER` |
| `OrderInsurance_OrderInsurance` | `NUMBER` |
| `InvoiceDetail_InvoiceDetail` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.InvoiceSummary`

**Description:** View providing claims or invoice transactions; aggregated summary/KPI metrics.

**Columns (17):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `InvoiceSummaryId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `PosTransactionId` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `DoctorEmployeeId` | `NUMBER` |
| `EmployeeId` | `NUMBER` |
| `Doctor_Employee` | `NUMBER` |
| `Employee_Employee` | `NUMBER` |
| `Order_Order` | `NUMBER` |
| `OriginalDoctorEmployeeId` | `NUMBER` |
| `OriginalDoctor_Employee` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RefundTypeId` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Item`

**Description:** View providing product or item catalog data.

**Columns (15):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `ItemTypeId` | `NUMBER` |
| `Number` | `TEXT` |
| `Name` | `TEXT` |
| `UPCCode` | `TEXT` |
| `Group` | `TEXT` |
| `ItemKey` | `TEXT` |
| `ItemType_ItemType` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `CPTCode` | `TEXT` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.ItemCL`

**Description:** View providing product or item catalog data.

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `CLStyleId` | `NUMBER` |
| `CLStyle_CLStyle` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.ItemCoat`

**Description:** View providing product or item catalog data.

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.ItemCompany`

**Description:** View providing product or item catalog data.

**Columns (12):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ItemCompanyId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `CompanyInfoId` | `TEXT` |
| `Name` | `TEXT` |
| `Item_Item` | `NUMBER` |
| `CompanyInfo_CompanyInfo` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.ItemEGColor`

**Description:** View providing product or item catalog data.

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `Item_Item` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.ItemEGLens`

**Description:** View providing product or item catalog data; general ledger or transaction data.

**Columns (18):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `ItemEGTypeId` | `NUMBER` |
| `ItemEGMaterialId` | `NUMBER` |
| `ItemEGStyleId` | `NUMBER` |
| `ItemEGColorId` | `NUMBER` |
| `ItemCoatId` | `NUMBER` |
| `Item_Item` | `NUMBER` |
| `ItemEGType_ItemEGType` | `NUMBER` |
| `ItemEGMaterial_ItemEGMaterial` | `NUMBER` |
| `ItemEGStyle_ItemEGStyle` | `NUMBER` |
| `ItemEGColor_ItemEGColor` | `NUMBER` |
| `ItemCoat_ItemCoat` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.ItemEGMaterial`

**Description:** View providing product or item catalog data.

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.ItemEGStyle`

**Description:** View providing product or item catalog data.

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.ItemEGType`

**Description:** View providing product or item catalog data.

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.ItemExam`

**Description:** View providing product or item catalog data; exam or encounter data.

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `IsFittingFee` | `TEXT` |
| `IsProcedure` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.ItemFrame`

**Description:** View providing product or item catalog data.

**Columns (11):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `FrameStyleId` | `NUMBER` |
| `Color` | `TEXT` |
| `Eye` | `NUMBER` |
| `FrameStyle_FrameStyle` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.ItemType`

**Description:** View providing product or item catalog data.

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ItemTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `ItemTypeGrouping` | `TEXT` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Lab`

**Description:** View providing lab order data.

**Columns (11):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `LabId` | `TEXT` |
| `_Customer` | `NUMBER` |
| `Name` | `TEXT` |
| `AddressId` | `NUMBER` |
| `Address_Address` | `NUMBER` |
| `OmicsVersion` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Manufacturer`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ManufacturerId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Name` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.MiscPaymentReason`

**Description:** View providing financial or billing data.

**Columns (10):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `MiscPaymentReasonId` | `NUMBER` |
| `CompanyInfoId` | `TEXT` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `CustomerReferenceNumber` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Note`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (10):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `NoteId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Detail` | `TEXT` |
| `EntityTypeId` | `NUMBER` |
| `EntityId` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Office`

**Description:** View providing location or office site data.

**Columns (26):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `OfficeKey` | `TEXT` |
| `_Customer` | `NUMBER` |
| `Name` | `TEXT` |
| `OfficeTypeId` | `NUMBER` |
| `RegionId` | `NUMBER` |
| `IsLive` | `TEXT` |
| `CompanyInfoId` | `TEXT` |
| `IsSingleLocation` | `TEXT` |
| `CompanyInfo_CompanyInfo` | `NUMBER` |
| `Type_OfficeType` | `NUMBER` |
| `Region_Region` | `NUMBER` |
| `DiscountThreshold` | `NUMBER` |
| `OfficeId` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `AddressId` | `NUMBER` |
| `PhoneNumber` | `TEXT` |
| `TimeZone` | `NUMBER` |
| `OfficeKeyUid` | `TEXT` |
| `Address_Address` | `NUMBER` |
| `IsUseDST` | `TEXT` |
| `IsPreAppointmentSupported` | `TEXT` |
| `IsActive` | `TEXT` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.OfficeEmployee`

**Description:** View providing location or office site data; user or staff data.

**Columns (11):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `OfficeKey` | `TEXT` |
| `EmployeeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `OfficeEmployeeId` | `NUMBER` |
| `Office_Office` | `NUMBER` |
| `Employee_Employee` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.OfficeHours`

**Description:** View providing location or office site data.

**Columns (14):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `OfficeHoursId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `OfficeKey` | `TEXT` |
| `Day` | `NUMBER` |
| `OpenFrom` | `NUMBER` |
| `OpenTo` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `LastOperation` | `TEXT` |
| `LastOperationDateTime` | `TIMESTAMP_NTZ` |
| `Office_Office` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.OfficeType`

**Description:** View providing location or office site data.

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `OfficeTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Value` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Order`

**Description:** View providing order transaction data.

**Columns (45):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `OfficeKey` | `TEXT` |
| `ShipTo` | `NUMBER` |
| `DateTime` | `TIMESTAMP_NTZ` |
| `PatientId` | `NUMBER` |
| `OrderTypeKey` | `TEXT` |
| `EmployeeId` | `NUMBER` |
| `IsRemake` | `TEXT` |
| `LabInstructions` | `TEXT` |
| `Patient_Patient` | `NUMBER` |
| `Employee_Employee` | `NUMBER` |
| `Office_Office` | `NUMBER` |
| `OrderType_OrderType` | `NUMBER` |
| `RemakeTypeId` | `NUMBER` |
| `RemadeOrder` | `NUMBER` |
| `LabJobNumber` | `TEXT` |
| `CompanyInfoId` | `TEXT` |
| `CompanyInfo_CompanyInfo` | `NUMBER` |
| `RemakeType_RemakeType` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RXDoctorEmployeeId` | `NUMBER` |
| `RXOutsideDoctorId` | `NUMBER` |
| `RXDoctorEmployee_Employee` | `NUMBER` |
| `RXOutsideDoctor_OutsideDoctor` | `NUMBER` |
| `StatusCode` | `TEXT` |
| `IsMixed` | `TEXT` |
| `IsFullyPaid` | `TEXT` |
| `DateTimeFullyPaid` | `TIMESTAMP_NTZ` |
| `OriginalOrderId` | `NUMBER` |
| `OriginalDoctorEmployeeId` | `NUMBER` |
| `OriginalEmployeeId` | `NUMBER` |
| `OriginalOfficeKey` | `TEXT` |
| `OriginalOrder_Order` | `NUMBER` |
| `OriginalDoctorEmployee_Employee` | `NUMBER` |
| `OriginalEmployee_Employee` | `NUMBER` |
| `OriginalOffice_Office` | `NUMBER` |
| `DateTimeFirstPayment` | `TIMESTAMP_NTZ` |
| `IsCollectionsItemizable` | `TEXT` |
| `StatusCodeDescription` | `TEXT` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |
| `StatusCodeChangedDateTime` | `TIMESTAMP_NTZ` |

### `DATAWAREHOUSE.OrderExamDetail`

**Description:** View providing order transaction data; exam or encounter data.

**Columns (23):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `OrderExamDetailId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `DiagnosisCodeId2` | `NUMBER` |
| `DiagnosisCodeId3` | `NUMBER` |
| `DiagnosisCodeId4` | `NUMBER` |
| `DiagnosisCodeId5` | `NUMBER` |
| `DiagnosisCodeId6` | `NUMBER` |
| `DiagnosisCodeId7` | `NUMBER` |
| `DiagnosisCodeId8` | `NUMBER` |
| `DiagnosisCodeId9` | `NUMBER` |
| `DiagnosisCodeId10` | `NUMBER` |
| `DiagnosisCodeId11` | `NUMBER` |
| `DiagnosisCodeId12` | `NUMBER` |
| `Order_Order` | `NUMBER` |
| `Item_Item` | `NUMBER` |
| `DiagnosisCodeId1` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.OrderInsurance`

**Description:** View providing insurance plan or coverage data; order transaction data.

**Columns (13):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `OrderInsuranceId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `CarrierName` | `TEXT` |
| `PlanName` | `TEXT` |
| `CarrierKey` | `TEXT` |
| `Carrier_Carrier` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `Order_Order` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.OrderType`

**Description:** View providing order transaction data.

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `OrderTypeKey` | `TEXT` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.OutsideDoctor`

**Description:** View providing provider/practitioner information.

**Columns (13):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `OutsideDoctorId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `LastName` | `TEXT` |
| `FirstName` | `TEXT` |
| `NPI` | `TEXT` |
| `CompanyInfoId` | `TEXT` |
| `IsActive` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `CompanyInfo_CompanyInfo` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Patient`

**Description:** View providing patient/member data.

**Columns (30):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PatientId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `LastName` | `TEXT` |
| `FirstName` | `TEXT` |
| `BirthDate` | `TEXT` |
| `Sex` | `TEXT` |
| `MiddleInitial` | `TEXT` |
| `Email_01` | `TEXT` |
| `ResponsiblePatientId` | `NUMBER` |
| `ReferralTypeId` | `NUMBER` |
| `LastExamDate` | `TIMESTAMP_NTZ` |
| `HomeOfficeKey` | `TEXT` |
| `IsAddressBad` | `TEXT` |
| `IsEmailBad` | `TEXT` |
| `IsInActive` | `TEXT` |
| `NickName` | `TEXT` |
| `ProviderEmployeeId` | `NUMBER` |
| `IsPatient` | `TEXT` |
| `ReferralType_ReferralType` | `NUMBER` |
| `ResponsiblePartyFullName` | `TEXT` |
| `CompanyInfoId` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `FullName` | `TEXT` |
| `PatientKey` | `TEXT` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |
| `IsDeceased` | `TEXT` |
| `IsBadPhoneNumber` | `TEXT` |

### `DATAWAREHOUSE.PatientAddress`

**Description:** View providing patient/member data; location or office site data.

**Columns (10):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PatientId` | `NUMBER` |
| `AddressId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Patient_Patient` | `NUMBER` |
| `Address_Address` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.PatientCommunicationEventType`

**Description:** View providing patient/member data.

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PatientCommunicationEventTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.PatientCommunicationMethod`

**Description:** View providing patient/member data.

**Columns (15):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PatientCommunicationMethodId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `PatientId` | `NUMBER` |
| `PatientCommunicationEventTypeId` | `NUMBER` |
| `IsCall` | `TEXT` |
| `IsMail` | `TEXT` |
| `IsText` | `TEXT` |
| `IsEmail` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `Patient_Patient` | `NUMBER` |
| `PatientCommunicationEventType_PatientCommunicationEventType` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.PatientExam`

**Description:** View providing patient/member data; exam or encounter data.

**Columns (19):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PatientExamId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `PatientId` | `NUMBER` |
| `DateTime` | `TIMESTAMP_NTZ` |
| `ExpireDateTime` | `TIMESTAMP_NTZ` |
| `DoctorEmployeeId` | `NUMBER` |
| `EmployeeId` | `NUMBER` |
| `CreatedDateTime` | `TIMESTAMP_NTZ` |
| `OfficeKey` | `TEXT` |
| `Patient_Patient` | `NUMBER` |
| `Doctor_Employee` | `NUMBER` |
| `Employee_Employee` | `NUMBER` |
| `Office_Office` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |
| `ExamRXType` | `NUMBER` |

### `DATAWAREHOUSE.PatientExamDetail`

**Description:** View providing patient/member data; exam or encounter data.

**Columns (30):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PatientExamDetailId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `PatientExamId` | `NUMBER` |
| `Eye` | `NUMBER` |
| `Sphere` | `NUMBER` |
| `Cylinder` | `NUMBER` |
| `Axis` | `NUMBER` |
| `AddPower1` | `NUMBER` |
| `Base` | `NUMBER` |
| `Diameter` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `CLPowerId` | `NUMBER` |
| `OpticalZone` | `NUMBER` |
| `PerCurve1` | `NUMBER` |
| `PerCurve2` | `NUMBER` |
| `SecCurve1` | `NUMBER` |
| `SecCurve2` | `NUMBER` |
| `CenterThickness` | `NUMBER` |
| `Blend` | `TEXT` |
| `SupplierId` | `NUMBER` |
| `Item_Item` | `NUMBER` |
| `CLPower_CLPower` | `NUMBER` |
| `Supplier_Supplier` | `NUMBER` |
| `PatientExam_PatientExam` | `NUMBER` |
| `CLStockItemId` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.PatientInsurance`

**Description:** View providing patient/member data; insurance plan or coverage data.

**Columns (11):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PatientInsuranceId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `PlanId` | `NUMBER` |
| `PatientId` | `NUMBER` |
| `Patient_Patient` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `Plan_Plan` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.PatientPhone`

**Description:** View providing patient/member data.

**Columns (11):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PatientPhoneId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `PatientId` | `NUMBER` |
| `PhoneId` | `NUMBER` |
| `Patient_Patient` | `NUMBER` |
| `Phone_Phone` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.PatientPhoneType`

**Description:** View providing patient/member data.

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PatientPhoneTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Value` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.PatientRecall`

**Description:** View providing patient/member data; patient recall or follow-up data.

**Columns (13):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PatientRecallId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `PatientId` | `NUMBER` |
| `Date` | `TIMESTAMP_NTZ` |
| `IsActive` | `TEXT` |
| `Patient_Patient` | `NUMBER` |
| `PatientRecallTypeId` | `NUMBER` |
| `PatientRecallType_PatientRecallType` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.PatientRecallType`

**Description:** View providing patient/member data; patient recall or follow-up data.

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PatientRecallTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.PaymentType`

**Description:** View providing financial or billing data.

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PaymentTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `IsPrivateLabel` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Phone`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (12):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PhoneId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Number` | `TEXT` |
| `PatientPhoneTypeId` | `NUMBER` |
| `IsPrimary` | `TEXT` |
| `Extension` | `TEXT` |
| `PatientPhoneType_PatientPhoneType` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Plan`

**Description:** View providing insurance plan or coverage data.

**Columns (10):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PlanId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Name` | `TEXT` |
| `CarrierCode` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `Carrier_Carrier` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.PosMiscPayment`

**Description:** View providing financial or billing data.

**Columns (13):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PosMiscPaymentId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `MiscPaymentReasonId` | `NUMBER` |
| `PosTransactionId` | `NUMBER` |
| `OriginalOrderNumber` | `TEXT` |
| `OriginalStoreNumber` | `TEXT` |
| `PosTransaction_PosTransaction` | `NUMBER` |
| `MiscPaymentReason_MiscPaymentReason` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.PosPayment`

**Description:** View providing financial or billing data.

**Columns (11):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PosPaymentId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `PatientId` | `NUMBER` |
| `Amount` | `NUMBER` |
| `Patient_Patient` | `NUMBER` |
| `DateTime` | `TIMESTAMP_NTZ` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.PosPaymentDetail`

**Description:** View providing financial or billing data.

**Columns (12):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PosPaymentDetailId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `PosPaymentId` | `NUMBER` |
| `PaymentTypeId` | `NUMBER` |
| `Amount` | `NUMBER` |
| `PosPayment_PosPayment` | `NUMBER` |
| `PaymentType_PaymentType` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.PosTransaction`

**Description:** View providing general ledger or transaction data; contains financial measures.

**Columns (25):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PosTransactionId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `PosTransactionTypeId` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `EmployeeId` | `NUMBER` |
| `PatientId` | `NUMBER` |
| `OfficeKey` | `TEXT` |
| `PosPaymentId` | `NUMBER` |
| `Amount` | `NUMBER` |
| `DateTime` | `TIMESTAMP_NTZ` |
| `InvoiceSummaryId` | `NUMBER` |
| `DayCloseId` | `NUMBER` |
| `PosTransactionType_PosTransactionType` | `NUMBER` |
| `Employee_Employee` | `NUMBER` |
| `Patient_Patient` | `NUMBER` |
| `PosPayment_PosPayment` | `NUMBER` |
| `DayClose_DayClose` | `NUMBER` |
| `Order_Order` | `NUMBER` |
| `Office_Office` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `InvoiceSummary_InvoiceSummary` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.PosTransactionType`

**Description:** View providing general ledger or transaction data.

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `PosTransactionTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.ReferralType`

**Description:** View providing referral data.

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ReferralTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Value` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Region`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `RegionId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Value` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.RemakeType`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `CompanyInfoId` | `TEXT` |
| `RemakeTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Role`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (11):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `RoleId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Name` | `TEXT` |
| `Description` | `TEXT` |
| `RoleTypeId` | `NUMBER` |
| `CompanyInfoId` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.ScheduledAppointment`

**Description:** View providing appointment or scheduling data; includes demographic attributes.

**Columns (30):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ScheduledAppointmentId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `EmployeeId` | `NUMBER` |
| `OfficeId` | `NUMBER` |
| `PatientInsuranceId` | `NUMBER` |
| `Date` | `TIMESTAMP_NTZ` |
| `StartTime` | `TEXT` |
| `IsCanceled` | `TEXT` |
| `PatientId` | `NUMBER` |
| `IsDeleted` | `TEXT` |
| `Employee_Employee` | `NUMBER` |
| `PatientInsurance_PatientInsurance` | `NUMBER` |
| `Patient_Patient` | `NUMBER` |
| `IsConfirmed` | `TEXT` |
| `Notes` | `TEXT` |
| `EndTime` | `TEXT` |
| `IsMessageLeft` | `TEXT` |
| `IsNoAnswer` | `TEXT` |
| `Status_appt_show_ind` | `NUMBER` |
| `PatientMedicalInsuranceId` | `NUMBER` |
| `DerivedStatus` | `TEXT` |
| `IsPatientAppointment` | `TEXT` |
| `IsNonPatientAppointment` | `TEXT` |
| `IsInsuranceAssigned` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.ScheduledAppointmentNote`

**Description:** View providing appointment or scheduling data.

**Columns (10):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `NoteId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Detail` | `TEXT` |
| `ScheduledAppointmentId` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `ScheduledAppointment_ScheduledAppointment` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.ScheduledAppointmentType`

**Description:** View providing appointment or scheduling data.

**Columns (13):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `ScheduledAppointmentTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `OfficeId` | `NUMBER` |
| `Duration` | `NUMBER` |
| `IsAvailableToSchedule` | `TEXT` |
| `Color` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `Item_Item` | `NUMBER` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Supplier`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `SupplierId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Time`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (10):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `TIME` | `TIME` |
| `HOUR12` | `NUMBER` |
| `HOUR24` | `NUMBER` |
| `MINUTE` | `NUMBER` |
| `AM_PM` | `TEXT` |
| `HOUR12_DISPLAY` | `TEXT` |
| `HOUR24_DISPLAY` | `TEXT` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.User`

**Description:** View providing user or staff data.

**Columns (13):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `UserId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Name` | `TEXT` |
| `Email` | `TEXT` |
| `IsLockedOut` | `TEXT` |
| `CreationDateTime` | `TIMESTAMP_NTZ` |
| `LastLoginDateTime` | `TIMESTAMP_NTZ` |
| `CompanyInfoId` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.UserRole`

**Description:** View providing user or staff data.

**Columns (10):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `UserId` | `NUMBER` |
| `RoleId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `User_User` | `NUMBER` |
| `Role_Role` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.Vendor`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `VendorId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `AddressId` | `NUMBER` |
| `Address_Address` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.WorkflowActivity`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (9):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `WorkflowActivityId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `WorkflowEventId` | `NUMBER` |
| `WorkflowEvent_WorkflowEvent` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.WorkflowEvent`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (19):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `WorkflowEventId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Name` | `TEXT` |
| `Note` | `TEXT` |
| `AssociatedEntityId` | `NUMBER` |
| `WorkflowTypeId` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `BillingClaimId` | `NUMBER` |
| `AppointmentId` | `NUMBER` |
| `Order_Order` | `NUMBER` |
| `BillingClaim_BillingClaim` | `NUMBER` |
| `WorkflowType_WorkflowType` | `NUMBER` |
| `UserId` | `NUMBER` |
| `User_User` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |

### `DATAWAREHOUSE.WorkflowType`

**Description:** Business view in DEV_ANALYTICS (review DDL for details).

**Columns (8):**

| Column | Type |
|--------|------|
| `ID` | `NUMBER` |
| `WorkflowTypeId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `Description` | `TEXT` |
| `DELETED_FLAG` | `TEXT` |
| `DELETED_DATETIME` | `TIMESTAMP_NTZ` |
| `RUNNO_INSERT` | `NUMBER` |
| `RUNNO_UPDATE` | `NUMBER` |


---

## Schema: `DATAMART`

*18 views*

### `DATAMART.ARMartFct_Billing`

**Description:** View providing financial or billing data; fact table for analytical measures; joins 8+ source tables.

**Columns (28):**

| Column | Type |
|--------|------|
| `Source` | `TEXT` |
| `BalanceCategory` | `TEXT` |
| `_Customer` | `NUMBER` |
| `CompanyInfoId` | `TEXT` |
| `OfficeNum` | `TEXT` |
| `OfficeName` | `TEXT` |
| `PatientId` | `NUMBER` |
| `PatientFirstName` | `TEXT` |
| `PatientLastName` | `TEXT` |
| `PatientFullName` | `TEXT` |
| `OrderNum` | `NUMBER` |
| `OrderStatus` | `TEXT` |
| `OrderStatusCodeDescription` | `TEXT` |
| `OrderDate` | `TIMESTAMP_NTZ` |
| `OrderBalance` | `TEXT` |
| `OrderBalanceDaysOutstanding` | `TEXT` |
| `CarrierId` | `TEXT` |
| `CarrierName` | `TEXT` |
| `PlanId` | `NUMBER` |
| `PlanName` | `TEXT` |
| `ClaimId` | `NUMBER` |
| `ClaimStatus` | `TEXT` |
| `ClaimLatestNote` | `TEXT` |
| `ClaimServiceDate` | `TIMESTAMP_NTZ` |
| `ClaimInsuranceBalance` | `NUMBER` |
| `ClaimPatientBalance` | `NUMBER` |
| `ClaimTotalBalance` | `NUMBER` |
| `ClaimBalanceDaysOutstanding` | `NUMBER` |

### `DATAMART.ARMartFct_MonthlyMeasuresBilling`

**Description:** View providing financial or billing data; fact table for analytical measures; joins 12+ source tables.

**Columns (14):**

| Column | Type |
|--------|------|
| `Source` | `TEXT` |
| `_Customer` | `NUMBER` |
| `CompanyInfoId` | `TEXT` |
| `OfficeKey` | `TEXT` |
| `BalanceType` | `TEXT` |
| `BalanceCategory` | `TEXT` |
| `OrderId` | `NUMBER` |
| `MONTH_END` | `DATE` |
| `InsuranceBalanceDue` | `NUMBER` |
| `PatientBalanceDue` | `NUMBER` |
| `ClaimBalanceDue` | `NUMBER` |
| `InsuranceDaysOutstanding` | `NUMBER` |
| `PatientDaysOutstanding` | `NUMBER` |
| `ClaimDaysOutstanding` | `NUMBER` |

### `DATAMART.ARMartFct_MonthlyMeasuresPOS`

**Description:** View providing fact table for analytical measures; joins 7+ source tables.

**Columns (10):**

| Column | Type |
|--------|------|
| `Source` | `TEXT` |
| `_Customer` | `NUMBER` |
| `CompanyInfoId` | `TEXT` |
| `OfficeKey` | `TEXT` |
| `BalanceType` | `TEXT` |
| `BalanceCategory` | `TEXT` |
| `OrderId` | `NUMBER` |
| `MONTH_END` | `DATE` |
| `BalanceDue` | `NUMBER` |
| `DaysOutstanding` | `NUMBER` |

### `DATAMART.ARMartFct_MonthlyMeasures_Test`

**Description:** View providing fact table for analytical measures; contains financial measures.

**Columns (9):**

| Column | Type |
|--------|------|
| `CompanyInfoId` | `TEXT` |
| `OfficeKey` | `TEXT` |
| `MonthEnd` | `DATE` |
| `OrderId` | `NUMBER` |
| `TotalBalanceDue` | `NUMBER` |
| `TotalDaysOutstanding` | `NUMBER` |
| `Source` | `TEXT` |
| `BalanceCategory` | `TEXT` |
| `AgingBucket` | `TEXT` |

### `DATAMART.ARMartFct_POSBalanceDetails`

**Description:** View providing fact table for analytical measures; contains financial measures; joins 5+ source tables.

**Columns (28):**

| Column | Type |
|--------|------|
| `Source` | `TEXT` |
| `BalanceCategory` | `TEXT` |
| `_Customer` | `NUMBER` |
| `CompanyId` | `TEXT` |
| `OfficeNum` | `TEXT` |
| `OfficeName` | `TEXT` |
| `PatientId` | `NUMBER` |
| `PatientFirstName` | `TEXT` |
| `PatientLastName` | `TEXT` |
| `PatientFullName` | `TEXT` |
| `OrderNum` | `NUMBER` |
| `OrderStatus` | `TEXT` |
| `OrderStatusCodeDescription` | `TEXT` |
| `OrderDate` | `TIMESTAMP_NTZ` |
| `OrderBalance` | `NUMBER` |
| `OrderBalanceDaysOutstanding` | `NUMBER` |
| `CarrierId` | `TEXT` |
| `CarrierName` | `TEXT` |
| `PlanId` | `TEXT` |
| `PlanName` | `TEXT` |
| `ClaimId` | `TEXT` |
| `ClaimStatus` | `TEXT` |
| `ClaimLatestNote` | `TEXT` |
| `ClaimServiceDate` | `TEXT` |
| `ClaimInsuranceBalance` | `TEXT` |
| `ClaimPatientBalance` | `TEXT` |
| `ClaimTotalBalance` | `TEXT` |
| `ClaimBalanceDaysOutstanding` | `TEXT` |

### `DATAMART.DashboardMartDim_CombinedInvoiceDetail`

**Description:** View providing claims or invoice transactions; dimension table for analytical joins; contains financial measures; includes demographic attributes.

**Columns (21):**

| Column | Type |
|--------|------|
| `InvoiceDetailId` | `NUMBER` |
| `InvoiceSummaryId` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `ItemTypeId` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `Quantity` | `NUMBER` |
| `Price` | `NUMBER` |
| `DiscountTypeId` | `NUMBER` |
| `Tax` | `NUMBER` |
| `Discount` | `NUMBER` |
| `PromotionDiscount` | `NUMBER` |
| `LineDiscount` | `NUMBER` |
| `PackageDiscount` | `NUMBER` |
| `Amount` | `NUMBER` |
| `Allowance` | `NUMBER` |
| `Copay` | `NUMBER` |
| `Receivable` | `NUMBER` |
| `InsuranceDiscount` | `NUMBER` |
| `EmployeeId` | `NUMBER` |
| `ValuationCost` | `NUMBER` |
| `_Customer` | `NUMBER` |

### `DATAMART.DashboardMartDim_DiagnosisCode`

**Description:** View providing diagnosis or clinical data; dimension table for analytical joins; joins 27+ source tables.

**Columns (64):**

| Column | Type |
|--------|------|
| `OrderId` | `NUMBER` |
| `ItemId` | `NUMBER` |
| `InvoiceSummaryId` | `NUMBER` |
| `OrderExamDetail_DiagnosisCodeId1` | `NUMBER` |
| `OrderExamDetail_DiagnosisCodeId2` | `NUMBER` |
| `OrderExamDetail_DiagnosisCodeId3` | `NUMBER` |
| `OrderExamDetail_DiagnosisCodeId4` | `NUMBER` |
| `OrderExamDetail_DiagnosisCodeId5` | `NUMBER` |
| `OrderExamDetail_DiagnosisCodeId6` | `NUMBER` |
| `OrderExamDetail_DiagnosisCodeId7` | `NUMBER` |
| `OrderExamDetail_DiagnosisCodeId8` | `NUMBER` |
| `OrderExamDetail_DiagnosisCodeId9` | `NUMBER` |
| `OrderExamDetail_DiagnosisCodeId10` | `NUMBER` |
| `OrderExamDetail_DiagnosisCodeId11` | `NUMBER` |
| `OrderExamDetail_DiagnosisCodeId12` | `NUMBER` |
| `DiagnosisCode1_Name` | `TEXT` |
| `DiagnosisCode1_Description` | `TEXT` |
| `DiagnosisCode1_DiagnosisCategoryId` | `NUMBER` |
| `DiagnosisCategory1_Description` | `TEXT` |
| `DiagnosisCode2_Name` | `TEXT` |
| `DiagnosisCode2_Description` | `TEXT` |
| `DiagnosisCode2_DiagnosisCategoryId` | `NUMBER` |
| `DiagnosisCategory2_Description` | `TEXT` |
| `DiagnosisCode3_Name` | `TEXT` |
| `DiagnosisCode3_Description` | `TEXT` |
| `DiagnosisCode3_DiagnosisCategoryId` | `NUMBER` |
| `DiagnosisCategory3_Description` | `TEXT` |
| `DiagnosisCode4_Name` | `TEXT` |
| `DiagnosisCode4_Description` | `TEXT` |
| `DiagnosisCode4_DiagnosisCategoryId` | `NUMBER` |
| `DiagnosisCategory4_Description` | `TEXT` |
| `DiagnosisCode5_Name` | `TEXT` |
| `DiagnosisCode5_Description` | `TEXT` |
| `DiagnosisCode5_DiagnosisCategoryId` | `NUMBER` |
| `DiagnosisCategory5_Description` | `TEXT` |
| `DiagnosisCode6_Name` | `TEXT` |
| `DiagnosisCode6_Description` | `TEXT` |
| `DiagnosisCode6_DiagnosisCategoryId` | `NUMBER` |
| `DiagnosisCategory6_Description` | `TEXT` |
| `DiagnosisCode7_Name` | `TEXT` |
| `DiagnosisCode7_Description` | `TEXT` |
| `DiagnosisCode7_DiagnosisCategoryId` | `NUMBER` |
| `DiagnosisCategory7_Description` | `TEXT` |
| `DiagnosisCode8_Name` | `TEXT` |
| `DiagnosisCode8_Description` | `TEXT` |
| `DiagnosisCode8_DiagnosisCategoryId` | `NUMBER` |
| `DiagnosisCategory8_Description` | `TEXT` |
| `DiagnosisCode9_Name` | `TEXT` |
| `DiagnosisCode9_Description` | `TEXT` |
| `DiagnosisCode9_DiagnosisCategoryId` | `NUMBER` |
| `DiagnosisCategory9_Description` | `TEXT` |
| `DiagnosisCode10_Name` | `TEXT` |
| `DiagnosisCode10_Description` | `TEXT` |
| `DiagnosisCode10_DiagnosisCategoryId` | `NUMBER` |
| `DiagnosisCategory10_Description` | `TEXT` |
| `DiagnosisCode11_Name` | `TEXT` |
| `DiagnosisCode11_Description` | `TEXT` |
| `DiagnosisCode11_DiagnosisCategoryId` | `NUMBER` |
| `DiagnosisCategory11_Description` | `TEXT` |
| `DiagnosisCode12_Name` | `TEXT` |
| `DiagnosisCode12_Description` | `TEXT` |
| `DiagnosisCode12_DiagnosisCategoryId` | `NUMBER` |
| `DiagnosisCategory12_Description` | `TEXT` |
| `_Customer` | `NUMBER` |

### `DATAMART.DashboardMartFactPatientMarketingDim_PatientInsurance`

**Description:** View providing patient/member data; insurance plan or coverage data; dimension table for analytical joins; joins 3+ source tables.

**Columns (5):**

| Column | Type |
|--------|------|
| `PatientId` | `NUMBER` |
| `Name` | `TEXT` |
| `CarrierKey` | `TEXT` |
| `CompanyInfoId` | `TEXT` |
| `_Customer` | `NUMBER` |

### `DATAMART.DashboardMartFact_AccountsReceivable_Dev`

**Description:** View providing fact table for analytical measures; contains financial measures.

**Columns (30):**

| Column | Type |
|--------|------|
| `Source` | `TEXT` |
| `_Customer` | `NUMBER` |
| `BalanceCategory` | `TEXT` |
| `CompanyId` | `TEXT` |
| `OfficeNum` | `TEXT` |
| `OfficeName` | `TEXT` |
| `PatientId` | `NUMBER` |
| `PatientFirstName` | `TEXT` |
| `PatientLastName` | `TEXT` |
| `PatientFullName` | `TEXT` |
| `OrderNum` | `NUMBER` |
| `OrderStatus` | `TEXT` |
| `OrderDate` | `TIMESTAMP_NTZ` |
| `OrderBalance` | `NUMBER` |
| `OrderBalanceDaysOutstanding` | `NUMBER` |
| `CarrierId` | `TEXT` |
| `CarrierName` | `TEXT` |
| `PlanId` | `NUMBER` |
| `PlanName` | `TEXT` |
| `ClaimId` | `NUMBER` |
| `ClaimStatus` | `TEXT` |
| `ClaimLatestNote` | `TEXT` |
| `ClaimServiceDate` | `TIMESTAMP_NTZ` |
| `ClaimInsuranceBalance` | `NUMBER` |
| `ClaimPatientBalance` | `NUMBER` |
| `ClaimTotalBalance` | `NUMBER` |
| `ClaimBalanceDaysOutstanding` | `NUMBER` |
| `BalanceDue` | `NUMBER` |
| `BalanceType` | `TEXT` |
| `DaysOutstanding` | `NUMBER` |

### `DATAMART.DashboardMartFact_Billing`

**Description:** View providing financial or billing data; fact table for analytical measures; joins 10+ source tables.

**Columns (39):**

| Column | Type |
|--------|------|
| `_Customer` | `NUMBER` |
| `Amount` | `NUMBER` |
| `DateTime` | `TIMESTAMP_NTZ` |
| `BillingTransactionId` | `NUMBER` |
| `MonthCloseId` | `NUMBER` |
| `BillingTransactionTypeId` | `NUMBER` |
| `InsuranceARDelta` | `NUMBER` |
| `PatientARDelta` | `NUMBER` |
| `BillingPaymentId` | `NUMBER` |
| `PaidAmount` | `NUMBER` |
| `IsOnHold` | `BOOLEAN` |
| `CurrentState` | `TEXT` |
| `CurrentStateGroup` | `TEXT` |
| `IsBilled` | `BOOLEAN` |
| `IsReadyToBill` | `BOOLEAN` |
| `IsRejected` | `BOOLEAN` |
| `BillingClaimDataId` | `NUMBER` |
| `BillingClaimId` | `NUMBER` |
| `OfficeKey` | `TEXT` |
| `PatientFirstName` | `TEXT` |
| `PatientLastName` | `TEXT` |
| `PatientName` | `TEXT` |
| `InsuredFirstName` | `TEXT` |
| `InsuredLastName` | `TEXT` |
| `InsuredName` | `TEXT` |
| `AuthorizationNumber` | `TEXT` |
| `IsCurrent` | `TEXT` |
| `PlanName` | `TEXT` |
| `CarrierId` | `TEXT` |
| `OrderId` | `NUMBER` |
| `ServiceDate` | `TIMESTAMP_NTZ` |
| `ServiceDate_DDMMYYYY` | `TEXT` |
| `CompanyInfoId` | `TEXT` |
| `DepositDate` | `TIMESTAMP_NTZ` |
| `DepositDate_DDMMYYYY` | `TEXT` |
| `PaymentNumber` | `TEXT` |
| `Name` | `TEXT` |
| `CarrierName` | `TEXT` |
| `OfficeName` | `TEXT` |

### `DATAMART.DashboardMartFact_BillingClaimAging`

**Description:** View providing claims or invoice transactions; financial or billing data; fact table for analytical measures; includes demographic attributes; joins 6+ source tables.

**Columns (15):**

| Column | Type |
|--------|------|
| `AGE` | `TEXT` |
| `CUTOFFDATE` | `DATE` |
| `ARAMOUNT` | `NUMBER` |
| `CompanyInfoId` | `TEXT` |
| `OfficeKey` | `TEXT` |
| `Office_Name` | `TEXT` |
| `BillingClaimId` | `NUMBER` |
| `PatientFirstName` | `TEXT` |
| `PatientLastName` | `TEXT` |
| `PatientName` | `TEXT` |
| `CarrierName` | `TEXT` |
| `PlanName` | `TEXT` |
| `ServiceDate` | `TIMESTAMP_NTZ` |
| `ServiceDate_DDMMYYYY` | `TEXT` |
| `_Customer` | `NUMBER` |

### `DATAMART.DashboardMartFact_NetCollections`

**Description:** View providing fact table for analytical measures; contains financial measures; joins 7+ source tables.

**Columns (27):**

| Column | Type |
|--------|------|
| `Billing Id` | `NUMBER` |
| `Carrier Key` | `TEXT` |
| `_Customer` | `NUMBER` |
| `Company Id` | `TEXT` |
| `Transaction Date` | `TIMESTAMP_NTZ` |
| `Deposit Date` | `TIMESTAMP_NTZ` |
| `First Payment Date` | `TIMESTAMP_NTZ` |
| `Fully Paid Date` | `TIMESTAMP_NTZ` |
| `Order Date` | `TIMESTAMP_NTZ` |
| `Item Type` | `TEXT` |
| `Item Type Grouping` | `TEXT` |
| `Order Type Grouping` | `TEXT` |
| `Item Id` | `NUMBER` |
| `CPT Code` | `TEXT` |
| `OfficeKey` | `TEXT` |
| `Location` | `TEXT` |
| `Order Id` | `NUMBER` |
| `Patient Id` | `NUMBER` |
| `Patient` | `TEXT` |
| `Staff` | `TEXT` |
| `Doctor` | `TEXT` |
| `Source` | `TEXT` |
| `Transaction Type` | `TEXT` |
| `Payment Type` | `TEXT` |
| `View Type` | `TEXT` |
| `Payment Amount` | `NUMBER` |
| `Tax` | `NUMBER` |

### `DATAMART.DashboardMartFact_PatientDemographics`

**Description:** View providing patient/member data; fact table for analytical measures; joins 16+ source tables.

**Columns (44):**

| Column | Type |
|--------|------|
| `PatientId` | `NUMBER` |
| `CompanyInfoId` | `TEXT` |
| `PatientName` | `TEXT` |
| `NickName` | `TEXT` |
| `ReferralTypeId` | `NUMBER` |
| `IsPatient` | `TEXT` |
| `ProviderEmployeeId` | `NUMBER` |
| `Sex` | `TEXT` |
| `Email` | `TEXT` |
| `EmailType` | `TEXT` |
| `HomeOfficeKey` | `TEXT` |
| `IsAddressBad` | `TEXT` |
| `IsEmailBad` | `TEXT` |
| `IsInActive` | `TEXT` |
| `LastExamDate` | `TIMESTAMP_NTZ` |
| `BirthDate` | `DATE` |
| `ResponsiblePartyFullName` | `TEXT` |
| `_Customer` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `OfficeKey` | `TEXT` |
| `DateTime` | `TIMESTAMP_NTZ` |
| `InvoiceSummaryId` | `NUMBER` |
| `Address_Line1` | `TEXT` |
| `Address_Line2` | `TEXT` |
| `Address_City` | `TEXT` |
| `Address_State` | `TEXT` |
| `Address_ZipCode` | `TEXT` |
| `IsPrimaryPhone` | `TEXT` |
| `Phone_Number` | `TEXT` |
| `Phone_Extension` | `TEXT` |
| `ReferralType` | `TEXT` |
| `CarrierName` | `TEXT` |
| `PlanName` | `TEXT` |
| `CarrierKey` | `TEXT` |
| `IsPrimaryInsurance` | `TEXT` |
| `FrameCollectionId` | `NUMBER` |
| `FrameCollection_Description` | `TEXT` |
| `NextPatientRecall_Date` | `TIMESTAMP_NTZ` |
| `NextPatientRecallType_Description` | `TEXT` |
| `Office_Name` | `TEXT` |
| `Is Referral` | `TEXT` |
| `Is Carrier` | `TEXT` |
| `OrderCarrier` | `TEXT` |
| `ActiveStatus` | `TEXT` |

### `DATAMART.DashboardMartFact_SalesByProviderAndStaff`

**Description:** View providing provider/practitioner information; fact table for analytical measures; user or staff data; contains financial measures; joins 22+ source tables.

**Columns (39):**

| Column | Type |
|--------|------|
| `InvoiceSummaryId` | `NUMBER` |
| `InvoiceDetailId` | `NUMBER` |
| `Quantity` | `NUMBER` |
| `Tax` | `NUMBER` |
| `Amount` | `NUMBER` |
| `NetSales` | `NUMBER` |
| `Receivable` | `NUMBER` |
| `DoctorEmployeeId` | `NUMBER` |
| `DoctorEmployeeName` | `TEXT` |
| `OfficeKey` | `TEXT` |
| `Office_Name` | `TEXT` |
| `DateTime` | `TIMESTAMP_NTZ` |
| `PosTransactionId` | `NUMBER` |
| `PosTransactionTypeId` | `NUMBER` |
| `PosTransactionType_Description` | `TEXT` |
| `PatientId` | `NUMBER` |
| `CompanyInfoId` | `TEXT` |
| `OrderId` | `NUMBER` |
| `EmployeeId` | `NUMBER` |
| `AssociateName` | `TEXT` |
| `ItemId` | `NUMBER` |
| `Item_Name` | `TEXT` |
| `ItemTypeId` | `NUMBER` |
| `UPCCode` | `TEXT` |
| `Item_Description` | `TEXT` |
| `SalesTypeName` | `TEXT` |
| `FrameCollection_Description` | `TEXT` |
| `Manufacturer_Name` | `TEXT` |
| `VendorName` | `TEXT` |
| `CPTCode` | `TEXT` |
| `DiagCode1_Code` | `TEXT` |
| `DiagCode2_Code` | `TEXT` |
| `DiagCode3_Code` | `TEXT` |
| `DiagCode1_Description` | `TEXT` |
| `DiagCode2_Description` | `TEXT` |
| `DiagCode3_Description` | `TEXT` |
| `ConcatDiagCodes` | `TEXT` |
| `Sales Type` | `TEXT` |
| `_Customer` | `NUMBER` |

### `DATAMART.DashboardMartFact_SalesByProviderAndStaff_PRODReport`

**Description:** View providing provider/practitioner information; aggregated summary/KPI metrics; fact table for analytical measures; user or staff data; contains financial measures; joins 22+ source tables.

**Columns (40):**

| Column | Type |
|--------|------|
| `InvoiceSummaryId` | `NUMBER` |
| `InvoiceDetailId` | `NUMBER` |
| `Retail` | `NUMBER` |
| `Quantity` | `NUMBER` |
| `Tax` | `NUMBER` |
| `Amount` | `NUMBER` |
| `NetSales` | `NUMBER` |
| `Receivable` | `NUMBER` |
| `DoctorEmployeeId` | `NUMBER` |
| `DoctorEmployeeName` | `TEXT` |
| `OfficeKey` | `TEXT` |
| `Office_Name` | `TEXT` |
| `DateTime` | `TIMESTAMP_NTZ` |
| `PosTransactionId` | `NUMBER` |
| `PosTransactionTypeId` | `NUMBER` |
| `PosTransactionType_Description` | `TEXT` |
| `PatientId` | `NUMBER` |
| `CompanyInfoId` | `TEXT` |
| `OrderId` | `NUMBER` |
| `EmployeeId` | `NUMBER` |
| `AssociateName` | `TEXT` |
| `ItemId` | `NUMBER` |
| `Item_Name` | `TEXT` |
| `ItemTypeId` | `NUMBER` |
| `UPCCode` | `TEXT` |
| `Item_Description` | `TEXT` |
| `SalesTypeName` | `TEXT` |
| `FrameCollection_Description` | `TEXT` |
| `Manufacturer_Name` | `TEXT` |
| `VendorName` | `TEXT` |
| `CPTCode` | `TEXT` |
| `DiagCode1_Code` | `TEXT` |
| `DiagCode2_Code` | `TEXT` |
| `DiagCode3_Code` | `TEXT` |
| `DiagCode1_Description` | `TEXT` |
| `DiagCode2_Description` | `TEXT` |
| `DiagCode3_Description` | `TEXT` |
| `ConcatDiagCodes` | `TEXT` |
| `Sales Type` | `TEXT` |
| `_Customer` | `NUMBER` |

### `DATAMART.DashboardPatientMarketingDim_SalesRevenue`

**Description:** View providing patient/member data; financial or billing data; dimension table for analytical joins; joins 22+ source tables.

**Columns (51):**

| Column | Type |
|--------|------|
| `PatientId` | `NUMBER` |
| `FirstName` | `TEXT` |
| `LastName` | `TEXT` |
| `PatientName` | `TEXT` |
| `CompanyInfoId` | `TEXT` |
| `PosTransactionTypeId` | `NUMBER` |
| `PosTransactionType_Description` | `TEXT` |
| `PosTransactionId` | `NUMBER` |
| `PosTransaction_DateTime` | `TIMESTAMP_NTZ` |
| `OfficeKey` | `TEXT` |
| `TransactionAssociate` | `NUMBER` |
| `OrderAssociateName` | `TEXT` |
| `Associate` | `NUMBER` |
| `CombinedInvoiceAssociateName` | `TEXT` |
| `OriginalAssociate` | `NUMBER` |
| `ItemTypeId` | `NUMBER` |
| `InvoiceSummaryId` | `NUMBER` |
| `OrderId` | `NUMBER` |
| `DoctorEmployeeId` | `NUMBER` |
| `DoctorId` | `NUMBER` |
| `DOCTOREMPLOYEENAME` | `TEXT` |
| `ItemType_Description` | `TEXT` |
| `ItemTypeGroup` | `TEXT` |
| `Retail` | `NUMBER` |
| `Tax` | `NUMBER` |
| `Discount` | `NUMBER` |
| `Amount` | `NUMBER` |
| `Allowance` | `NUMBER` |
| `Receivable` | `NUMBER` |
| `Copay` | `NUMBER` |
| `InvoiceCount` | `NUMBER` |
| `DayCloseId` | `NUMBER` |
| `NetSales` | `NUMBER` |
| `OfficeType_Value` | `TEXT` |
| `Region_Value` | `TEXT` |
| `Office_Name` | `TEXT` |
| `SalesCategory` | `NUMBER` |
| `IsFittingFee` | `TEXT` |
| `IsProcedure` | `TEXT` |
| `ItemId` | `NUMBER` |
| `Quantity` | `NUMBER` |
| `CLStyleId` | `NUMBER` |
| `VendorName` | `TEXT` |
| `LENSSTYLE` | `TEXT` |
| `LensName` | `TEXT` |
| `CLTypeId` | `NUMBER` |
| `CLStyle_Style` | `TEXT` |
| `CLType_Value` | `TEXT` |
| `IsHard` | `TEXT` |
| `LensCategory` | `TEXT` |
| `_Customer` | `NUMBER` |

### `DATAMART.ICARE_REFERENCE_PATIENT`

**Description:** View providing patient/member data; referral data.

**Columns (6):**

| Column | Type |
|--------|------|
| `PatientId` | `NUMBER` |
| `LastName` | `TEXT` |
| `FirstName` | `TEXT` |
| `BirthDate` | `TEXT` |
| `Email` | `TEXT` |
| `NickName` | `TEXT` |

### `DATAMART.Reference_PatientAndAddress`

**Description:** View providing patient/member data; location or office site data; referral data; joins 3+ source tables.

**Columns (32):**

| Column | Type |
|--------|------|
| `PatientId` | `NUMBER` |
| `AddressId` | `NUMBER` |
| `_Customer` | `NUMBER` |
| `DELETED_FLAG` | `TEXT` |
| `Patient_LastName` | `TEXT` |
| `Patient_FirstName` | `TEXT` |
| `Patient_BirthDate` | `TEXT` |
| `Patient_Sex` | `TEXT` |
| `Patient_MiddleInitial` | `TEXT` |
| `Patient_Email` | `TEXT` |
| `Patient_ResponsiblePatientId` | `NUMBER` |
| `Patient_ReferralTypeId` | `NUMBER` |
| `Patient_LastExamDate` | `TIMESTAMP_NTZ` |
| `Patient_HomeOfficeKey` | `TEXT` |
| `Patient_IsAddressBad` | `TEXT` |
| `Patient_IsEmailBad` | `TEXT` |
| `Patient_IsInactive` | `TEXT` |
| `Patient_NickName` | `TEXT` |
| `Patient_ProviderEmployeeId` | `NUMBER` |
| `Patient_IsPatient` | `TEXT` |
| `Patient_ResponsiblePartyFullName` | `TEXT` |
| `Patient_CompanyInfoId` | `TEXT` |
| `Patient_DELETED_FLAG` | `TEXT` |
| `Address_Line1` | `TEXT` |
| `Address_Line2` | `TEXT` |
| `Address_City` | `TEXT` |
| `Address_State` | `TEXT` |
| `Address_ZipCode` | `TEXT` |
| `Address_IsPrimary` | `TEXT` |
| `Address_DELETED_FLAG` | `TEXT` |
| `ReferralType_Name` | `TEXT` |
| `ReferralType_DELETED_FLAG` | `TEXT` |

