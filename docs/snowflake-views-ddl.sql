-- ============================================================
-- DEV_ANALYTICS View Definitions (DATAWAREHOUSE + DATAMART)
-- Extracted: 2026-03-17T12:06:15.029443
-- Total views: 136
-- ============================================================


-- ============================================================
-- SCHEMA: DATAWAREHOUSE
-- ============================================================

-- View: DATAWAREHOUSE.Address
-- Columns: 15
create or replace view "Address"(
	ID,
	"AddressId",
	"_Customer",
	"Line1",
	"Line2",
	"City",
	"State",
	"ZipCode",
	"IsPrimary",
	"AddressTypeId",
	"AddressType_AddressType",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "AddressId" "AddressId", "_Customer" "_Customer", "Line1" "Line1", "Line2" "Line2", "City" "City", "State" "State", "ZipCode" "ZipCode", "IsPrimary" "IsPrimary", "AddressTypeId" "AddressTypeId", "AddressType_AddressType" "AddressType_AddressType", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Address_HUB";

-- View: DATAWAREHOUSE.AddressType
-- Columns: 8
create or replace view "AddressType"(
	ID,
	"AddressTypeId",
	"_Customer",
	"Value",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "AddressTypeId" "AddressTypeId", "_Customer" "_Customer", "Value" "Value", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."AddressType_HUB";

-- View: DATAWAREHOUSE.BillingActivityReason
-- Columns: 9
create or replace view "BillingActivityReason"(
	ID,
	"BillingActivityReasonId",
	"_Customer",
	"BillingClaimLineItemId",
	"BillingClaimLineItem_BillingClaimLineItem",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "BillingActivityReasonId" "BillingActivityReasonId", "_Customer" "_Customer", "BillingClaimLineItemId" "BillingClaimLineItemId", "BillingClaimLineItem_BillingClaimLineItem" "BillingClaimLineItem_BillingClaimLineItem", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."BillingActivityReason_HUB";

-- View: DATAWAREHOUSE.BillingAdjustmentType
-- Columns: 8
create or replace view "BillingAdjustmentType"(
	ID,
	"BillingAdjustmentTypeId",
	"_Customer",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "BillingAdjustmentTypeId" "BillingAdjustmentTypeId", "_Customer" "_Customer", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."BillingAdjustmentType_HUB";

-- View: DATAWAREHOUSE.BillingClaim
-- Columns: 14
create or replace view "BillingClaim"(
	ID,
	"BillingClaimId",
	"_Customer",
	"ServiceDate",
	"BillingDate",
	"CompanyInfoId",
	"CompanyInfo_CompanyInfo",
	"PatientId",
	"Patient_Patient",
	"ExternalClaimNumber",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "BillingClaimId" "BillingClaimId", "_Customer" "_Customer", "ServiceDate" "ServiceDate", "BillingDate" "BillingDate", "CompanyInfoId" "CompanyInfoId", "CompanyInfo_CompanyInfo" "CompanyInfo_CompanyInfo", "PatientId" "PatientId", "Patient_Patient" "Patient_Patient", "ExternalClaimNumber" "ExternalClaimNumber", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."BillingClaim_HUB";

-- View: DATAWAREHOUSE.BillingClaimData
-- Columns: 33
create or replace view "BillingClaimData"(
	ID,
	"BillingClaimDataId",
	"BillingClaimId",
	"_Customer",
	"OfficeKey",
	"AuthorizationNumber",
	"CarrierName",
	"PlanName",
	"InsuredId",
	"InsuredFirstName",
	"InsuredLastName",
	"InsuredAddress",
	"InsuredCity",
	"InsuredState",
	"InsuredZip",
	"PatientFirstName",
	"PatientLastName",
	"RenderingProviderFirstName",
	"RenderingProviderLastName",
	"IsCurrent",
	"PatientBirthDate",
	"Office_Office",
	"CarrierKey",
	"Carrier_Carrier",
	"PlanId",
	"Plan_Plan",
	"BillingClaim_BillingClaim",
	DELETED_FLAG,
	DELETED_DATETIME,
	"RenderingProviderNPI",
	"RenderingProviderNumber",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "BillingClaimDataId" "BillingClaimDataId", "BillingClaimId" "BillingClaimId", "_Customer" "_Customer", "OfficeKey" "OfficeKey", "AuthorizationNumber" "AuthorizationNumber", "CarrierName" "CarrierName", "PlanName" "PlanName", "InsuredId" "InsuredId", "InsuredFirstName" "InsuredFirstName", "InsuredLastName" "InsuredLastName", "InsuredAddress" "InsuredAddress", "InsuredCity" "InsuredCity", "InsuredState" "InsuredState", "InsuredZip" "InsuredZip", "PatientFirstName" "PatientFirstName", "PatientLastName" "PatientLastName", "RenderingProviderFirstName" "RenderingProviderFirstName", "RenderingProviderLastName" "RenderingProviderLastName", "IsCurrent" "IsCurrent", "PatientBirthDate" "PatientBirthDate", "Office_Office" "Office_Office", "CarrierKey" "CarrierKey", "Carrier_Carrier" "Carrier_Carrier", "PlanId" "PlanId", "Plan_Plan" "Plan_Plan", "BillingClaim_BillingClaim" "BillingClaim_BillingClaim", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RenderingProviderNPI" "RenderingProviderNPI", "RenderingProviderNumber" "RenderingProviderNumber", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."BillingClaimData_HUB";

-- View: DATAWAREHOUSE.BillingClaimLineItem
-- Columns: 18
create or replace view "BillingClaimLineItem"(
	ID,
	"BillingClaimLineItemId",
	"_Customer",
	"ItemTypeId",
	"BillingClaimId",
	"CurrentState",
	"Description",
	"ItemType_ItemType",
	"BillingClaim_BillingClaim",
	"ItemId",
	"Item_Item",
	DELETED_FLAG,
	DELETED_DATETIME,
	"RetailAmount",
	"PaidAmount",
	"OrderId",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "BillingClaimLineItemId" "BillingClaimLineItemId", "_Customer" "_Customer", "ItemTypeId" "ItemTypeId", "BillingClaimId" "BillingClaimId", "CurrentState" "CurrentState", "Description" "Description", "ItemType_ItemType" "ItemType_ItemType", "BillingClaim_BillingClaim" "BillingClaim_BillingClaim", "ItemId" "ItemId", "Item_Item" "Item_Item", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RetailAmount" "RetailAmount", "PaidAmount" "PaidAmount", "OrderId" "OrderId", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."BillingClaimLineItem_HUB";

-- View: DATAWAREHOUSE.BillingClaimOrder
-- Columns: 11
create or replace view "BillingClaimOrder"(
	ID,
	"BillingClaimOrderId",
	"_Customer",
	"BillingClaimId",
	"OrderId",
	"BillingClaim_BillingClaim",
	"Orders_Order",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "BillingClaimOrderId" "BillingClaimOrderId", "_Customer" "_Customer", "BillingClaimId" "BillingClaimId", "OrderId" "OrderId", "BillingClaim_BillingClaim" "BillingClaim_BillingClaim", "Orders_Order" "Orders_Order", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."BillingClaimOrder_HUB";

-- View: DATAWAREHOUSE.BillingLabFeeTransaction
-- Columns: 16
create or replace view "BillingLabFeeTransaction"(
	ID,
	"BillingLabFeeTransactionId",
	"_Customer",
	"BillingClaimLineItemId",
	"BillingPaymentId",
	"CreatedDateTimeUtc",
	"Amount",
	"BillingTransactionTypeId",
	DELETED_FLAG,
	DELETED_DATETIME,
	"BillingTransactionType_BillingTransactionType",
	"BillingClaimLineItem_BillingClaimLineItem",
	"BillingPayment_BillingPayment",
	"CreatedDateTime",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "BillingLabFeeTransactionId" "BillingLabFeeTransactionId", "_Customer" "_Customer", "BillingClaimLineItemId" "BillingClaimLineItemId", "BillingPaymentId" "BillingPaymentId", "CreatedDateTimeUtc" "CreatedDateTimeUtc", "Amount" "Amount", "BillingTransactionTypeId" "BillingTransactionTypeId", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "BillingTransactionType_BillingTransactionType" "BillingTransactionType_BillingTransactionType", "BillingClaimLineItem_BillingClaimLineItem" "BillingClaimLineItem_BillingClaimLineItem", "BillingPayment_BillingPayment" "BillingPayment_BillingPayment", "CreatedDateTime" "CreatedDateTime", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."BillingLabFeeTransaction_HUB";

-- View: DATAWAREHOUSE.BillingLineDetail
-- Columns: 17
create or replace view "BillingLineDetail"(
	ID,
	"BillingLineDetailId",
	"_Customer",
	"BillingClaimId",
	"BillingClaimLineItemId",
	"ProcedureCode",
	"IsCurrent",
	"BillingClaim_BillingClaim",
	"BillingClaimLineItem_BillingClaimLineItem",
	"ServiceDateTime",
	DELETED_FLAG,
	DELETED_DATETIME,
	"Quantity",
	"ProcedureCodeDescription",
	"ChargeAmount",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "BillingLineDetailId" "BillingLineDetailId", "_Customer" "_Customer", "BillingClaimId" "BillingClaimId", "BillingClaimLineItemId" "BillingClaimLineItemId", "ProcedureCode" "ProcedureCode", "IsCurrent" "IsCurrent", "BillingClaim_BillingClaim" "BillingClaim_BillingClaim", "BillingClaimLineItem_BillingClaimLineItem" "BillingClaimLineItem_BillingClaimLineItem", "ServiceDateTime" "ServiceDateTime", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "Quantity" "Quantity", "ProcedureCodeDescription" "ProcedureCodeDescription", "ChargeAmount" "ChargeAmount", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."BillingLineDetail_HUB";

-- View: DATAWAREHOUSE.BillingLineItemCurrentAR
-- Columns: 9
create or replace view "BillingLineItemCurrentAR"(
	ID,
	"BillingClaimLineItemId",
	"_Customer",
	"InsuranceAR",
	"PatientAR",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "BillingClaimLineItemId" "BillingClaimLineItemId", "_Customer" "_Customer", "InsuranceAR" "InsuranceAR", "PatientAR" "PatientAR", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."BillingLineItemCurrentAR_HUB";

-- View: DATAWAREHOUSE.BillingPayment
-- Columns: 24
create or replace view "BillingPayment"(
	ID,
	"BillingPaymentId",
	"_Customer",
	"CarrierKey",
	"PatientId",
	"BillingPaymentTypeId",
	"Amount",
	"Number",
	"Date",
	"DepositDate",
	"NumberOfSections",
	"OutstandingAmount",
	"IsDeleted",
	"CompanyInfoId",
	"IsCommitted",
	"IsEra",
	"CompanyInfo_CompanyInfo",
	"Patient_Patient",
	"Carrier_Carrier",
	"BillingPaymentType_BillingPaymentType",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "BillingPaymentId" "BillingPaymentId", "_Customer" "_Customer", "CarrierKey" "CarrierKey", "PatientId" "PatientId", "BillingPaymentTypeId" "BillingPaymentTypeId", "Amount" "Amount", "Number" "Number", "Date" "Date", "DepositDate" "DepositDate", "NumberOfSections" "NumberOfSections", "OutstandingAmount" "OutstandingAmount", "IsDeleted" "IsDeleted", "CompanyInfoId" "CompanyInfoId", "IsCommitted" "IsCommitted", "IsEra" "IsEra", "CompanyInfo_CompanyInfo" "CompanyInfo_CompanyInfo", "Patient_Patient" "Patient_Patient", "Carrier_Carrier" "Carrier_Carrier", "BillingPaymentType_BillingPaymentType" "BillingPaymentType_BillingPaymentType", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."BillingPayment_HUB";

-- View: DATAWAREHOUSE.BillingPaymentSection
-- Columns: 9
create or replace view "BillingPaymentSection"(
	ID,
	"BillingPaymentSectionId",
	"_Customer",
	"BillingPaymentId",
	"BillingPayment_BillingPayment",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "BillingPaymentSectionId" "BillingPaymentSectionId", "_Customer" "_Customer", "BillingPaymentId" "BillingPaymentId", "BillingPayment_BillingPayment" "BillingPayment_BillingPayment", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."BillingPaymentSection_HUB";

-- View: DATAWAREHOUSE.BillingPaymentType
-- Columns: 8
create or replace view "BillingPaymentType"(
	ID,
	"BillingPaymentTypeId",
	"_Customer",
	"Value",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "BillingPaymentTypeId" "BillingPaymentTypeId", "_Customer" "_Customer", "Value" "Value", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."BillingPaymentType_HUB";

-- View: DATAWAREHOUSE.BillingTransaction
-- Columns: 33
create or replace view "BillingTransaction"(
	ID,
	"BillingTransactionId",
	"BillingClaimLineItemId",
	"_Customer",
	"OrderId",
	"BillingTransactionTypeId",
	"BillingPaymentId",
	"Amount",
	"DateTime",
	"InsuranceAR",
	"PatientAR",
	"InsuranceARDelta",
	"PatientARDelta",
	"BillingTransactionType_BillingTransactionType",
	"BillingPayment_BillingPayment",
	"Order_Order",
	"MonthCloseId",
	"WorkflowActivityId",
	"WorkflowActivity_WorkflowActivity",
	"BillingClaimLineItem_BillingClaimLineItem",
	"BillingClaimId",
	"BillingWriteoffReasonId",
	"BillingAdjustmentTypeId",
	"BillingWriteoffReason_BillingWriteoffReason",
	"BillingAdjustmentType_BillingAdjustmentType",
	DELETED_FLAG,
	DELETED_DATETIME,
	"PatientInsurancePayment",
	"CarrierPayment",
	"CarrierCredit",
	"PatientCredit",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "BillingTransactionId" "BillingTransactionId", "BillingClaimLineItemId" "BillingClaimLineItemId", "_Customer" "_Customer", "OrderId" "OrderId", "BillingTransactionTypeId" "BillingTransactionTypeId", "BillingPaymentId" "BillingPaymentId", "Amount" "Amount", "DateTime" "DateTime", "InsuranceAR" "InsuranceAR", "PatientAR" "PatientAR", "InsuranceARDelta" "InsuranceARDelta", "PatientARDelta" "PatientARDelta", "BillingTransactionType_BillingTransactionType" "BillingTransactionType_BillingTransactionType", "BillingPayment_BillingPayment" "BillingPayment_BillingPayment", "Order_Order" "Order_Order", "MonthCloseId" "MonthCloseId", "WorkflowActivityId" "WorkflowActivityId", "WorkflowActivity_WorkflowActivity" "WorkflowActivity_WorkflowActivity", "BillingClaimLineItem_BillingClaimLineItem" "BillingClaimLineItem_BillingClaimLineItem", "BillingClaimId" "BillingClaimId", "BillingWriteoffReasonId" "BillingWriteoffReasonId", "BillingAdjustmentTypeId" "BillingAdjustmentTypeId", "BillingWriteoffReason_BillingWriteoffReason" "BillingWriteoffReason_BillingWriteoffReason", "BillingAdjustmentType_BillingAdjustmentType" "BillingAdjustmentType_BillingAdjustmentType", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "PatientInsurancePayment" "PatientInsurancePayment", "CarrierPayment" "CarrierPayment", "CarrierCredit" "CarrierCredit", "PatientCredit" "PatientCredit", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."BillingTransaction_HUB";

-- View: DATAWAREHOUSE.BillingTransactionType
-- Columns: 9
create or replace view "BillingTransactionType"(
	ID,
	"BillingTransactionTypeId",
	"_Customer",
	"Description",
	"IsGLTransaction",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "BillingTransactionTypeId" "BillingTransactionTypeId", "_Customer" "_Customer", "Description" "Description", "IsGLTransaction" "IsGLTransaction", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."BillingTransactionType_HUB";

-- View: DATAWAREHOUSE.BillingWriteoffReason
-- Columns: 9
create or replace view "BillingWriteoffReason"(
	ID,
	"BillingWriteoffReasonId",
	"_Customer",
	"Code",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "BillingWriteoffReasonId" "BillingWriteoffReasonId", "_Customer" "_Customer", "Code" "Code", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."BillingWriteoffReason_HUB";

-- View: DATAWAREHOUSE.CLHardRX
-- Columns: 13
create or replace view "CLHardRX"(
	ID,
	"CLHardRXId",
	"_Customer",
	"OrderId",
	"DoctorEmployeeId",
	"Employee_Employee",
	"Order_Order",
	DELETED_FLAG,
	DELETED_DATETIME,
	"OutsideDoctorId",
	"OutsideDoctor_OutsideDoctor",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "CLHardRXId" "CLHardRXId", "_Customer" "_Customer", "OrderId" "OrderId", "DoctorEmployeeId" "DoctorEmployeeId", "Employee_Employee" "Employee_Employee", "Order_Order" "Order_Order", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "OutsideDoctorId" "OutsideDoctorId", "OutsideDoctor_OutsideDoctor" "OutsideDoctor_OutsideDoctor", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."CLHardRX_HUB";

-- View: DATAWAREHOUSE.CLManufacturer
-- Columns: 8
create or replace view "CLManufacturer"(
	ID,
	"CLManufacturerId",
	"_Customer",
	"VendorName",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "CLManufacturerId" "CLManufacturerId", "_Customer" "_Customer", "VendorName" "VendorName", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."CLManufacturer_HUB";

-- View: DATAWAREHOUSE.CLMedicallyNecessaryRX
-- Columns: 9
create or replace view "CLMedicallyNecessaryRX"(
	ID,
	"CLMedicallyNecessaryRXId",
	"_Customer",
	"OrderId",
	"Order_Order",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "CLMedicallyNecessaryRXId" "CLMedicallyNecessaryRXId", "_Customer" "_Customer", "OrderId" "OrderId", "Order_Order" "Order_Order", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."CLMedicallyNecessaryRX_HUB";

-- View: DATAWAREHOUSE.CLOrder
-- Columns: 14
create or replace view "CLOrder"(
	ID,
	"OrderId",
	"_Customer",
	"DispenseTypeId",
	"DispenseNote",
	"CLSupplySourceId",
	"SupplierId",
	"Supplier_Supplier",
	"DispenseType_DispenseType",
	"CLSupplySource_CLSupplySource",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "OrderId" "OrderId", "_Customer" "_Customer", "DispenseTypeId" "DispenseTypeId", "DispenseNote" "DispenseNote", "CLSupplySourceId" "CLSupplySourceId", "SupplierId" "SupplierId", "Supplier_Supplier" "Supplier_Supplier", "DispenseType_DispenseType" "DispenseType_DispenseType", "CLSupplySource_CLSupplySource" "CLSupplySource_CLSupplySource", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."CLOrder_HUB";

-- View: DATAWAREHOUSE.CLPower
-- Columns: 11
create or replace view "CLPower"(
	ID,
	"CLPowerId",
	"_Customer",
	"CLStyleId",
	"Base",
	"Diameter",
	"CLStyle_CLStyle",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "CLPowerId" "CLPowerId", "_Customer" "_Customer", "CLStyleId" "CLStyleId", "Base" "Base", "Diameter" "Diameter", "CLStyle_CLStyle" "CLStyle_CLStyle", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."CLPower_HUB";

-- View: DATAWAREHOUSE.CLSoftRX
-- Columns: 14
create or replace view "CLSoftRX"(
	ID,
	"CLSoftRXId",
	"_Customer",
	"OrderId",
	"DoctorEmployeeId",
	"Employee_Employee",
	"Order_Order",
	"PatientExamId",
	DELETED_FLAG,
	DELETED_DATETIME,
	"OutsideDoctorId",
	"OutsideDoctor_OutsideDoctor",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "CLSoftRXId" "CLSoftRXId", "_Customer" "_Customer", "OrderId" "OrderId", "DoctorEmployeeId" "DoctorEmployeeId", "Employee_Employee" "Employee_Employee", "Order_Order" "Order_Order", "PatientExamId" "PatientExamId", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "OutsideDoctorId" "OutsideDoctorId", "OutsideDoctor_OutsideDoctor" "OutsideDoctor_OutsideDoctor", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."CLSoftRX_HUB";

-- View: DATAWAREHOUSE.CLStyle
-- Columns: 15
create or replace view "CLStyle"(
	ID,
	"CLStyleId",
	"_Customer",
	"CLManufacturerId",
	"Style",
	"IsHard",
	"CLTypeId",
	"CLManufacturer_CLManufacturer",
	"CLType_CLType",
	"CLStyleTypeId",
	"CLStyleType_CLStyleType",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "CLStyleId" "CLStyleId", "_Customer" "_Customer", "CLManufacturerId" "CLManufacturerId", "Style" "Style", "IsHard" "IsHard", "CLTypeId" "CLTypeId", "CLManufacturer_CLManufacturer" "CLManufacturer_CLManufacturer", "CLType_CLType" "CLType_CLType", "CLStyleTypeId" "CLStyleTypeId", "CLStyleType_CLStyleType" "CLStyleType_CLStyleType", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."CLStyle_HUB";

-- View: DATAWAREHOUSE.CLStyleType
-- Columns: 8
create or replace view "CLStyleType"(
	ID,
	"CLStyleTypeId",
	"_Customer",
	"Value",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "CLStyleTypeId" "CLStyleTypeId", "_Customer" "_Customer", "Value" "Value", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."CLStyleType_HUB";

-- View: DATAWAREHOUSE.CLSupplierManufacturer
-- Columns: 12
create or replace view "CLSupplierManufacturer"(
	ID,
	"CLSupplierManufacturerId",
	"_Customer",
	"CLManufacturerId",
	"SupplierId",
	"SupplierManufacturer",
	"CLManufacturer_CLManufacturer",
	"Supplier_Supplier",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "CLSupplierManufacturerId" "CLSupplierManufacturerId", "_Customer" "_Customer", "CLManufacturerId" "CLManufacturerId", "SupplierId" "SupplierId", "SupplierManufacturer" "SupplierManufacturer", "CLManufacturer_CLManufacturer" "CLManufacturer_CLManufacturer", "Supplier_Supplier" "Supplier_Supplier", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."CLSupplierManufacturer_HUB";

-- View: DATAWAREHOUSE.CLSupplySource
-- Columns: 9
create or replace view "CLSupplySource"(
	ID,
	"CLSupplySourceId",
	"_Customer",
	"CLSupplySourceSurrogateId",
	"Name",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "CLSupplySourceId" "CLSupplySourceId", "_Customer" "_Customer", "CLSupplySourceSurrogateId" "CLSupplySourceSurrogateId", "Name" "Name", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."CLSupplySource_HUB";

-- View: DATAWAREHOUSE.CLType
-- Columns: 8
create or replace view "CLType"(
	ID,
	"CLTypeId",
	"_Customer",
	"Value",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "CLTypeId" "CLTypeId", "_Customer" "_Customer", "Value" "Value", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."CLType_HUB";

-- View: DATAWAREHOUSE.CPTCode
-- Columns: 10
create or replace view "CPTCode"(
	ID,
	"CPTCodeId",
	"_Customer",
	"Code",
	"Description",
	"IsActive",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "CPTCodeId" "CPTCodeId", "_Customer" "_Customer", "Code" "Code", "Description" "Description", "IsActive" "IsActive", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."CPTCode_HUB";

-- View: DATAWAREHOUSE.Carrier
-- Columns: 9
create or replace view "Carrier"(
	ID,
	"CarrierKey",
	"_Customer",
	"Name",
	"IsPrepaid",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "CarrierKey" "CarrierKey", "_Customer" "_Customer", "Name" "Name", "IsPrepaid" "IsPrepaid", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Carrier_HUB";

-- View: DATAWAREHOUSE.CatElementLU
-- Columns: 10
create or replace view "CatElementLU"(
	ID,
	"CatElementLUId",
	"_Customer",
	"Value",
	"Type",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE,
	"Ordinal"
) as  SELECT *  FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."CatElementLU_HUB";

-- View: DATAWAREHOUSE.CompanyInfo
-- Columns: 8
create or replace view "CompanyInfo"(
	ID,
	"CompanyInfoId",
	"_Customer",
	"Name",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "CompanyInfoId" "CompanyInfoId", "_Customer" "_Customer", "Name" "Name", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."CompanyInfo_HUB";

-- View: DATAWAREHOUSE.CompanyItemType
-- Columns: 12
create or replace view "CompanyItemType"(
	ID,
	"CompanyItemTypeId",
	"_Customer",
	"CompanyInfoId",
	"SalesCategory",
	"CompanyInfo_CompanyInfo",
	"ItemTypeId",
	"ItemType_ItemType",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "CompanyItemTypeId" "CompanyItemTypeId", "_Customer" "_Customer", "CompanyInfoId" "CompanyInfoId", "SalesCategory" "SalesCategory", "CompanyInfo_CompanyInfo" "CompanyInfo_CompanyInfo", "ItemTypeId" "ItemTypeId", "ItemType_ItemType" "ItemType_ItemType", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."CompanyItemType_HUB";

-- View: DATAWAREHOUSE.Date
-- Columns: 26
create or replace view "Date"(
	ID,
	DATE,
	DATE_YYYY_MM_DD,
	DATE_YYYYMMDD,
	YEAR,
	YEAR_STR,
	YEAR_STR2,
	MONTH,
	MONTH_STR2,
	MONTH_NAME,
	MONTH_NAME3,
	DAY_IN_MONTH,
	DAY_IN_MONTH_STR2,
	DAY_IN_YEAR,
	DAY_IN_WEEK,
	DAY_IN_WEEK_NAME,
	DAY_IN_WEEK_STR3,
	QTR,
	YYYYQQ,
	YYQQ,
	WEEK_IN_YEAR,
	IS_CURRENT_DATE,
	MONTH_END,
	MONTH_START,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "DATE" "DATE", "DATE_YYYY_MM_DD" "DATE_YYYY_MM_DD", "DATE_YYYYMMDD" "DATE_YYYYMMDD", "YEAR" "YEAR", "YEAR_STR" "YEAR_STR", "YEAR_STR2" "YEAR_STR2", "MONTH" "MONTH", "MONTH_STR2" "MONTH_STR2", "MONTH_NAME" "MONTH_NAME", "MONTH_NAME3" "MONTH_NAME3", "DAY_IN_MONTH" "DAY_IN_MONTH", "DAY_IN_MONTH_STR2" "DAY_IN_MONTH_STR2", "DAY_IN_YEAR" "DAY_IN_YEAR", "DAY_IN_WEEK" "DAY_IN_WEEK", "DAY_IN_WEEK_NAME" "DAY_IN_WEEK_NAME", "DAY_IN_WEEK_STR3" "DAY_IN_WEEK_STR3", "QTR" "QTR", "YYYYQQ" "YYYYQQ", "YYQQ" "YYQQ", "WEEK_IN_YEAR" "WEEK_IN_YEAR", "IS_CURRENT_DATE" "IS_CURRENT_DATE", "MONTH_END" "MONTH_END", "MONTH_START" "MONTH_START", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Date_HUB";

-- View: DATAWAREHOUSE.DayClose
-- Columns: 10
create or replace view "DayClose"(
	ID,
	"DayCloseId",
	"_Customer",
	"OfficeKey",
	"TransactionDate",
	"Office_Office",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "DayCloseId" "DayCloseId", "_Customer" "_Customer", "OfficeKey" "OfficeKey", "TransactionDate" "TransactionDate", "Office_Office" "Office_Office", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."DayClose_HUB";

-- View: DATAWAREHOUSE.DayCloseDetail
-- Columns: 12
create or replace view "DayCloseDetail"(
	ID,
	"DayCloseDetailId",
	"_Customer",
	"DayCloseId",
	"PaymentTypeId",
	"Computed",
	"DayClose_DayClose",
	"PaymentType_PaymentType",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "DayCloseDetailId" "DayCloseDetailId", "_Customer" "_Customer", "DayCloseId" "DayCloseId", "PaymentTypeId" "PaymentTypeId", "Computed" "Computed", "DayClose_DayClose" "DayClose_DayClose", "PaymentType_PaymentType" "PaymentType_PaymentType", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."DayCloseDetail_HUB";

-- View: DATAWAREHOUSE.DiagnosisCategory
-- Columns: 8
create or replace view "DiagnosisCategory"(
	ID,
	"DiagnosisCategoryId",
	"_Customer",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "DiagnosisCategoryId" "DiagnosisCategoryId", "_Customer" "_Customer", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."DiagnosisCategory_HUB";

-- View: DATAWAREHOUSE.DiagnosisCode
-- Columns: 11
create or replace view "DiagnosisCode"(
	ID,
	"DiagnosisCodeId",
	"_Customer",
	"Name",
	"Description",
	"DiagnosisCategoryId",
	"DiagnosisCategory_DiagnosisCategory",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "DiagnosisCodeId" "DiagnosisCodeId", "_Customer" "_Customer", "Name" "Name", "Description" "Description", "DiagnosisCategoryId" "DiagnosisCategoryId", "DiagnosisCategory_DiagnosisCategory" "DiagnosisCategory_DiagnosisCategory", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."DiagnosisCode_HUB";

-- View: DATAWAREHOUSE.DiscountType
-- Columns: 17
create or replace view "DiscountType"(
	ID,
	"DiscountTypeId",
	"CompanyInfoId",
	"_Customer",
	"Description",
	"ViewOrder",
	"IsActive",
	"StartDate",
	"EndDate",
	"IsCoupon",
	"LabCode1",
	"LabCode2",
	"SystemUseCode",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "DiscountTypeId" "DiscountTypeId", "CompanyInfoId" "CompanyInfoId", "_Customer" "_Customer", "Description" "Description", "ViewOrder" "ViewOrder", "IsActive" "IsActive", "StartDate" "StartDate", "EndDate" "EndDate", "IsCoupon" "IsCoupon", "LabCode1" "LabCode1", "LabCode2" "LabCode2", "SystemUseCode" "SystemUseCode", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."DiscountType_HUB";

-- View: DATAWAREHOUSE.DispenseType
-- Columns: 9
create or replace view "DispenseType"(
	ID,
	"CompanyInfoId",
	"DispenseTypeId",
	"_Customer",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "CompanyInfoId" "CompanyInfoId", "DispenseTypeId" "DispenseTypeId", "_Customer" "_Customer", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."DispenseType_HUB";

-- View: DATAWAREHOUSE.EGFrame
-- Columns: 15
create or replace view "EGFrame"(
	ID,
	"OrderId",
	"_Customer",
	"AMeas",
	"BMeas",
	"EDMeas",
	"DBLMeas",
	"Temple",
	"FrameEdgeTypeId",
	"RetailPrice",
	"FrameEdgeType_FrameEdgeType",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "OrderId" "OrderId", "_Customer" "_Customer", "AMeas" "AMeas", "BMeas" "BMeas", "EDMeas" "EDMeas", "DBLMeas" "DBLMeas", "Temple" "Temple", "FrameEdgeTypeId" "FrameEdgeTypeId", "RetailPrice" "RetailPrice", "FrameEdgeType_FrameEdgeType" "FrameEdgeType_FrameEdgeType", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."EGFrame_HUB";

-- View: DATAWAREHOUSE.EGOrder
-- Columns: 17
create or replace view "EGOrder"(
	ID,
	"OrderId",
	"_Customer",
	"IsUncut",
	"DispenseTypeId",
	"DispenseNote",
	"IsMakeFrame",
	"IsMakeRLens",
	"IsMakeLLens",
	"IsMakeExtra",
	"IsSafety",
	"IsManualLabOrder",
	"DispenseType_DispenseType",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "OrderId" "OrderId", "_Customer" "_Customer", "IsUncut" "IsUncut", "DispenseTypeId" "DispenseTypeId", "DispenseNote" "DispenseNote", "IsMakeFrame" "IsMakeFrame", "IsMakeRLens" "IsMakeRLens", "IsMakeLLens" "IsMakeLLens", "IsMakeExtra" "IsMakeExtra", "IsSafety" "IsSafety", "IsManualLabOrder" "IsManualLabOrder", "DispenseType_DispenseType" "DispenseType_DispenseType", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."EGOrder_HUB";

-- View: DATAWAREHOUSE.EGRX
-- Columns: 49
create or replace view EGRX(
	ID,
	"EGRXId",
	"_Customer",
	"OrderId",
	"Eye",
	"ItemId",
	FPD,
	NPD,
	"Sphere",
	"Cylinder",
	"Axis",
	"Prism1",
	"PrismDir1",
	"Dir1",
	"Prism2",
	"PrismDir2",
	"Dir2",
	"ResultPrism",
	"ResultAngle",
	"Base",
	"OCHeight",
	"AddPower1",
	"AddPower2",
	"SegHeight",
	"Thick",
	"IsBalance",
	"IsSlabOff",
	"DateTime",
	"DoctorEmployeeId",
	"PatientExamId",
	"OutsideDoctorId",
	"EGRXTypeId",
	"FrameWrap",
	"Vertex",
	"PantoscopicTilt",
	"EGRXKey",
	"NonFormularyLensDescription",
	"NonFormularyLensPrice",
	"StyleRetailPrice",
	"ColorRetailPrice",
	"Item_Item",
	"Doctor_Employee",
	"Order_Order",
	"EGRXType_EGRXType",
	DELETED_FLAG,
	DELETED_DATETIME,
	"OutsideDoctor_OutsideDoctor",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "EGRXId" "EGRXId", "_Customer" "_Customer", "OrderId" "OrderId", "Eye" "Eye", "ItemId" "ItemId", "FPD" "FPD", "NPD" "NPD", "Sphere" "Sphere", "Cylinder" "Cylinder", "Axis" "Axis", "Prism1" "Prism1", "PrismDir1" "PrismDir1", "Dir1" "Dir1", "Prism2" "Prism2", "PrismDir2" "PrismDir2", "Dir2" "Dir2", "ResultPrism" "ResultPrism", "ResultAngle" "ResultAngle", "Base" "Base", "OCHeight" "OCHeight", "AddPower1" "AddPower1", "AddPower2" "AddPower2", "SegHeight" "SegHeight", "Thick" "Thick", "IsBalance" "IsBalance", "IsSlabOff" "IsSlabOff", "DateTime" "DateTime", "DoctorEmployeeId" "DoctorEmployeeId", "PatientExamId" "PatientExamId", "OutsideDoctorId" "OutsideDoctorId", "EGRXTypeId" "EGRXTypeId", "FrameWrap" "FrameWrap", "Vertex" "Vertex", "PantoscopicTilt" "PantoscopicTilt", "EGRXKey" "EGRXKey", "NonFormularyLensDescription" "NonFormularyLensDescription", "NonFormularyLensPrice" "NonFormularyLensPrice", "StyleRetailPrice" "StyleRetailPrice", "ColorRetailPrice" "ColorRetailPrice", "Item_Item" "Item_Item", "Doctor_Employee" "Doctor_Employee", "Order_Order" "Order_Order", "EGRXType_EGRXType" "EGRXType_EGRXType", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "OutsideDoctor_OutsideDoctor" "OutsideDoctor_OutsideDoctor", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."EGRX_HUB";

-- View: DATAWAREHOUSE.EGRXType
-- Columns: 8
create or replace view "EGRXType"(
	ID,
	"EGRXTypeId",
	"_Customer",
	"Value",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "EGRXTypeId" "EGRXTypeId", "_Customer" "_Customer", "Value" "Value", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."EGRXType_HUB";

-- View: DATAWAREHOUSE.Eligibility
-- Columns: 14
create or replace view "Eligibility"(
	ID,
	"EligibilityId",
	"_Customer",
	"CarrierKey",
	"PlanId",
	"AuthorizationNumber",
	"AuthorizationDateTime",
	"AuthorizationExpireDate",
	"PlanPlan",
	"Carrier_Carrier",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "EligibilityId" "EligibilityId", "_Customer" "_Customer", "CarrierKey" "CarrierKey", "PlanId" "PlanId", "AuthorizationNumber" "AuthorizationNumber", "AuthorizationDateTime" "AuthorizationDateTime", "AuthorizationExpireDate" "AuthorizationExpireDate", "PlanPlan" "PlanPlan", "Carrier_Carrier" "Carrier_Carrier", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Eligibility_HUB";

-- View: DATAWAREHOUSE.Employee
-- Columns: 19
create or replace view "Employee"(
	ID,
	"EmployeeId",
	"_Customer",
	"CompanyInfoId",
	"LastName",
	"FirstName",
	"EmployeeTypeId",
	"CompanyInfo_CompanyInfo",
	"EmployeeType_EmployeeType",
	"IsActive",
	"IsAllowedToBeScheduled",
	"UserId",
	"User_User",
	DELETED_FLAG,
	DELETED_DATETIME,
	NPI,
	"FullName",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "EmployeeId" "EmployeeId", "_Customer" "_Customer", "CompanyInfoId" "CompanyInfoId", "LastName" "LastName", "FirstName" "FirstName", "EmployeeTypeId" "EmployeeTypeId", "CompanyInfo_CompanyInfo" "CompanyInfo_CompanyInfo", "EmployeeType_EmployeeType" "EmployeeType_EmployeeType", "IsActive" "IsActive", "IsAllowedToBeScheduled" "IsAllowedToBeScheduled", "UserId" "UserId", "User_User" "User_User", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "NPI" "NPI", "FullName" "FullName", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Employee_HUB";

-- View: DATAWAREHOUSE.EmployeeType
-- Columns: 7
create or replace view "EmployeeType"(
	ID,
	"EmployeeTypeId",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "EmployeeTypeId" "EmployeeTypeId", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."EmployeeType_HUB";

-- View: DATAWAREHOUSE.EraAdjustment
-- Columns: 12
create or replace view "EraAdjustment"(
	ID,
	"EraAdjustmentId",
	"_Customer",
	"GroupCode",
	"ReasonCode",
	"EraLineItemId",
	"Amount",
	DELETED_FLAG,
	DELETED_DATETIME,
	"EraLineItem_EraLineItem",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "EraAdjustmentId" "EraAdjustmentId", "_Customer" "_Customer", "GroupCode" "GroupCode", "ReasonCode" "ReasonCode", "EraLineItemId" "EraLineItemId", "Amount" "Amount", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "EraLineItem_EraLineItem" "EraLineItem_EraLineItem", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."EraAdjustment_HUB";

-- View: DATAWAREHOUSE.EraClaim
-- Columns: 11
create or replace view "EraClaim"(
	ID,
	"EraClaimId",
	"_Customer",
	"BillingClaimId",
	"EraRemittanceId",
	DELETED_FLAG,
	DELETED_DATETIME,
	"BillingClaim_BillingClaim",
	"EraRemittance_EraRemittance",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "EraClaimId" "EraClaimId", "_Customer" "_Customer", "BillingClaimId" "BillingClaimId", "EraRemittanceId" "EraRemittanceId", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "BillingClaim_BillingClaim" "BillingClaim_BillingClaim", "EraRemittance_EraRemittance" "EraRemittance_EraRemittance", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."EraClaim_HUB";

-- View: DATAWAREHOUSE.EraLineItem
-- Columns: 11
create or replace view "EraLineItem"(
	ID,
	"EraLineItemId",
	"_Customer",
	"ProcedureCode",
	"EraClaimId",
	"ParentEraLineItemId",
	DELETED_FLAG,
	DELETED_DATETIME,
	"EraClaim_EraClaim",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "EraLineItemId" "EraLineItemId", "_Customer" "_Customer", "ProcedureCode" "ProcedureCode", "EraClaimId" "EraClaimId", "ParentEraLineItemId" "ParentEraLineItemId", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "EraClaim_EraClaim" "EraClaim_EraClaim", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."EraLineItem_HUB";

-- View: DATAWAREHOUSE.EraRemittance
-- Columns: 9
create or replace view "EraRemittance"(
	ID,
	"EraRemittanceId",
	"_Customer",
	"BillingPaymentId",
	DELETED_FLAG,
	DELETED_DATETIME,
	"BillingPayment_BillingPayment",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "EraRemittanceId" "EraRemittanceId", "_Customer" "_Customer", "BillingPaymentId" "BillingPaymentId", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "BillingPayment_BillingPayment" "BillingPayment_BillingPayment", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."EraRemittance_HUB";

-- View: DATAWAREHOUSE.FrameCollection
-- Columns: 12
create or replace view "FrameCollection"(
	ID,
	"FrameCollectionId",
	"_Customer",
	"Description",
	"VendorId",
	"ManufacturerId",
	"Manufacturer_Manufacturer",
	"Vendor_Vendor",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "FrameCollectionId" "FrameCollectionId", "_Customer" "_Customer", "Description" "Description", "VendorId" "VendorId", "ManufacturerId" "ManufacturerId", "Manufacturer_Manufacturer" "Manufacturer_Manufacturer", "Vendor_Vendor" "Vendor_Vendor", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."FrameCollection_HUB";

-- View: DATAWAREHOUSE.FrameEdgeType
-- Columns: 9
create or replace view "FrameEdgeType"(
	ID,
	"FrameEdgeTypeId",
	"_Customer",
	"Description",
	"IsGroovedRimless",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "FrameEdgeTypeId" "FrameEdgeTypeId", "_Customer" "_Customer", "Description" "Description", "IsGroovedRimless" "IsGroovedRimless", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."FrameEdgeType_HUB";

-- View: DATAWAREHOUSE.FrameStyle
-- Columns: 12
create or replace view "FrameStyle"(
	ID,
	"FrameStyleId",
	"_Customer",
	"Model",
	"Description",
	"FrameCollectionId",
	"IsMedicaid",
	"FrameCollection_FrameCollection",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "FrameStyleId" "FrameStyleId", "_Customer" "_Customer", "Model" "Model", "Description" "Description", "FrameCollectionId" "FrameCollectionId", "IsMedicaid" "IsMedicaid", "FrameCollection_FrameCollection" "FrameCollection_FrameCollection", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."FrameStyle_HUB";

-- View: DATAWAREHOUSE.ICare_EnabledCompany
-- Columns: 13
create or replace view "ICare_EnabledCompany"(
	ID,
	"ICare_EnabledCompanyId",
	"_Customer",
	"Name",
	"OfficeKey",
	"OfficeKeyUid",
	DELETED_FLAG,
	DELETED_DATETIME,
	"LastOperation",
	"LastOperationDateTime",
	"Office_Office",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ICare_EnabledCompanyId" "ICare_EnabledCompanyId", "_Customer" "_Customer", "Name" "Name", "OfficeKey" "OfficeKey", "OfficeKeyUid" "OfficeKeyUid", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "LastOperation" "LastOperation", "LastOperationDateTime" "LastOperationDateTime", "Office_Office" "Office_Office", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."ICare_EnabledCompany_HUB";

-- View: DATAWAREHOUSE.InsuranceProcedureCode
-- Columns: 11
create or replace view "InsuranceProcedureCode"(
	ID,
	"InsuranceProcedureCodeId",
	"_Customer",
	"Code",
	"ItemId",
	DELETED_FLAG,
	DELETED_DATETIME,
	"Item_Item",
	"ItemGroupId",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "InsuranceProcedureCodeId" "InsuranceProcedureCodeId", "_Customer" "_Customer", "Code" "Code", "ItemId" "ItemId", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "Item_Item" "Item_Item", "ItemGroupId" "ItemGroupId", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."InsuranceProcedureCode_HUB";

-- View: DATAWAREHOUSE.InvoiceDetail
-- Columns: 40
create or replace view "InvoiceDetail"(
	ID,
	"_Customer",
	"InvoiceDetailId",
	"InvoiceSummaryId",
	"LineNumber",
	"OrderId",
	"ItemId",
	"Quantity",
	"Price",
	"DiscountTypeId",
	"Tax",
	"Discount",
	"LineDiscount",
	"PackageDiscount",
	"Amount",
	"CouponNumber",
	"IsPreAppointment",
	"GiftCardNumber",
	"IsLensItem",
	"IsCalculateLineDiscountByPercent",
	"StockOrderNumber",
	"UnitCost",
	"ValuationCost",
	"InvoiceDetailKey",
	"IsBillToInsurance",
	"TaxedPriceType",
	"IsAddToInventory",
	"PromotionDiscount",
	"PromotionId",
	"ItemType_ItemType",
	"Item_Item",
	"DiscountType_DiscountType",
	"Order_Order",
	"InvoiceSummary_InvoiceSummary",
	"ItemTypeId",
	DELETED_FLAG,
	DELETED_DATETIME,
	"LastOperationDateTime",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "_Customer" "_Customer", "InvoiceDetailId" "InvoiceDetailId", "InvoiceSummaryId" "InvoiceSummaryId", "LineNumber" "LineNumber", "OrderId" "OrderId", "ItemId" "ItemId", "Quantity" "Quantity", "Price" "Price", "DiscountTypeId" "DiscountTypeId", "Tax" "Tax", "Discount" "Discount", "LineDiscount" "LineDiscount", "PackageDiscount" "PackageDiscount", "Amount" "Amount", "CouponNumber" "CouponNumber", "IsPreAppointment" "IsPreAppointment", "GiftCardNumber" "GiftCardNumber", "IsLensItem" "IsLensItem", "IsCalculateLineDiscountByPercent" "IsCalculateLineDiscountByPercent", "StockOrderNumber" "StockOrderNumber", "UnitCost" "UnitCost", "ValuationCost" "ValuationCost", "InvoiceDetailKey" "InvoiceDetailKey", "IsBillToInsurance" "IsBillToInsurance", "TaxedPriceType" "TaxedPriceType", "IsAddToInventory" "IsAddToInventory", "PromotionDiscount" "PromotionDiscount", "PromotionId" "PromotionId", "ItemType_ItemType" "ItemType_ItemType", "Item_Item" "Item_Item", "DiscountType_DiscountType" "DiscountType_DiscountType", "Order_Order" "Order_Order", "InvoiceSummary_InvoiceSummary" "InvoiceSummary_InvoiceSummary", "ItemTypeId" "ItemTypeId", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "LastOperationDateTime" "LastOperationDateTime", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."InvoiceDetail_HUB";

-- View: DATAWAREHOUSE.InvoiceInsuranceDetail
-- Columns: 16
create or replace view "InvoiceInsuranceDetail"(
	ID,
	"InvoiceInsuranceDetailId",
	"_Customer",
	"InvoiceDetailId",
	"Allowance",
	"Copay",
	"Receivable",
	"InsuranceDiscount",
	"IsPrimary",
	"OrderInsuranceId",
	"OrderInsurance_OrderInsurance",
	"InvoiceDetail_InvoiceDetail",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "InvoiceInsuranceDetailId" "InvoiceInsuranceDetailId", "_Customer" "_Customer", "InvoiceDetailId" "InvoiceDetailId", "Allowance" "Allowance", "Copay" "Copay", "Receivable" "Receivable", "InsuranceDiscount" "InsuranceDiscount", "IsPrimary" "IsPrimary", "OrderInsuranceId" "OrderInsuranceId", "OrderInsurance_OrderInsurance" "OrderInsurance_OrderInsurance", "InvoiceDetail_InvoiceDetail" "InvoiceDetail_InvoiceDetail", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."InvoiceInsuranceDetail_HUB";

-- View: DATAWAREHOUSE.InvoiceSummary
-- Columns: 17
create or replace view "InvoiceSummary"(
	ID,
	"InvoiceSummaryId",
	"_Customer",
	"PosTransactionId",
	"OrderId",
	"DoctorEmployeeId",
	"EmployeeId",
	"Doctor_Employee",
	"Employee_Employee",
	"Order_Order",
	"OriginalDoctorEmployeeId",
	"OriginalDoctor_Employee",
	DELETED_FLAG,
	DELETED_DATETIME,
	"RefundTypeId",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "InvoiceSummaryId" "InvoiceSummaryId", "_Customer" "_Customer", "PosTransactionId" "PosTransactionId", "OrderId" "OrderId", "DoctorEmployeeId" "DoctorEmployeeId", "EmployeeId" "EmployeeId", "Doctor_Employee" "Doctor_Employee", "Employee_Employee" "Employee_Employee", "Order_Order" "Order_Order", "OriginalDoctorEmployeeId" "OriginalDoctorEmployeeId", "OriginalDoctor_Employee" "OriginalDoctor_Employee", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RefundTypeId" "RefundTypeId", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."InvoiceSummary_HUB";

-- View: DATAWAREHOUSE.Item
-- Columns: 15
create or replace view "Item"(
	ID,
	"ItemId",
	"_Customer",
	"ItemTypeId",
	"Number",
	"Name",
	"UPCCode",
	"Group",
	"ItemKey",
	"ItemType_ItemType",
	DELETED_FLAG,
	DELETED_DATETIME,
	"CPTCode",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ItemId" "ItemId", "_Customer" "_Customer", "ItemTypeId" "ItemTypeId", "Number" "Number", "Name" "Name", "UPCCode" "UPCCode", "Group" "Group", "ItemKey" "ItemKey", "ItemType_ItemType" "ItemType_ItemType", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "CPTCode" "CPTCode", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Item_HUB";

-- View: DATAWAREHOUSE.ItemCL
-- Columns: 9
create or replace view "ItemCL"(
	ID,
	"ItemId",
	"_Customer",
	"CLStyleId",
	"CLStyle_CLStyle",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ItemId" "ItemId", "_Customer" "_Customer", "CLStyleId" "CLStyleId", "CLStyle_CLStyle" "CLStyle_CLStyle", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."ItemCL_HUB";

-- View: DATAWAREHOUSE.ItemCoat
-- Columns: 8
create or replace view "ItemCoat"(
	ID,
	"ItemId",
	"_Customer",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ItemId" "ItemId", "_Customer" "_Customer", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."ItemCoat_HUB";

-- View: DATAWAREHOUSE.ItemCompany
-- Columns: 12
create or replace view "ItemCompany"(
	ID,
	"ItemCompanyId",
	"_Customer",
	"ItemId",
	"CompanyInfoId",
	"Name",
	"Item_Item",
	"CompanyInfo_CompanyInfo",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ItemCompanyId" "ItemCompanyId", "_Customer" "_Customer", "ItemId" "ItemId", "CompanyInfoId" "CompanyInfoId", "Name" "Name", "Item_Item" "Item_Item", "CompanyInfo_CompanyInfo" "CompanyInfo_CompanyInfo", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."ItemCompany_HUB";

-- View: DATAWAREHOUSE.ItemEGColor
-- Columns: 9
create or replace view "ItemEGColor"(
	ID,
	"ItemId",
	"_Customer",
	"Description",
	"Item_Item",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ItemId" "ItemId", "_Customer" "_Customer", "Description" "Description", "Item_Item" "Item_Item", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."ItemEGColor_HUB";

-- View: DATAWAREHOUSE.ItemEGLens
-- Columns: 18
create or replace view "ItemEGLens"(
	ID,
	"ItemId",
	"_Customer",
	"ItemEGTypeId",
	"ItemEGMaterialId",
	"ItemEGStyleId",
	"ItemEGColorId",
	"ItemCoatId",
	"Item_Item",
	"ItemEGType_ItemEGType",
	"ItemEGMaterial_ItemEGMaterial",
	"ItemEGStyle_ItemEGStyle",
	"ItemEGColor_ItemEGColor",
	"ItemCoat_ItemCoat",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ItemId" "ItemId", "_Customer" "_Customer", "ItemEGTypeId" "ItemEGTypeId", "ItemEGMaterialId" "ItemEGMaterialId", "ItemEGStyleId" "ItemEGStyleId", "ItemEGColorId" "ItemEGColorId", "ItemCoatId" "ItemCoatId", "Item_Item" "Item_Item", "ItemEGType_ItemEGType" "ItemEGType_ItemEGType", "ItemEGMaterial_ItemEGMaterial" "ItemEGMaterial_ItemEGMaterial", "ItemEGStyle_ItemEGStyle" "ItemEGStyle_ItemEGStyle", "ItemEGColor_ItemEGColor" "ItemEGColor_ItemEGColor", "ItemCoat_ItemCoat" "ItemCoat_ItemCoat", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."ItemEGLens_HUB";

-- View: DATAWAREHOUSE.ItemEGMaterial
-- Columns: 8
create or replace view "ItemEGMaterial"(
	ID,
	"ItemId",
	"_Customer",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ItemId" "ItemId", "_Customer" "_Customer", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."ItemEGMaterial_HUB";

-- View: DATAWAREHOUSE.ItemEGStyle
-- Columns: 8
create or replace view "ItemEGStyle"(
	ID,
	"ItemId",
	"_Customer",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ItemId" "ItemId", "_Customer" "_Customer", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."ItemEGStyle_HUB";

-- View: DATAWAREHOUSE.ItemEGType
-- Columns: 8
create or replace view "ItemEGType"(
	ID,
	"ItemId",
	"_Customer",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ItemId" "ItemId", "_Customer" "_Customer", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."ItemEGType_HUB";

-- View: DATAWAREHOUSE.ItemExam
-- Columns: 9
create or replace view "ItemExam"(
	ID,
	"ItemId",
	"_Customer",
	"IsFittingFee",
	"IsProcedure",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ItemId" "ItemId", "_Customer" "_Customer", "IsFittingFee" "IsFittingFee", "IsProcedure" "IsProcedure", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."ItemExam_HUB";

-- View: DATAWAREHOUSE.ItemFrame
-- Columns: 11
create or replace view "ItemFrame"(
	ID,
	"ItemId",
	"_Customer",
	"FrameStyleId",
	"Color",
	"Eye",
	"FrameStyle_FrameStyle",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ItemId" "ItemId", "_Customer" "_Customer", "FrameStyleId" "FrameStyleId", "Color" "Color", "Eye" "Eye", "FrameStyle_FrameStyle" "FrameStyle_FrameStyle", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."ItemFrame_HUB";

-- View: DATAWAREHOUSE.ItemType
-- Columns: 9
create or replace view "ItemType"(
	ID,
	"ItemTypeId",
	"_Customer",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	"ItemTypeGrouping",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ItemTypeId" "ItemTypeId", "_Customer" "_Customer", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "ItemTypeGrouping" "ItemTypeGrouping", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."ItemType_HUB";

-- View: DATAWAREHOUSE.Lab
-- Columns: 11
create or replace view "Lab"(
	ID,
	"LabId",
	"_Customer",
	"Name",
	"AddressId",
	"Address_Address",
	"OmicsVersion",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "LabId" "LabId", "_Customer" "_Customer", "Name" "Name", "AddressId" "AddressId", "Address_Address" "Address_Address", "OmicsVersion" "OmicsVersion", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Lab_HUB";

-- View: DATAWAREHOUSE.Manufacturer
-- Columns: 8
create or replace view "Manufacturer"(
	ID,
	"ManufacturerId",
	"_Customer",
	"Name",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ManufacturerId" "ManufacturerId", "_Customer" "_Customer", "Name" "Name", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Manufacturer_HUB";

-- View: DATAWAREHOUSE.MiscPaymentReason
-- Columns: 10
create or replace view "MiscPaymentReason"(
	ID,
	"MiscPaymentReasonId",
	"CompanyInfoId",
	"_Customer",
	"Description",
	"CustomerReferenceNumber",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "MiscPaymentReasonId" "MiscPaymentReasonId", "CompanyInfoId" "CompanyInfoId", "_Customer" "_Customer", "Description" "Description", "CustomerReferenceNumber" "CustomerReferenceNumber", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."MiscPaymentReason_HUB";

-- View: DATAWAREHOUSE.Note
-- Columns: 10
create or replace view "Note"(
	ID,
	"NoteId",
	"_Customer",
	"Detail",
	"EntityTypeId",
	"EntityId",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "NoteId" "NoteId", "_Customer" "_Customer", "Detail" "Detail", "EntityTypeId" "EntityTypeId", "EntityId" "EntityId", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Note_HUB";

-- View: DATAWAREHOUSE.Office
-- Columns: 26
create or replace view "Office"(
	ID,
	"OfficeKey",
	"_Customer",
	"Name",
	"OfficeTypeId",
	"RegionId",
	"IsLive",
	"CompanyInfoId",
	"IsSingleLocation",
	"CompanyInfo_CompanyInfo",
	"Type_OfficeType",
	"Region_Region",
	"DiscountThreshold",
	"OfficeId",
	DELETED_FLAG,
	DELETED_DATETIME,
	"AddressId",
	"PhoneNumber",
	"TimeZone",
	"OfficeKeyUid",
	"Address_Address",
	"IsUseDST",
	"IsPreAppointmentSupported",
	"IsActive",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "OfficeKey" "OfficeKey", "_Customer" "_Customer", "Name" "Name", "OfficeTypeId" "OfficeTypeId", "RegionId" "RegionId", "IsLive" "IsLive", "CompanyInfoId" "CompanyInfoId", "IsSingleLocation" "IsSingleLocation", "CompanyInfo_CompanyInfo" "CompanyInfo_CompanyInfo", "Type_OfficeType" "Type_OfficeType", "Region_Region" "Region_Region", "DiscountThreshold" "DiscountThreshold", "OfficeId" "OfficeId", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "AddressId" "AddressId", "PhoneNumber" "PhoneNumber", "TimeZone" "TimeZone", "OfficeKeyUid" "OfficeKeyUid", "Address_Address" "Address_Address", "IsUseDST" "IsUseDST", "IsPreAppointmentSupported" "IsPreAppointmentSupported", "IsActive" "IsActive", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Office_HUB";

-- View: DATAWAREHOUSE.OfficeEmployee
-- Columns: 11
create or replace view "OfficeEmployee"(
	ID,
	"OfficeKey",
	"EmployeeId",
	"_Customer",
	"OfficeEmployeeId",
	"Office_Office",
	"Employee_Employee",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "OfficeKey" "OfficeKey", "EmployeeId" "EmployeeId", "_Customer" "_Customer", "OfficeEmployeeId" "OfficeEmployeeId", "Office_Office" "Office_Office", "Employee_Employee" "Employee_Employee", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."OfficeEmployee_HUB";

-- View: DATAWAREHOUSE.OfficeHours
-- Columns: 14
create or replace view "OfficeHours"(
	ID,
	"OfficeHoursId",
	"_Customer",
	"OfficeKey",
	"Day",
	"OpenFrom",
	"OpenTo",
	DELETED_FLAG,
	DELETED_DATETIME,
	"LastOperation",
	"LastOperationDateTime",
	"Office_Office",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "OfficeHoursId" "OfficeHoursId", "_Customer" "_Customer", "OfficeKey" "OfficeKey", "Day" "Day", "OpenFrom" "OpenFrom", "OpenTo" "OpenTo", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "LastOperation" "LastOperation", "LastOperationDateTime" "LastOperationDateTime", "Office_Office" "Office_Office", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."OfficeHours_HUB";

-- View: DATAWAREHOUSE.OfficeType
-- Columns: 8
create or replace view "OfficeType"(
	ID,
	"OfficeTypeId",
	"_Customer",
	"Value",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "OfficeTypeId" "OfficeTypeId", "_Customer" "_Customer", "Value" "Value", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."OfficeType_HUB";

-- View: DATAWAREHOUSE.Order
-- Columns: 45
create or replace view "Order"(
	ID,
	"OrderId",
	"_Customer",
	"OfficeKey",
	"ShipTo",
	"DateTime",
	"PatientId",
	"OrderTypeKey",
	"EmployeeId",
	"IsRemake",
	"LabInstructions",
	"Patient_Patient",
	"Employee_Employee",
	"Office_Office",
	"OrderType_OrderType",
	"RemakeTypeId",
	"RemadeOrder",
	"LabJobNumber",
	"CompanyInfoId",
	"CompanyInfo_CompanyInfo",
	"RemakeType_RemakeType",
	DELETED_FLAG,
	DELETED_DATETIME,
	"RXDoctorEmployeeId",
	"RXOutsideDoctorId",
	"RXDoctorEmployee_Employee",
	"RXOutsideDoctor_OutsideDoctor",
	"StatusCode",
	"IsMixed",
	"IsFullyPaid",
	"DateTimeFullyPaid",
	"OriginalOrderId",
	"OriginalDoctorEmployeeId",
	"OriginalEmployeeId",
	"OriginalOfficeKey",
	"OriginalOrder_Order",
	"OriginalDoctorEmployee_Employee",
	"OriginalEmployee_Employee",
	"OriginalOffice_Office",
	"DateTimeFirstPayment",
	"IsCollectionsItemizable",
	"StatusCodeDescription",
	RUNNO_INSERT,
	RUNNO_UPDATE,
	"StatusCodeChangedDateTime"
) as  SELECT *  FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Order_HUB";

-- View: DATAWAREHOUSE.OrderExamDetail
-- Columns: 23
create or replace view "OrderExamDetail"(
	ID,
	"OrderExamDetailId",
	"_Customer",
	"OrderId",
	"ItemId",
	"DiagnosisCodeId2",
	"DiagnosisCodeId3",
	"DiagnosisCodeId4",
	"DiagnosisCodeId5",
	"DiagnosisCodeId6",
	"DiagnosisCodeId7",
	"DiagnosisCodeId8",
	"DiagnosisCodeId9",
	"DiagnosisCodeId10",
	"DiagnosisCodeId11",
	"DiagnosisCodeId12",
	"Order_Order",
	"Item_Item",
	"DiagnosisCodeId1",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "OrderExamDetailId" "OrderExamDetailId", "_Customer" "_Customer", "OrderId" "OrderId", "ItemId" "ItemId", "DiagnosisCodeId2" "DiagnosisCodeId2", "DiagnosisCodeId3" "DiagnosisCodeId3", "DiagnosisCodeId4" "DiagnosisCodeId4", "DiagnosisCodeId5" "DiagnosisCodeId5", "DiagnosisCodeId6" "DiagnosisCodeId6", "DiagnosisCodeId7" "DiagnosisCodeId7", "DiagnosisCodeId8" "DiagnosisCodeId8", "DiagnosisCodeId9" "DiagnosisCodeId9", "DiagnosisCodeId10" "DiagnosisCodeId10", "DiagnosisCodeId11" "DiagnosisCodeId11", "DiagnosisCodeId12" "DiagnosisCodeId12", "Order_Order" "Order_Order", "Item_Item" "Item_Item", "DiagnosisCodeId1" "DiagnosisCodeId1", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."OrderExamDetail_HUB";

-- View: DATAWAREHOUSE.OrderInsurance
-- Columns: 13
create or replace view "OrderInsurance"(
	ID,
	"OrderInsuranceId",
	"_Customer",
	"CarrierName",
	"PlanName",
	"CarrierKey",
	"Carrier_Carrier",
	"OrderId",
	"Order_Order",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "OrderInsuranceId" "OrderInsuranceId", "_Customer" "_Customer", "CarrierName" "CarrierName", "PlanName" "PlanName", "CarrierKey" "CarrierKey", "Carrier_Carrier" "Carrier_Carrier", "OrderId" "OrderId", "Order_Order" "Order_Order", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."OrderInsurance_HUB";

-- View: DATAWAREHOUSE.OrderType
-- Columns: 8
create or replace view "OrderType"(
	ID,
	"OrderTypeKey",
	"_Customer",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "OrderTypeKey" "OrderTypeKey", "_Customer" "_Customer", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."OrderType_HUB";

-- View: DATAWAREHOUSE.OutsideDoctor
-- Columns: 13
create or replace view "OutsideDoctor"(
	ID,
	"OutsideDoctorId",
	"_Customer",
	"LastName",
	"FirstName",
	NPI,
	"CompanyInfoId",
	"IsActive",
	DELETED_FLAG,
	DELETED_DATETIME,
	"CompanyInfo_CompanyInfo",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "OutsideDoctorId" "OutsideDoctorId", "_Customer" "_Customer", "LastName" "LastName", "FirstName" "FirstName", "NPI" "NPI", "CompanyInfoId" "CompanyInfoId", "IsActive" "IsActive", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "CompanyInfo_CompanyInfo" "CompanyInfo_CompanyInfo", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."OutsideDoctor_HUB";

-- View: DATAWAREHOUSE.Patient
-- Columns: 30
create or replace view "Patient"(
	ID,
	"PatientId",
	"_Customer",
	"LastName",
	"FirstName",
	"BirthDate",
	"Sex",
	"MiddleInitial",
	"Email_01",
	"ResponsiblePatientId",
	"ReferralTypeId",
	"LastExamDate",
	"HomeOfficeKey",
	"IsAddressBad",
	"IsEmailBad",
	"IsInActive",
	"NickName",
	"ProviderEmployeeId",
	"IsPatient",
	"ReferralType_ReferralType",
	"ResponsiblePartyFullName",
	"CompanyInfoId",
	DELETED_FLAG,
	DELETED_DATETIME,
	"FullName",
	"PatientKey",
	RUNNO_INSERT,
	RUNNO_UPDATE,
	"IsDeceased",
	"IsBadPhoneNumber"
) as  SELECT *  FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Patient_HUB";

-- View: DATAWAREHOUSE.PatientAddress
-- Columns: 10
create or replace view "PatientAddress"(
	ID,
	"PatientId",
	"AddressId",
	"_Customer",
	"Patient_Patient",
	"Address_Address",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PatientId" "PatientId", "AddressId" "AddressId", "_Customer" "_Customer", "Patient_Patient" "Patient_Patient", "Address_Address" "Address_Address", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."PatientAddress_HUB";

-- View: DATAWAREHOUSE.PatientCommunicationEventType
-- Columns: 8
create or replace view "PatientCommunicationEventType"(
	ID,
	"PatientCommunicationEventTypeId",
	"_Customer",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PatientCommunicationEventTypeId" "PatientCommunicationEventTypeId", "_Customer" "_Customer", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."PatientCommunicationEventType_HUB";

-- View: DATAWAREHOUSE.PatientCommunicationMethod
-- Columns: 15
create or replace view "PatientCommunicationMethod"(
	ID,
	"PatientCommunicationMethodId",
	"_Customer",
	"PatientId",
	"PatientCommunicationEventTypeId",
	"IsCall",
	"IsMail",
	"IsText",
	"IsEmail",
	DELETED_FLAG,
	DELETED_DATETIME,
	"Patient_Patient",
	"PatientCommunicationEventType_PatientCommunicationEventType",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PatientCommunicationMethodId" "PatientCommunicationMethodId", "_Customer" "_Customer", "PatientId" "PatientId", "PatientCommunicationEventTypeId" "PatientCommunicationEventTypeId", "IsCall" "IsCall", "IsMail" "IsMail", "IsText" "IsText", "IsEmail" "IsEmail", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "Patient_Patient" "Patient_Patient", "PatientCommunicationEventType_PatientCommunicationEventType" "PatientCommunicationEventType_PatientCommunicationEventType", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."PatientCommunicationMethod_HUB";

-- View: DATAWAREHOUSE.PatientExam
-- Columns: 19
create or replace view "PatientExam"(
	ID,
	"PatientExamId",
	"_Customer",
	"PatientId",
	"DateTime",
	"ExpireDateTime",
	"DoctorEmployeeId",
	"EmployeeId",
	"CreatedDateTime",
	"OfficeKey",
	"Patient_Patient",
	"Doctor_Employee",
	"Employee_Employee",
	"Office_Office",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE,
	"ExamRXType"
) as  SELECT *  FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."PatientExam_HUB";

-- View: DATAWAREHOUSE.PatientExamDetail
-- Columns: 30
create or replace view "PatientExamDetail"(
	ID,
	"PatientExamDetailId",
	"_Customer",
	"PatientExamId",
	"Eye",
	"Sphere",
	"Cylinder",
	"Axis",
	"AddPower1",
	"Base",
	"Diameter",
	"ItemId",
	"CLPowerId",
	"OpticalZone",
	"PerCurve1",
	"PerCurve2",
	"SecCurve1",
	"SecCurve2",
	"CenterThickness",
	"Blend",
	"SupplierId",
	"Item_Item",
	"CLPower_CLPower",
	"Supplier_Supplier",
	"PatientExam_PatientExam",
	"CLStockItemId",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PatientExamDetailId" "PatientExamDetailId", "_Customer" "_Customer", "PatientExamId" "PatientExamId", "Eye" "Eye", "Sphere" "Sphere", "Cylinder" "Cylinder", "Axis" "Axis", "AddPower1" "AddPower1", "Base" "Base", "Diameter" "Diameter", "ItemId" "ItemId", "CLPowerId" "CLPowerId", "OpticalZone" "OpticalZone", "PerCurve1" "PerCurve1", "PerCurve2" "PerCurve2", "SecCurve1" "SecCurve1", "SecCurve2" "SecCurve2", "CenterThickness" "CenterThickness", "Blend" "Blend", "SupplierId" "SupplierId", "Item_Item" "Item_Item", "CLPower_CLPower" "CLPower_CLPower", "Supplier_Supplier" "Supplier_Supplier", "PatientExam_PatientExam" "PatientExam_PatientExam", "CLStockItemId" "CLStockItemId", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."PatientExamDetail_HUB";

-- View: DATAWAREHOUSE.PatientInsurance
-- Columns: 11
create or replace view "PatientInsurance"(
	ID,
	"PatientInsuranceId",
	"_Customer",
	"PlanId",
	"PatientId",
	"Patient_Patient",
	DELETED_FLAG,
	DELETED_DATETIME,
	"Plan_Plan",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PatientInsuranceId" "PatientInsuranceId", "_Customer" "_Customer", "PlanId" "PlanId", "PatientId" "PatientId", "Patient_Patient" "Patient_Patient", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "Plan_Plan" "Plan_Plan", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."PatientInsurance_HUB";

-- View: DATAWAREHOUSE.PatientPhone
-- Columns: 11
create or replace view "PatientPhone"(
	ID,
	"PatientPhoneId",
	"_Customer",
	"PatientId",
	"PhoneId",
	"Patient_Patient",
	"Phone_Phone",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PatientPhoneId" "PatientPhoneId", "_Customer" "_Customer", "PatientId" "PatientId", "PhoneId" "PhoneId", "Patient_Patient" "Patient_Patient", "Phone_Phone" "Phone_Phone", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."PatientPhone_HUB";

-- View: DATAWAREHOUSE.PatientPhoneType
-- Columns: 8
create or replace view "PatientPhoneType"(
	ID,
	"PatientPhoneTypeId",
	"_Customer",
	"Value",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PatientPhoneTypeId" "PatientPhoneTypeId", "_Customer" "_Customer", "Value" "Value", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."PatientPhoneType_HUB";

-- View: DATAWAREHOUSE.PatientRecall
-- Columns: 13
create or replace view "PatientRecall"(
	ID,
	"PatientRecallId",
	"_Customer",
	"PatientId",
	"Date",
	"IsActive",
	"Patient_Patient",
	"PatientRecallTypeId",
	"PatientRecallType_PatientRecallType",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PatientRecallId" "PatientRecallId", "_Customer" "_Customer", "PatientId" "PatientId", "Date" "Date", "IsActive" "IsActive", "Patient_Patient" "Patient_Patient", "PatientRecallTypeId" "PatientRecallTypeId", "PatientRecallType_PatientRecallType" "PatientRecallType_PatientRecallType", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."PatientRecall_HUB";

-- View: DATAWAREHOUSE.PatientRecallType
-- Columns: 8
create or replace view "PatientRecallType"(
	ID,
	"PatientRecallTypeId",
	"_Customer",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PatientRecallTypeId" "PatientRecallTypeId", "_Customer" "_Customer", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."PatientRecallType_HUB";

-- View: DATAWAREHOUSE.PaymentType
-- Columns: 9
create or replace view "PaymentType"(
	ID,
	"PaymentTypeId",
	"_Customer",
	"Description",
	"IsPrivateLabel",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PaymentTypeId" "PaymentTypeId", "_Customer" "_Customer", "Description" "Description", "IsPrivateLabel" "IsPrivateLabel", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."PaymentType_HUB";

-- View: DATAWAREHOUSE.Phone
-- Columns: 12
create or replace view "Phone"(
	ID,
	"PhoneId",
	"_Customer",
	"Number",
	"PatientPhoneTypeId",
	"IsPrimary",
	"Extension",
	"PatientPhoneType_PatientPhoneType",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PhoneId" "PhoneId", "_Customer" "_Customer", "Number" "Number", "PatientPhoneTypeId" "PatientPhoneTypeId", "IsPrimary" "IsPrimary", "Extension" "Extension", "PatientPhoneType_PatientPhoneType" "PatientPhoneType_PatientPhoneType", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Phone_HUB";

-- View: DATAWAREHOUSE.Plan
-- Columns: 10
create or replace view "Plan"(
	ID,
	"PlanId",
	"_Customer",
	"Name",
	"CarrierCode",
	DELETED_FLAG,
	DELETED_DATETIME,
	"Carrier_Carrier",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PlanId" "PlanId", "_Customer" "_Customer", "Name" "Name", "CarrierCode" "CarrierCode", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "Carrier_Carrier" "Carrier_Carrier", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Plan_HUB";

-- View: DATAWAREHOUSE.PosMiscPayment
-- Columns: 13
create or replace view "PosMiscPayment"(
	ID,
	"PosMiscPaymentId",
	"_Customer",
	"MiscPaymentReasonId",
	"PosTransactionId",
	"OriginalOrderNumber",
	"OriginalStoreNumber",
	"PosTransaction_PosTransaction",
	"MiscPaymentReason_MiscPaymentReason",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PosMiscPaymentId" "PosMiscPaymentId", "_Customer" "_Customer", "MiscPaymentReasonId" "MiscPaymentReasonId", "PosTransactionId" "PosTransactionId", "OriginalOrderNumber" "OriginalOrderNumber", "OriginalStoreNumber" "OriginalStoreNumber", "PosTransaction_PosTransaction" "PosTransaction_PosTransaction", "MiscPaymentReason_MiscPaymentReason" "MiscPaymentReason_MiscPaymentReason", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."PosMiscPayment_HUB";

-- View: DATAWAREHOUSE.PosPayment
-- Columns: 11
create or replace view "PosPayment"(
	ID,
	"PosPaymentId",
	"_Customer",
	"PatientId",
	"Amount",
	"Patient_Patient",
	"DateTime",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PosPaymentId" "PosPaymentId", "_Customer" "_Customer", "PatientId" "PatientId", "Amount" "Amount", "Patient_Patient" "Patient_Patient", "DateTime" "DateTime", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."PosPayment_HUB";

-- View: DATAWAREHOUSE.PosPaymentDetail
-- Columns: 12
create or replace view "PosPaymentDetail"(
	ID,
	"PosPaymentDetailId",
	"_Customer",
	"PosPaymentId",
	"PaymentTypeId",
	"Amount",
	"PosPayment_PosPayment",
	"PaymentType_PaymentType",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PosPaymentDetailId" "PosPaymentDetailId", "_Customer" "_Customer", "PosPaymentId" "PosPaymentId", "PaymentTypeId" "PaymentTypeId", "Amount" "Amount", "PosPayment_PosPayment" "PosPayment_PosPayment", "PaymentType_PaymentType" "PaymentType_PaymentType", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."PosPaymentDetail_HUB";

-- View: DATAWAREHOUSE.PosTransaction
-- Columns: 25
create or replace view "PosTransaction"(
	ID,
	"PosTransactionId",
	"_Customer",
	"PosTransactionTypeId",
	"OrderId",
	"EmployeeId",
	"PatientId",
	"OfficeKey",
	"PosPaymentId",
	"Amount",
	"DateTime",
	"InvoiceSummaryId",
	"DayCloseId",
	"PosTransactionType_PosTransactionType",
	"Employee_Employee",
	"Patient_Patient",
	"PosPayment_PosPayment",
	"DayClose_DayClose",
	"Order_Order",
	"Office_Office",
	DELETED_FLAG,
	DELETED_DATETIME,
	"InvoiceSummary_InvoiceSummary",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PosTransactionId" "PosTransactionId", "_Customer" "_Customer", "PosTransactionTypeId" "PosTransactionTypeId", "OrderId" "OrderId", "EmployeeId" "EmployeeId", "PatientId" "PatientId", "OfficeKey" "OfficeKey", "PosPaymentId" "PosPaymentId", "Amount" "Amount", "DateTime" "DateTime", "InvoiceSummaryId" "InvoiceSummaryId", "DayCloseId" "DayCloseId", "PosTransactionType_PosTransactionType" "PosTransactionType_PosTransactionType", "Employee_Employee" "Employee_Employee", "Patient_Patient" "Patient_Patient", "PosPayment_PosPayment" "PosPayment_PosPayment", "DayClose_DayClose" "DayClose_DayClose", "Order_Order" "Order_Order", "Office_Office" "Office_Office", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "InvoiceSummary_InvoiceSummary" "InvoiceSummary_InvoiceSummary", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."PosTransaction_HUB";

-- View: DATAWAREHOUSE.PosTransactionType
-- Columns: 8
create or replace view "PosTransactionType"(
	ID,
	"PosTransactionTypeId",
	"_Customer",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "PosTransactionTypeId" "PosTransactionTypeId", "_Customer" "_Customer", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."PosTransactionType_HUB";

-- View: DATAWAREHOUSE.ReferralType
-- Columns: 8
create or replace view "ReferralType"(
	ID,
	"ReferralTypeId",
	"_Customer",
	"Value",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ReferralTypeId" "ReferralTypeId", "_Customer" "_Customer", "Value" "Value", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."ReferralType_HUB";

-- View: DATAWAREHOUSE.Region
-- Columns: 8
create or replace view "Region"(
	ID,
	"RegionId",
	"_Customer",
	"Value",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "RegionId" "RegionId", "_Customer" "_Customer", "Value" "Value", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Region_HUB";

-- View: DATAWAREHOUSE.RemakeType
-- Columns: 9
create or replace view "RemakeType"(
	ID,
	"CompanyInfoId",
	"RemakeTypeId",
	"_Customer",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "CompanyInfoId" "CompanyInfoId", "RemakeTypeId" "RemakeTypeId", "_Customer" "_Customer", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."RemakeType_HUB";

-- View: DATAWAREHOUSE.Role
-- Columns: 11
create or replace view "Role"(
	ID,
	"RoleId",
	"_Customer",
	"Name",
	"Description",
	"RoleTypeId",
	"CompanyInfoId",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "RoleId" "RoleId", "_Customer" "_Customer", "Name" "Name", "Description" "Description", "RoleTypeId" "RoleTypeId", "CompanyInfoId" "CompanyInfoId", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Role_HUB";

-- View: DATAWAREHOUSE.ScheduledAppointment
-- Columns: 30
create or replace view "ScheduledAppointment"(
	ID,
	"ScheduledAppointmentId",
	"_Customer",
	"ItemId",
	"EmployeeId",
	"OfficeId",
	"PatientInsuranceId",
	"Date",
	"StartTime",
	"IsCanceled",
	"PatientId",
	"IsDeleted",
	"Employee_Employee",
	"PatientInsurance_PatientInsurance",
	"Patient_Patient",
	"IsConfirmed",
	"Notes",
	"EndTime",
	"IsMessageLeft",
	"IsNoAnswer",
	"Status_appt_show_ind",
	"PatientMedicalInsuranceId",
	"DerivedStatus",
	"IsPatientAppointment",
	"IsNonPatientAppointment",
	"IsInsuranceAssigned",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ScheduledAppointmentId" "ScheduledAppointmentId", "_Customer" "_Customer", "ItemId" "ItemId", "EmployeeId" "EmployeeId", "OfficeId" "OfficeId", "PatientInsuranceId" "PatientInsuranceId", "Date" "Date", "StartTime" "StartTime", "IsCanceled" "IsCanceled", "PatientId" "PatientId", "IsDeleted" "IsDeleted", "Employee_Employee" "Employee_Employee", "PatientInsurance_PatientInsurance" "PatientInsurance_PatientInsurance", "Patient_Patient" "Patient_Patient", "IsConfirmed" "IsConfirmed", "Notes" "Notes", "EndTime" "EndTime", "IsMessageLeft" "IsMessageLeft", "IsNoAnswer" "IsNoAnswer", "Status_appt_show_ind" "Status_appt_show_ind", "PatientMedicalInsuranceId" "PatientMedicalInsuranceId", "DerivedStatus" "DerivedStatus", "IsPatientAppointment" "IsPatientAppointment", "IsNonPatientAppointment" "IsNonPatientAppointment", "IsInsuranceAssigned" "IsInsuranceAssigned", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."ScheduledAppointment_HUB";

-- View: DATAWAREHOUSE.ScheduledAppointmentNote
-- Columns: 10
create or replace view "ScheduledAppointmentNote"(
	ID,
	"NoteId",
	"_Customer",
	"Detail",
	"ScheduledAppointmentId",
	DELETED_FLAG,
	DELETED_DATETIME,
	"ScheduledAppointment_ScheduledAppointment",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "NoteId" "NoteId", "_Customer" "_Customer", "Detail" "Detail", "ScheduledAppointmentId" "ScheduledAppointmentId", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "ScheduledAppointment_ScheduledAppointment" "ScheduledAppointment_ScheduledAppointment", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."ScheduledAppointmentNote_HUB";

-- View: DATAWAREHOUSE.ScheduledAppointmentType
-- Columns: 13
create or replace view "ScheduledAppointmentType"(
	ID,
	"ScheduledAppointmentTypeId",
	"_Customer",
	"ItemId",
	"OfficeId",
	"Duration",
	"IsAvailableToSchedule",
	"Color",
	DELETED_FLAG,
	DELETED_DATETIME,
	"Item_Item",
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "ScheduledAppointmentTypeId" "ScheduledAppointmentTypeId", "_Customer" "_Customer", "ItemId" "ItemId", "OfficeId" "OfficeId", "Duration" "Duration", "IsAvailableToSchedule" "IsAvailableToSchedule", "Color" "Color", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "Item_Item" "Item_Item", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."ScheduledAppointmentType_HUB";

-- View: DATAWAREHOUSE.Supplier
-- Columns: 8
create or replace view "Supplier"(
	ID,
	"SupplierId",
	"_Customer",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "SupplierId" "SupplierId", "_Customer" "_Customer", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Supplier_HUB";

-- View: DATAWAREHOUSE.Time
-- Columns: 10
create or replace view "Time"(
	ID,
	TIME,
	HOUR12,
	HOUR24,
	MINUTE,
	AM_PM,
	HOUR12_DISPLAY,
	HOUR24_DISPLAY,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "TIME" "TIME", "HOUR12" "HOUR12", "HOUR24" "HOUR24", "MINUTE" "MINUTE", "AM_PM" "AM_PM", "HOUR12_DISPLAY" "HOUR12_DISPLAY", "HOUR24_DISPLAY" "HOUR24_DISPLAY", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Time_HUB";

-- View: DATAWAREHOUSE.User
-- Columns: 13
create or replace view "User"(
	ID,
	"UserId",
	"_Customer",
	"Name",
	"Email",
	"IsLockedOut",
	"CreationDateTime",
	"LastLoginDateTime",
	"CompanyInfoId",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "UserId" "UserId", "_Customer" "_Customer", "Name" "Name", "Email" "Email", "IsLockedOut" "IsLockedOut", "CreationDateTime" "CreationDateTime", "LastLoginDateTime" "LastLoginDateTime", "CompanyInfoId" "CompanyInfoId", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."User_HUB";

-- View: DATAWAREHOUSE.UserRole
-- Columns: 10
create or replace view "UserRole"(
	ID,
	"UserId",
	"RoleId",
	"_Customer",
	"User_User",
	"Role_Role",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "UserId" "UserId", "RoleId" "RoleId", "_Customer" "_Customer", "User_User" "User_User", "Role_Role" "Role_Role", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."UserRole_HUB";

-- View: DATAWAREHOUSE.Vendor
-- Columns: 9
create or replace view "Vendor"(
	ID,
	"VendorId",
	"_Customer",
	"AddressId",
	"Address_Address",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "VendorId" "VendorId", "_Customer" "_Customer", "AddressId" "AddressId", "Address_Address" "Address_Address", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."Vendor_HUB";

-- View: DATAWAREHOUSE.WorkflowActivity
-- Columns: 9
create or replace view "WorkflowActivity"(
	ID,
	"WorkflowActivityId",
	"_Customer",
	"WorkflowEventId",
	"WorkflowEvent_WorkflowEvent",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "WorkflowActivityId" "WorkflowActivityId", "_Customer" "_Customer", "WorkflowEventId" "WorkflowEventId", "WorkflowEvent_WorkflowEvent" "WorkflowEvent_WorkflowEvent", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."WorkflowActivity_HUB";

-- View: DATAWAREHOUSE.WorkflowEvent
-- Columns: 19
create or replace view "WorkflowEvent"(
	ID,
	"WorkflowEventId",
	"_Customer",
	"Name",
	"Note",
	"AssociatedEntityId",
	"WorkflowTypeId",
	"OrderId",
	"BillingClaimId",
	"AppointmentId",
	"Order_Order",
	"BillingClaim_BillingClaim",
	"WorkflowType_WorkflowType",
	"UserId",
	"User_User",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "WorkflowEventId" "WorkflowEventId", "_Customer" "_Customer", "Name" "Name", "Note" "Note", "AssociatedEntityId" "AssociatedEntityId", "WorkflowTypeId" "WorkflowTypeId", "OrderId" "OrderId", "BillingClaimId" "BillingClaimId", "AppointmentId" "AppointmentId", "Order_Order" "Order_Order", "BillingClaim_BillingClaim" "BillingClaim_BillingClaim", "WorkflowType_WorkflowType" "WorkflowType_WorkflowType", "UserId" "UserId", "User_User" "User_User", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."WorkflowEvent_HUB";

-- View: DATAWAREHOUSE.WorkflowType
-- Columns: 8
create or replace view "WorkflowType"(
	ID,
	"WorkflowTypeId",
	"_Customer",
	"Description",
	DELETED_FLAG,
	DELETED_DATETIME,
	RUNNO_INSERT,
	RUNNO_UPDATE
) as  SELECT "ID", "WorkflowTypeId" "WorkflowTypeId", "_Customer" "_Customer", "Description" "Description", "DELETED_FLAG" "DELETED_FLAG", "DELETED_DATETIME" "DELETED_DATETIME", "RUNNO_INSERT", "RUNNO_UPDATE" FROM "DEV_ANALYTICS"."DATAWAREHOUSE"."WorkflowType_HUB";


-- ============================================================
-- SCHEMA: DATAMART
-- ============================================================

-- View: DATAMART.ARMartFct_Billing
-- Columns: 28
create or replace view "ARMartFct_Billing"(
	"Source",
	"BalanceCategory",
	"_Customer",
	"CompanyInfoId",
	"OfficeNum",
	"OfficeName",
	"PatientId",
	"PatientFirstName",
	"PatientLastName",
	"PatientFullName",
	"OrderNum",
	"OrderStatus",
	"OrderStatusCodeDescription",
	"OrderDate",
	"OrderBalance",
	"OrderBalanceDaysOutstanding",
	"CarrierId",
	"CarrierName",
	"PlanId",
	"PlanName",
	"ClaimId",
	"ClaimStatus",
	"ClaimLatestNote",
	"ClaimServiceDate",
	"ClaimInsuranceBalance",
	"ClaimPatientBalance",
	"ClaimTotalBalance",
	"ClaimBalanceDaysOutstanding"
) as
WITH 
latest_bt AS (
    /* Latest transaction per LineItemId before tomorrow, excluding zero-balance */
    SELECT
        "BillingClaimLineItemId",
        "BillingClaimId",
        "OrderId",
        "_Customer",
        "DateTime",
        "InsuranceAR",
        "PatientAR"
    FROM "DATAMART"."ARMartFct_BillingTransaction"
    WHERE "DateTime" < DATEADD('day', 1, CURRENT_DATE)
      --AND ("InsuranceAR" + "PatientAR") <> 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY "_Customer","OrderId","BillingClaimId","BillingClaimLineItemId"   ORDER BY "BillingTransactionId" DESC
    ) = 1
),
claim_status AS (
    /* Distinct status list per order */
    SELECT
        li2."BillingClaimId",
        li2."_Customer",
        COALESCE(
            LISTAGG(
                DISTINCT IFF(li2."BillingClaimLineItem_CurrentState" = 'Timed Out',
                    'Billed-No Activity for 60 Days',
                    li2."BillingClaimLineItem_CurrentState"
                ), ', '
            ) WITHIN GROUP (
                ORDER BY IFF(li2."BillingClaimLineItem_CurrentState" = 'Timed Out',
                    'Billed-No Activity for 60 Days',
                    li2."BillingClaimLineItem_CurrentState"
                )
            ),
            ''
        ) AS "ClaimStatus"
    FROM "DATAMART"."ARMartFct_BillingTransaction" li2
    GROUP BY li2."BillingClaimId", li2."_Customer"
),
latest_note AS (
    /* Latest non-null note per claim */
    SELECT
        "AssociatedEntityId" AS "ClaimId",
        "Note"
    FROM "DATAMART"."ARMartDim_WorkflowEvent"
    WHERE "Note" IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY "AssociatedEntityId"
        ORDER BY "WorkflowEventId" DESC
    ) = 1
)

SELECT 
    'Billing'                                                                           AS "Source",
    CASE 
        WHEN SUM(b."PatientAR") <> 0 AND SUM(b."InsuranceAR") <> 0 THEN 'Both'
        WHEN SUM(b."PatientAR") <> 0 THEN 'Patient'
        ELSE 'Carrier'
    END                                                                                 AS "BalanceCategory",
    b."_Customer"                                                                       AS "_Customer",
    bc."CompanyInfoId"                                                                  AS "CompanyInfoId",
    bc."OfficeKey"                                                                      AS "OfficeNum",
    office."Name"                                                                       AS "OfficeName",
    bc."PatientId"                                                                      AS "PatientId",
    p."FirstName"                                                                       AS "PatientFirstName",
    p."LastName"                                                                        AS "PatientLastName",
    CONCAT_WS(' ',p."FirstName", p."LastName")                                          AS "PatientFullName",
    ord."OrderId"                                                                       AS "OrderNum",
    ord."StatusCode"                                                                    AS "OrderStatus",
    ord."StatusCode_Description" 														AS "OrderStatusCodeDescription",
    MAX(ord."DateTime")                                                                 AS "OrderDate",
    NULL                                                                                AS "OrderBalance",
    NULL                                                                                AS "OrderBalanceDaysOutstanding",
    bc."CarrierKey"                                                                     AS "CarrierId",
    c."Name"                                                                            AS "CarrierName",
    bc."PlanId"                                                                         AS "PlanId",
    pn."Name"                                                                           AS "PlanName",
    bc."BillingClaimId"                                                                 AS "ClaimId",
    MAX(cs."ClaimStatus")                                                               AS "ClaimStatus",
    MAX(note."Note")                                                                    AS "ClaimLatestNote",
    bc."ServiceDate"                                                                    AS "ClaimServiceDate",
    SUM(b."InsuranceAR")                                                                AS "ClaimInsuranceBalance",
    SUM(b."PatientAR")                                                                  AS "ClaimPatientBalance",
    SUM(b."InsuranceAR" + b."PatientAR")                                                AS "ClaimTotalBalance",
    CASE 
        WHEN SUM(b."InsuranceAR" + b."PatientAR") = 0 
            THEN DATEDIFF('day', MAX(ord."DateTime"), MAX(b."DateTime"))
        ELSE DATEDIFF('day', MAX(ord."DateTime"), CURRENT_TIMESTAMP)
    END                                                                                 AS "ClaimBalanceDaysOutstanding"
FROM latest_bt b
JOIN "DATAMART"."ARMartFct_BillingClaimData" bc 
    ON b."BillingClaimId" = bc."BillingClaimId" 
    AND b."_Customer" = bc."_Customer"
    AND bc."PatientId" Is NOT NULL
LEFT JOIN "DATAMART"."Reference_Patient" p 
    ON bc."PatientId" = p."PatientId" 
    AND bc."_Customer" = p."_Customer"
LEFT JOIN "DATAMART"."Reference_Office" office 
    ON office."OfficeKey" = bc."OfficeKey" 
    AND office."IsLive" = TRUE 
    AND bc."_Customer" = office."_Customer"
LEFT JOIN "DATAMART"."Reference_Order" ord 
    ON ord."OrderId" = b."OrderId" 
    AND ord."_Customer" = b."_Customer"
INNER JOIN "DATAMART"."Reference_Carrier" c 
    ON c."CarrierKey" = bc."CarrierKey" 
    AND bc."_Customer" = c."_Customer"
INNER JOIN "DATAMART"."Reference_Plan" pn 
    ON pn."PlanId" = bc."PlanId" 
    AND bc."_Customer" = pn."_Customer"
LEFT JOIN claim_status cs 
    ON bc."BillingClaimId" = cs."BillingClaimId" 
    AND bc."_Customer" = cs."_Customer"
LEFT JOIN latest_note note 
    ON note."ClaimId" = b."BillingClaimId"
WHERE bc."IsCurrent" = TRUE
GROUP BY ALL
HAVING SUM(b."InsuranceAR" + b."PatientAR") <> 0
ORDER BY "PatientId";

-- View: DATAMART.ARMartFct_MonthlyMeasuresBilling
-- Columns: 14
create or replace view "ARMartFct_MonthlyMeasuresBilling"(
	"Source",
	"_Customer",
	"CompanyInfoId",
	"OfficeKey",
	"BalanceType",
	"BalanceCategory",
	"OrderId",
	MONTH_END,
	"InsuranceBalanceDue",
	"PatientBalanceDue",
	"ClaimBalanceDue",
	"InsuranceDaysOutstanding",
	"PatientDaysOutstanding",
	"ClaimDaysOutstanding"
) as
WITH cte_floor as (
    SELECT 
        cd."DATE" as "CurrentDate",
        cd."MONTH_START",
        DATEADD(MONTH, -12, cd."MONTH_START") AS "window_startDate",
        DATEADD(DAY, 1, cd."DATE") AS "window_endDate"
    FROM "DATAMART"."ARMartDim_Date" cd
    WHERE cd."IS_CURRENT_DATE" = 1
),
cte_rankedTransaction AS (
    SELECT
        t."_Customer",
        t."OrderId",
        t."BillingClaimId",
        t."BillingClaimLineItemId",
        t."DateTime"::DATE AS "DateTransaction",
        t."InsuranceAR",
        t."PatientAR",
        t."InsuranceAR" + t."PatientAR" AS "ClaimAR"
    FROM DATAMART."ARMartFct_BillingTransaction" t
    JOIN cte_floor cd ON 1=1
    WHERE t."DateTime" >= cd."window_startDate" AND t."DateTime" < cd."window_endDate"
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t."_Customer", t."OrderId", t."BillingClaimId", t."BillingClaimLineItemId", t."DateTime"::DATE
        ORDER BY t."BillingTransactionId" DESC
    ) = 1

    UNION ALL

    SELECT
        t."_Customer",
        t."OrderId",
        t."BillingClaimId",
        t."BillingClaimLineItemId",
        t."DateTime"::DATE AS "DateTransaction",
        t."InsuranceAR",
        t."PatientAR",
        t."InsuranceAR" + t."PatientAR" AS "ClaimAR"
    FROM DATAMART."ARMartFct_BillingTransaction" t
    JOIN cte_floor cd ON 1=1
    WHERE t."DateTime" < cd."window_startDate"
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t."_Customer", t."OrderId", t."BillingClaimId", t."BillingClaimLineItemId"
        ORDER BY t."BillingTransactionId" DESC
    ) = 1
    AND (t."InsuranceAR" + t."PatientAR") <> 0
),
cte_ClaimLineItemByDayGrid AS (
    SELECT
        l."_Customer",
        l."OrderId",
        l."BillingClaimId",
        l."BillingClaimLineItemId",
        d."DateTransaction"
    FROM (
        SELECT DISTINCT
            "_Customer",
            "OrderId",
            "BillingClaimId",
            "BillingClaimLineItemId"
        FROM cte_rankedTransaction
    ) l
    JOIN (
        SELECT DISTINCT
            "_Customer",
            "OrderId",
            "BillingClaimId",
            "DateTransaction"
        FROM cte_rankedTransaction
    ) d ON l."_Customer" = d."_Customer"
        AND l."OrderId" = d."OrderId"
        AND l."BillingClaimId" = d."BillingClaimId"
),
cte_CalculateDailyLineItemBalance AS (
    SELECT
        g."_Customer",
        g."OrderId",
        g."BillingClaimId",
        g."BillingClaimLineItemId",
        g."DateTransaction",
        LAST_VALUE(li."InsuranceAR") IGNORE NULLS OVER (
            PARTITION BY g."_Customer", g."OrderId", g."BillingClaimId", g."BillingClaimLineItemId"
            ORDER BY g."DateTransaction"
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS "InsuranceAR_asof",
        LAST_VALUE(li."PatientAR") IGNORE NULLS OVER (
            PARTITION BY g."_Customer", g."OrderId", g."BillingClaimId", g."BillingClaimLineItemId"
            ORDER BY g."DateTransaction"
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS "PatientAR_asof",
        LAST_VALUE(li."ClaimAR") IGNORE NULLS OVER (
            PARTITION BY g."_Customer", g."OrderId", g."BillingClaimId", g."BillingClaimLineItemId"
            ORDER BY g."DateTransaction"
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS "ClaimAR_asof"
    FROM cte_ClaimLineItemByDayGrid g
    LEFT JOIN cte_rankedTransaction li
      ON li."_Customer"             = g."_Customer"
     AND li."OrderId"               = g."OrderId"
     AND li."BillingClaimId"        = g."BillingClaimId"
     AND li."BillingClaimLineItemId"= g."BillingClaimLineItemId"
     AND li."DateTransaction"       = g."DateTransaction"
),
cte_CalculateDailyClaimBalance as (
    SELECT 
        "_Customer",
        "OrderId",
        "BillingClaimId",
        "DateTransaction",
        SUM(COALESCE("InsuranceAR_asof",0)) as "DailyInsuranceBalance",
        SUM(COALESCE("PatientAR_asof",0)) as "DailyPatientBalance",
        SUM(COALESCE("ClaimAR_asof",0)) as "DailyClaimBalance",
        ROW_NUMBER() OVER (
            PARTITION BY "_Customer", "OrderId", "BillingClaimId", LAST_DAY("DateTransaction")
            ORDER BY "DateTransaction" DESC
        ) AS "rn_m_end"
    FROM cte_CalculateDailyLineItemBalance
    GROUP BY 
        "_Customer",
        "OrderId",
        "BillingClaimId",
        "DateTransaction"
    QUALIFY "rn_m_end" = 1
),
cte_CalculateMonthlyClaimBalance AS (
    SELECT
        "_Customer",
        "OrderId",
        "BillingClaimId",
        LAST_DAY("DateTransaction") as "MonthEnd",
        "DailyInsuranceBalance" AS "MonthlyInsuranceBalance",
        "DailyPatientBalance" AS "MonthlyPatientBalance",
        "DailyClaimBalance" AS "MonthlyClaimBalance"
    FROM cte_CalculateDailyClaimBalance
),
cte_DetermineLatestNonZeroClaimBalance AS (
    SELECT
        "_Customer",
        "OrderId",
        "BillingClaimId",
        MAX(IFF("DailyInsuranceBalance" <> 0, "DateTransaction", NULL)) as "DateLatestNonZeroInsuranceBalance",
        MAX(IFF("DailyPatientBalance" <> 0, "DateTransaction", NULL)) as "DateLatestNonZeroPatientBalance"
    FROM cte_CalculateDailyClaimBalance
    WHERE "DailyInsuranceBalance" <> 0 OR "DailyPatientBalance" <> 0
    GROUP BY
        "_Customer",
        "OrderId",
        "BillingClaimId"
),
cte_DetermineClaimZeroBalance AS (
    SELECT
        d."_Customer",
        d."OrderId",
        d."BillingClaimId", 
        MIN(IFF(n."DateLatestNonZeroInsuranceBalance" IS NOT NULL 
                    AND d."DailyInsuranceBalance" = 0
                    AND d."DateTransaction" > n."DateLatestNonZeroInsuranceBalance"
                ,d."DateTransaction"
                ,NULL)
            )as "DateZeroInsuranceBalance",
        MIN(IFF(n."DateLatestNonZeroPatientBalance" IS NOT NULL 
                AND d."DailyPatientBalance" = 0 
                AND d."DateTransaction" > n."DateLatestNonZeroPatientBalance"
                , d."DateTransaction"
                , NULL)
            ) as "DateZeroPatientBalance",
        MIN(IFF(n."DateLatestNonZeroInsuranceBalance" IS NOT NULL 
                AND n."DateLatestNonZeroPatientBalance" IS NOT NULL 
                AND d."DailyClaimBalance" = 0 
                AND d."DateTransaction" > GREATEST(n."DateLatestNonZeroInsuranceBalance", n."DateLatestNonZeroPatientBalance")
                , d."DateTransaction"
                , NULL)
            ) as "DateZeroClaimBalance"
    FROM cte_CalculateDailyClaimBalance d
    LEFT JOIN cte_DetermineLatestNonZeroClaimBalance n on d."_Customer" = n."_Customer"
        AND d."OrderId" = n."OrderId"
        AND d."BillingClaimId" = n."BillingClaimId"
    GROUP BY
        d."_Customer",
        d."OrderId",
        d."BillingClaimId"
),
cte_AdjustedClaimInfo AS (
    SELECT
        cd."_Customer",
        cd."CompanyInfoId",
        cd."OfficeKey",
        t."OrderId",
        cd."BillingClaimId",
        MIN(cd."ServiceDate") AS "DateTimeSale",
        TO_DATE(MIN(cd."ServiceDate")) AS "DateSale",
        MAX(IFF(l."DateLatestNonZeroInsuranceBalance" IS NOT NULL, 1, 0)) AS "IsHadInsuranceBalance",
        MAX(IFF(l."DateLatestNonZeroPatientBalance" IS NOT NULL, 1, 0)) AS "IsHadPatientBalance",
        MIN(b."DateZeroInsuranceBalance") AS "DateZeroInsuranceBalance",
        MIN(b."DateZeroPatientBalance") AS "DateZeroPatientBalance",
        MIN(b."DateZeroClaimBalance") AS "DateZeroClaimBalance"
    FROM DATAMART."ARMartFct_BillingClaimData" cd
        JOIN cte_rankedTransaction t 
            ON cd."BillingClaimId" = t."BillingClaimId" 
                AND cd."_Customer" = t."_Customer"
        JOIN DATAWAREHOUSE."BillingClaimOrder_HUB" co 
            ON cd."BillingClaimId" = co."BillingClaimId" 
                AND t."OrderId" = co."OrderId" 
                AND cd."_Customer" = co."_Customer"
        JOIN cte_DetermineLatestNonZeroClaimBalance l 
            ON l."_Customer" = cd."_Customer" 
                AND l."BillingClaimId" = cd."BillingClaimId"
                AND l."OrderId" = co."OrderId"
        LEFT JOIN cte_DetermineClaimZeroBalance b 
            ON t."_Customer" = b."_Customer" 
                AND t."BillingClaimId" = b."BillingClaimId"
                AND t."OrderId" = b."OrderId"
    WHERE cd."IsCurrent" = 'true'
    GROUP BY
        cd."_Customer",
        cd."CompanyInfoId",
        cd."OfficeKey",
        t."OrderId",
        cd."BillingClaimId"
),
cte_aggr AS (
    SELECT 
  		'Billing' AS "Source",
  		c."_Customer",
        c."CompanyInfoId",
        c."OfficeKey",
        'Claim' AS "BalanceType",
        c."OrderId",
        cdd."MONTH_END",
        CASE WHEN c."IsHadInsuranceBalance" = 1 THEN
            COALESCE(
                LAST_VALUE(b."MonthlyInsuranceBalance") IGNORE NULLS OVER (PARTITION BY c."_Customer", c."BillingClaimId" 
                ORDER BY cdd."MONTH_END" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            , 0) 
        END AS "InsuranceBalanceDue",
        CASE WHEN c."IsHadPatientBalance" = 1 THEN
            COALESCE(
                LAST_VALUE(b."MonthlyPatientBalance") IGNORE NULLS OVER (PARTITION BY c."_Customer", c."BillingClaimId" 
                ORDER BY cdd."MONTH_END" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            , 0) 
        END AS "PatientBalanceDue",
        COALESCE(
            LAST_VALUE(b."MonthlyClaimBalance") IGNORE NULLS OVER (PARTITION BY c."_Customer", c."BillingClaimId" 
            ORDER BY cdd."MONTH_END" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            ,0
        ) AS "ClaimBalanceDue",
        CASE
            WHEN c."IsHadInsuranceBalance" <> 1 THEN NULL
            WHEN COALESCE(c."DateZeroInsuranceBalance", cd."CurrentDate") < cdd.DATE OR c."DateSale" > cdd."MONTH_END" THEN 0
            ELSE SUM(
                DATEDIFF(DAY, 
                    GREATEST(c."DateSale", cdd."MONTH_START"),
                    LEAST(COALESCE("DateZeroInsuranceBalance", cd."CurrentDate"), cdd."MONTH_END")
                ) + 1 
            ) OVER (PARTITION BY c."_Customer", c."BillingClaimId" ORDER BY cdd.DATE ROWS UNBOUNDED PRECEDING)
        END AS "InsuranceDaysOutstanding",
        CASE
            WHEN c."IsHadPatientBalance" <> 1 THEN NULL
            WHEN COALESCE(c."DateZeroPatientBalance", cd."CurrentDate") < cdd.DATE OR c."DateSale" > cdd."MONTH_END" THEN 0
            ELSE SUM(
                DATEDIFF(DAY, 
                    GREATEST(c."DateSale", cdd."MONTH_START"),
                    LEAST(COALESCE("DateZeroPatientBalance", cd."CurrentDate"), cdd."MONTH_END")
                ) + 1 
            ) OVER (PARTITION BY c."_Customer", c."BillingClaimId" ORDER BY cdd.DATE ROWS UNBOUNDED PRECEDING)
        END AS "PatientDaysOutstanding",
        CASE
            WHEN COALESCE(c."DateZeroClaimBalance", cd."CurrentDate") < cdd.DATE OR c."DateSale" > cdd."MONTH_END" THEN 0
            ELSE SUM(
                DATEDIFF(DAY, 
                    GREATEST(c."DateSale", cdd."MONTH_START"),
                    LEAST(COALESCE("DateZeroClaimBalance", cd."CurrentDate"), cdd."MONTH_END")
                ) + 1 
            ) OVER (PARTITION BY c."_Customer", c."BillingClaimId" ORDER BY cdd.DATE ROWS UNBOUNDED PRECEDING)
        END AS "ClaimDaysOutstanding"
    FROM cte_AdjustedClaimInfo c
        LEFT JOIN cte_floor cd ON 1=1
        JOIN "DATAMART"."ARMartDim_Date" cdd 
        	ON cdd."DAY_IN_MONTH" = 1
        		AND cdd."MONTH_END" >= c."DateSale"
        		AND cdd.DATE <= LAST_DAY(COALESCE(c."DateZeroClaimBalance", cd."CurrentDate"))
                AND cdd."MONTH_END" < cd."MONTH_START"
        LEFT JOIN cte_CalculateMonthlyClaimBalance b 
            on c."BillingClaimId" = b."BillingClaimId" 
                AND c."OrderId" = b."OrderId" 
                AND c."_Customer" = b."_Customer" 
                AND b."MonthEnd" = cdd."MONTH_END"
    WHERE c."DateSale" <> COALESCE(c."DateZeroClaimBalance", '1901-01-01')
        AND (c."IsHadInsuranceBalance" = 1 OR c."IsHadPatientBalance" = 1)
)
/*******************************************************************************
Patient Logic
*******************************************************************************/
SELECT "Source",
    "_Customer",
    "CompanyInfoId",
    "OfficeKey",
    "BalanceType",
    'Patient' AS "BalanceCategory",
    "OrderId",
    "MONTH_END",
    0 AS "InsuranceBalanceDue",
    "PatientBalanceDue",
    "PatientBalanceDue" AS "ClaimBalanceDue",
    0 AS "InsuranceDaysOutstanding",
    "PatientDaysOutstanding",
    "PatientDaysOutstanding" AS "ClaimDaysOutstanding"
FROM cte_aggr
WHERE "PatientBalanceDue" <> 0

UNION ALL
/*******************************************************************************
Insurance Logic
*******************************************************************************/
SELECT "Source",
    "_Customer",
    "CompanyInfoId",
    "OfficeKey",
    "BalanceType",
    'Carrier' AS "BalanceCategory",
    "OrderId",
    "MONTH_END",
    "InsuranceBalanceDue",
    0 AS "PatientBalanceDue",
    "InsuranceBalanceDue" AS "ClaimBalanceDue",
    "InsuranceDaysOutstanding",
    0 AS "PatientDaysOutstanding",
    "InsuranceDaysOutstanding" AS "ClaimDaysOutstanding"
FROM cte_aggr
WHERE "InsuranceBalanceDue" <> 0;

-- View: DATAMART.ARMartFct_MonthlyMeasuresPOS
-- Columns: 10
create or replace view "ARMartFct_MonthlyMeasuresPOS"(
	"Source",
	"_Customer",
	"CompanyInfoId",
	"OfficeKey",
	"BalanceType",
	"BalanceCategory",
	"OrderId",
	MONTH_END,
	"BalanceDue",
	"DaysOutstanding"
) as
WITH cte_floor as (
    SELECT 
        cd."DATE" as "CurrentDate",
        cd."MONTH_START" as "CurrentMonthStart"
    FROM "DATAMART"."ARMartDim_Date" cd
    WHERE cd."IS_CURRENT_DATE" = 1
),
cte_OrderedTransactions AS (
    SELECT t."_Customer",
        t."OrderId",
        t."PosTransactionId",
        t."PosTransactionTypeId",
        t."DateTime",
        t."Amount",
        SUM(t."Amount") OVER (
            PARTITION BY t."_Customer", t."OrderId"
            ORDER BY t."PosTransactionId" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS "RunningTransactionBalance"
    FROM "DATAMART"."ARMartFct_PosTransaction" t
    JOIN cte_floor cd ON 1=1
    WHERE t."OrderId" IS NOT NULL
        AND t."DateTime" < cd."CurrentMonthStart"
),
cte_AdjustedOrderInfo AS (
    SELECT o."_Customer",
        rof."CompanyInfoId",
        o."OfficeKey",
        o."OrderId",
        MIN(CASE WHEN ot."PosTransactionTypeId" IN (1,8) THEN ot."DateTime" END) AS "DateTimeSale",
        MIN(CASE WHEN ot."PosTransactionTypeId" IN (1,8) THEN TO_DATE(ot."DateTime") END) AS "DateSale",
        MIN(CASE WHEN ot."RunningTransactionBalance" = 0 THEN ot."DateTime" END) AS "DateTimeZeroBalance",
        MIN(CASE WHEN ot."RunningTransactionBalance" = 0 THEN TO_DATE(ot."DateTime") END) AS "DateZeroBalance"
    FROM "DATAMART"."Reference_Order" o
        JOIN cte_OrderedTransactions ot 
  			ON o."_Customer" = ot."_Customer"
        		AND o."OrderId" = ot."OrderId"
        LEFT JOIN "DATAMART"."Reference_Office" rof 
  			ON o."_Customer" = rof."_Customer"
                AND o."OfficeKey" = rof."OfficeKey"
    GROUP BY o."_Customer",
        rof."CompanyInfoId",
        o."OfficeKey",
        o."OrderId"
),
cte_CalculateMonthlyOrderBalance AS (
    SELECT t."_Customer",
        t."OrderId",
        bd."MONTH_END",
        MIN(IFF(t."RunningTransactionBalance" <= 0, 0, t."RunningTransactionBalance")) AS "OrderRunningBalance"
    FROM cte_OrderedTransactions t
        JOIN "DATAMART"."ARMartDim_Date" bd ON TO_DATE(t."DateTime") = bd."DATE"
    GROUP BY
        t."_Customer",
        t."OrderId",
        bd."MONTH_END"
),
cte_aggr AS (
    SELECT 
        'POS' AS "Source",
    	o."_Customer",
        o."CompanyInfoId",
        o."OfficeKey",
        'Order' AS "BalanceType",
        'Patient' AS "BalanceCategory",
        o."OrderId",
        c."MONTH_END",
        COALESCE(
            LAST_VALUE(b."OrderRunningBalance") IGNORE NULLS OVER (
                PARTITION BY o."_Customer", o."OrderId"
                ORDER BY c."MONTH_END" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            , 0
        ) AS "BalanceDue",
        SUM(
            CASE
                WHEN COALESCE(o."DateZeroBalance", cd."CurrentDate") < c."DATE"
                    OR o."DateSale" > c."MONTH_END" THEN 0
                ELSE DATEDIFF(
                    DAY,
                    GREATEST(o."DateSale", c."DATE"),
                    LEAST(COALESCE(o."DateZeroBalance", cd."CurrentDate"), c."MONTH_END")
                ) + 1
            END
        ) OVER (
            PARTITION BY o."_Customer", o."OrderId"
            ORDER BY c."DATE" ROWS UNBOUNDED PRECEDING
        ) AS "DaysOutstanding"
    FROM cte_AdjustedOrderInfo o
        JOIN cte_floor cd ON 1=1
        JOIN "DATAMART"."ARMartDim_Date" c ON c."DAY_IN_MONTH" = 1
            AND c."MONTH_END" >= o."DateSale"
            AND c.DATE <= LAST_DAY(COALESCE(o."DateZeroBalance", cd."CurrentDate"))
            AND c."MONTH_END" < cd."CurrentMonthStart"
            AND NOT (
                o."DateZeroBalance" IS NOT NULL
                AND o."DateZeroBalance" >= c."MONTH_START"
                AND o."DateZeroBalance" <  c."MONTH_END"
            )
        LEFT JOIN cte_CalculateMonthlyOrderBalance b ON o."_Customer" = b."_Customer"
            AND o."OrderId" = b."OrderId"
            AND b."MONTH_END" = c."MONTH_END"
    WHERE o."DateSale" <> COALESCE(o."DateZeroBalance", '1901-01-01')
)
select 
    "Source",
    "_Customer",
    "CompanyInfoId",
    "OfficeKey",
    "BalanceType",
    "BalanceCategory",
    "OrderId",
    "MONTH_END",
    "BalanceDue",
    "DaysOutstanding"
from cte_aggr;

-- View: DATAMART.ARMartFct_MonthlyMeasures_Test
-- Columns: 9
create or replace view "ARMartFct_MonthlyMeasures_Test"(
	"CompanyInfoId",
	"OfficeKey",
	"MonthEnd",
	"OrderId",
	"TotalBalanceDue",
	"TotalDaysOutstanding",
	"Source",
	"BalanceCategory",
	"AgingBucket"
) as

WITH  
cte_combine AS (
  SELECT 
      COALESCE(pos."_Customer",billing."_Customer")         	AS "_Customer",
      COALESCE(pos."CompanyInfoId",billing."CompanyInfoId") 	AS "CompanyInfoId",
      COALESCE(pos."OfficeKey",billing."OfficeKey")         	AS "OfficeKey",
      COALESCE(pos."BalanceType",billing."BalanceType")     	AS "BalanceType",
  	  COALESCE(pos."BalanceCategory",billing."BalanceCategory") AS "BalanceCategory",
      COALESCE(pos."OrderId",billing."OrderId")             	AS "OrderId",
      COALESCE(pos."MONTH_END",billing."MONTH_END")         	AS "MONTH_END",
      IFNULL(pos."BalanceDue",0) 								AS "OrderBalanceDue",	--order/patient
      IFNULL(pos."DaysOutstanding",0) 						    AS "OrderDaysOutstanding",	--order/patient
      IFNULL(billing."InsuranceBalanceDue",0) 				    AS "InsuranceBalanceDue", --claim/carrier
      IFNULL(billing."PatientBalanceDue",0) 					AS "PatientBalanceDue", --claim/patient
      IFNULL(billing."ClaimBalanceDue",0) 					    AS "ClaimBalanceDue", --claim/Both
      IFNULL(billing."InsuranceDaysOutstanding",0) 			    AS "InsuranceDaysOutstanding", --claim/carrier
      IFNULL(billing."PatientDaysOutstanding",0) 				AS "PatientDaysOutstanding", --claim/patient
      IFNULL(CASE 
        WHEN IFNULL(billing."InsuranceDaysOutstanding",0) >= IFNULL(billing."PatientDaysOutstanding",0) THEN IFNULL(billing."InsuranceDaysOutstanding",0)
            ELSE IFNULL(billing."PatientDaysOutstanding",0) END,0) AS "ClaimBalanceDaysOutstanding" --claim/both
  FROM "DATAMART"."ARMartFct_MonthlyMeasuresPOS" pos
    FULL OUTER JOIN "DATAMART"."ARMartFct_MonthlyMeasuresBilling" billing
        ON  pos."_Customer"=billing."_Customer"
          AND pos."CompanyInfoId"=billing."CompanyInfoId"
          AND pos."OfficeKey"=billing."OfficeKey"
          AND pos."OrderId"=billing."OrderId"
          AND pos."MONTH_END"=billing."MONTH_END"),
cte_totals AS (
    SELECT *,
    	"OrderBalanceDue"+"ClaimBalanceDue" 		  AS "TotalBalanceDue",
        IFNULL(CASE 
          WHEN "OrderDaysOutstanding" >= "ClaimBalanceDaysOutstanding" THEN "OrderDaysOutstanding"
          	ELSE "ClaimBalanceDaysOutstanding" END,0) AS "TotalDaysOutstanding"
    FROM cte_combine)

  SELECT m."CompanyInfoId",
    m."OfficeKey",
    TO_DATE(m."MONTH_END") AS "MonthEnd",
    m."OrderId",
    m."TotalBalanceDue",
    m."TotalDaysOutstanding",
    CASE
      WHEN m."BalanceType" = 'Order' THEN 'POS'
      WHEN m."BalanceType" = 'Claim' THEN 'Billing'
        ELSE NULL END AS "Source",
   m. "BalanceCategory",
    CASE
      WHEN m."TotalDaysOutstanding" IS NULL THEN NULL
      WHEN m."TotalDaysOutstanding" <= 30 THEN '1–30 days'
      WHEN m."TotalDaysOutstanding" <= 60 THEN '31–60 days'
      WHEN m."TotalDaysOutstanding" <= 90 THEN '61–90 days'
      WHEN m."TotalDaysOutstanding" <= 120 THEN '91–120 days'
        ELSE 'Over 120 days' END AS "AgingBucket"
  FROM cte_totals m
    LEFT JOIN "DATAMART"."ARMartDim_Date" cd 
    	ON cd."IS_CURRENT_DATE" = 1
  WHERE m."MONTH_END" > ADD_MONTHS(cd."MONTH_END",-12)
    -- and m."OrderId"=3057537
    and m."MONTH_END"='2026-02-28';

-- View: DATAMART.ARMartFct_POSBalanceDetails
-- Columns: 28
create or replace view "ARMartFct_POSBalanceDetails"(
	"Source",
	"BalanceCategory",
	"_Customer",
	"CompanyId",
	"OfficeNum",
	"OfficeName",
	"PatientId",
	"PatientFirstName",
	"PatientLastName",
	"PatientFullName",
	"OrderNum",
	"OrderStatus",
	"OrderStatusCodeDescription",
	"OrderDate",
	"OrderBalance",
	"OrderBalanceDaysOutstanding",
	"CarrierId",
	"CarrierName",
	"PlanId",
	"PlanName",
	"ClaimId",
	"ClaimStatus",
	"ClaimLatestNote",
	"ClaimServiceDate",
	"ClaimInsuranceBalance",
	"ClaimPatientBalance",
	"ClaimTotalBalance",
	"ClaimBalanceDaysOutstanding"
) as
WITH 
POSBalance AS (
    SELECT pt."_Customer", 
        IFNULL(ror."DateTime", TO_DATE('2000-01-01')) 			AS "OrderDate",
        rof."CompanyInfoId" 									AS "CompanyId",
        IFNULL(ror."OfficeKey", pt."OfficeKey") 				AS "OfficeNum",
        rof."Name" 												AS "OfficeName",
        IFNULL(ror."OrderId", 0) 								AS "OrderNum",
        ror."StatusCode" 										AS "OrderStatus",
  	    ror."StatusCode_Description" 							AS "OrderStatusCodeDescription",
        pt."Amount" 											AS "OrderAmount",
        0 														AS "PaidAmount",
        NULL 													AS "LastPaymentDate",
        pt."PatientId",
        pt."PosTransactionTypeId"
    FROM "DATAMART"."ARMartFct_PosTransaction" pt
        LEFT JOIN "DATAMART"."Reference_Order" ror 
            ON pt."_Customer" = ror."_Customer"
                AND pt."OrderId" = ror."OrderId"
        JOIN "DATAMART"."Reference_Office" rof 
            ON pt."_Customer" = rof."_Customer"
                AND COALESCE(ror."OfficeKey",pt."OfficeKey") = rof."OfficeKey"
    WHERE pt."InvoiceSummaryId" IS NOT NULL
        OR (
            (
                pt."PosTransactionTypeId" = 13
                OR pt."PosTransactionTypeId" = 14
            )
            AND pt."InvoiceSummaryId" IS NULL
        )
        
    UNION ALL
    
    SELECT pt."_Customer",
        IFNULL(ror."DateTime", pt."DateTime") 					AS "OrderDate",
        rof."CompanyInfoId" 									AS "CompanyId",
        IFNULL(ror."OfficeKey", pt."OfficeKey") 				AS "OfficeNum",
        rof."Name" 												AS "OfficeName",
        IFNULL(pt."OrderId", 0) 								AS "OrderNum",
        ror."StatusCode" 										AS "OrderStatus",
  		ror."StatusCode_Description" 							AS "OrderStatusCodeDescription",
        0 														AS "OrderAmount",
        CASE
            WHEN (
                ABS(pt."Amount") < ABS(
                    (
                        SELECT SUM("Amount")
                        FROM "DATAMART"."ARMartDim_PosPaymentDetail"
                        WHERE "PosPaymentId" = pt."PosPaymentId"
                      		and "_Customer" = pt."_Customer"
                    )
                )
            ) THEN pt."Amount"
            ELSE (
                SELECT SUM("Amount")
                FROM "DATAMART"."ARMartDim_PosPaymentDetail"
                WHERE "PosPaymentId" = pt."PosPaymentId"
              		and "_Customer" = pt."_Customer"
            )
        END 													AS "PaidAmount",
        CASE
            WHEN pt."PosTransactionTypeId" = 2 THEN pt."DateTime"
            ELSE NULL
        END 													AS "LastPaymentDate",
        pt."PatientId",
        pt."PosTransactionTypeId"
    FROM "DATAMART"."ARMartFct_PosTransaction" pt
        LEFT JOIN "DATAMART"."Reference_Order" ror 
            ON pt."_Customer" = ror."_Customer"
                AND pt."OrderId" = ror."OrderId"
        JOIN "DATAMART"."Reference_Office" rof 
            ON pt."_Customer" = rof."_Customer"
                AND COALESCE(ror."OfficeKey",pt."OfficeKey") = rof."OfficeKey"
    WHERE pt."PosPaymentId" IS NOT NULL
        OR pt."PosTransactionTypeId" = 8
)

SELECT 'POS' 													AS "Source",
    'Patient' 													AS "BalanceCategory",
    pos."_Customer",
    pos."CompanyId",
    pos."OfficeNum",
    pos."OfficeName",
    IFNULL(pos."PatientId", 0) 									AS "PatientId",
    p."FirstName" 												AS "PatientFirstName",
    p."LastName" 												AS "PatientLastName",
    p."FirstName" || ' ' || p."LastName" 						AS "PatientFullName",
    pos."OrderNum" 												AS "OrderNum",
    pos."OrderStatus",
    pos."OrderStatusCodeDescription",
    MAX(pos."OrderDate") 										AS "OrderDate",
    SUM(pos."OrderAmount") + SUM(pos."PaidAmount") 				AS "OrderBalance",
    CASE
        WHEN SUM(pos."OrderAmount") + SUM(pos."PaidAmount") = 0 THEN DATEDIFF(
            'day',
            MAX(pos."OrderDate"),
            MAX(pos."LastPaymentDate")
        )
        ELSE DATEDIFF('day', MAX(pos."OrderDate"), GETDATE())
    END 														AS "OrderBalanceDaysOutstanding",
    NULL 														AS "CarrierId",
    NULL 														AS "CarrierName",
    NULL 														AS "PlanId",
    NULL 														AS "PlanName",
    NULL 														AS "ClaimId",
    NULL 														AS "ClaimStatus",
    NULL 														AS "ClaimLatestNote",
    NULL 														AS "ClaimServiceDate",
    NULL 														AS "ClaimInsuranceBalance",
    NULL 														AS "ClaimPatientBalance",
    NULL 														AS "ClaimTotalBalance",
    NULL 														AS "ClaimBalanceDaysOutstanding"
FROM POSBalance pos
    LEFT JOIN "DATAMART"."Reference_Patient" p 
        ON pos."_Customer" = p."_Customer"
            AND pos."PatientId" = p."PatientId"
WHERE pos."OrderNum" > 0
GROUP BY ALL
HAVING (COALESCE(SUM(pos."OrderAmount"), 0) + COALESCE(SUM(pos."PaidAmount"), 0)) > 0;

-- View: DATAMART.DashboardMartDim_CombinedInvoiceDetail
-- Columns: 21
create or replace view "DashboardMartDim_CombinedInvoiceDetail"(
	"InvoiceDetailId",
	"InvoiceSummaryId",
	"OrderId",
	"ItemTypeId",
	"ItemId",
	"Quantity",
	"Price",
	"DiscountTypeId",
	"Tax",
	"Discount",
	"PromotionDiscount",
	"LineDiscount",
	"PackageDiscount",
	"Amount",
	"Allowance",
	"Copay",
	"Receivable",
	"InsuranceDiscount",
	"EmployeeId",
	"ValuationCost",
	"_Customer"
) as
SELECT "InvoiceDetail"."InvoiceDetailId"
     , "InvoiceDetail"."InvoiceSummaryId"
     , "InvoiceDetail"."OrderId"
     , "InvoiceDetail"."ItemTypeId"
     , "InvoiceDetail"."ItemId"
     , ifnull("InvoiceDetail"."Quantity",0) as "Quantity"
     , ifnull("InvoiceDetail"."Price",0) as "Price"
     , "InvoiceDetail"."DiscountTypeId"
     , ifnull("InvoiceDetail"."Tax",0) as "Tax"
     , ifnull("InvoiceDetail"."Discount",0) as "Discount"
     , ifnull("InvoiceDetail"."PromotionDiscount",0) as "PromotionDiscount"
     , ifnull("InvoiceDetail"."LineDiscount",0) as "LineDiscount"
     , ifnull("InvoiceDetail"."PackageDiscount",0) as "PackageDiscount"
     , ifnull("InvoiceDetail"."Amount",0) as "Amount"
     , sum(ifnull("InvoiceInsuranceDetail"."Allowance", 0.0))         AS "Allowance"
     , sum(ifnull("InvoiceInsuranceDetail"."Copay", 0.0))             AS "Copay"
     , sum(ifnull("InvoiceInsuranceDetail"."Receivable", 0.0))        AS "Receivable"
     , sum(ifnull("InvoiceInsuranceDetail"."InsuranceDiscount", 0.0)) AS "InsuranceDiscount"
     , "InvoiceSummary"."EmployeeId"
     , ifnull("InvoiceDetail"."ValuationCost",0) as "ValuationCost"
     , "InvoiceDetail"."_Customer"

FROM DATAWAREHOUSE."InvoiceDetail"

         INNER JOIN DATAWAREHOUSE."InvoiceSummary"
                    ON DATAWAREHOUSE."InvoiceSummary"."InvoiceSummaryId" =
                       DATAWAREHOUSE."InvoiceDetail"."InvoiceSummaryId"
                        and DATAWAREHOUSE."InvoiceSummary"."_Customer" = DATAWAREHOUSE."InvoiceDetail"."_Customer"
                        AND DATAWAREHOUSE."InvoiceSummary".DELETED_FLAG <> 'Y'

         LEFT JOIN DATAWAREHOUSE."InvoiceInsuranceDetail"
                   on DATAWAREHOUSE."InvoiceInsuranceDetail"."InvoiceDetailId" =
                      DATAWAREHOUSE."InvoiceDetail"."InvoiceDetailId"
                       and
                      DATAWAREHOUSE."InvoiceInsuranceDetail"."_Customer" = DATAWAREHOUSE."InvoiceDetail"."_Customer"
                       AND DATAWAREHOUSE."InvoiceInsuranceDetail".DELETED_FLAG <> 'Y'

WHERE DATAWAREHOUSE."InvoiceDetail".DELETED_FLAG <> 'Y'
--   AND DATAWAREHOUSE."InvoiceSummary".DELETED_FLAG <> 'Y'
--   AND DATAWAREHOUSE."InvoiceInsuranceDetail".DELETED_FLAG <> 'Y'
GROUP BY "InvoiceDetail"."_Customer"
       , "InvoiceDetail"."InvoiceSummaryId"
       , "InvoiceDetail"."OrderId"
       , "InvoiceDetail"."ItemTypeId"
       , "InvoiceDetail"."ItemId"
       , "InvoiceDetail"."Quantity"
       , "InvoiceDetail"."Price"
       , "InvoiceDetail"."DiscountTypeId"
       , "InvoiceDetail"."Tax"
       , "InvoiceDetail"."Discount"
       , "InvoiceDetail"."PromotionDiscount"
       , "InvoiceDetail"."LineDiscount"
       , "InvoiceDetail"."PackageDiscount"
       , "InvoiceDetail"."Amount"
       , "InvoiceDetail"."InvoiceDetailId"
       , "InvoiceSummary"."EmployeeId"
       , "InvoiceDetail"."ValuationCost";

-- View: DATAMART.DashboardMartDim_DiagnosisCode
-- Columns: 64
create or replace view "DashboardMartDim_DiagnosisCode"(
	"OrderId",
	"ItemId",
	"InvoiceSummaryId",
	"OrderExamDetail_DiagnosisCodeId1",
	"OrderExamDetail_DiagnosisCodeId2",
	"OrderExamDetail_DiagnosisCodeId3",
	"OrderExamDetail_DiagnosisCodeId4",
	"OrderExamDetail_DiagnosisCodeId5",
	"OrderExamDetail_DiagnosisCodeId6",
	"OrderExamDetail_DiagnosisCodeId7",
	"OrderExamDetail_DiagnosisCodeId8",
	"OrderExamDetail_DiagnosisCodeId9",
	"OrderExamDetail_DiagnosisCodeId10",
	"OrderExamDetail_DiagnosisCodeId11",
	"OrderExamDetail_DiagnosisCodeId12",
	"DiagnosisCode1_Name",
	"DiagnosisCode1_Description",
	"DiagnosisCode1_DiagnosisCategoryId",
	"DiagnosisCategory1_Description",
	"DiagnosisCode2_Name",
	"DiagnosisCode2_Description",
	"DiagnosisCode2_DiagnosisCategoryId",
	"DiagnosisCategory2_Description",
	"DiagnosisCode3_Name",
	"DiagnosisCode3_Description",
	"DiagnosisCode3_DiagnosisCategoryId",
	"DiagnosisCategory3_Description",
	"DiagnosisCode4_Name",
	"DiagnosisCode4_Description",
	"DiagnosisCode4_DiagnosisCategoryId",
	"DiagnosisCategory4_Description",
	"DiagnosisCode5_Name",
	"DiagnosisCode5_Description",
	"DiagnosisCode5_DiagnosisCategoryId",
	"DiagnosisCategory5_Description",
	"DiagnosisCode6_Name",
	"DiagnosisCode6_Description",
	"DiagnosisCode6_DiagnosisCategoryId",
	"DiagnosisCategory6_Description",
	"DiagnosisCode7_Name",
	"DiagnosisCode7_Description",
	"DiagnosisCode7_DiagnosisCategoryId",
	"DiagnosisCategory7_Description",
	"DiagnosisCode8_Name",
	"DiagnosisCode8_Description",
	"DiagnosisCode8_DiagnosisCategoryId",
	"DiagnosisCategory8_Description",
	"DiagnosisCode9_Name",
	"DiagnosisCode9_Description",
	"DiagnosisCode9_DiagnosisCategoryId",
	"DiagnosisCategory9_Description",
	"DiagnosisCode10_Name",
	"DiagnosisCode10_Description",
	"DiagnosisCode10_DiagnosisCategoryId",
	"DiagnosisCategory10_Description",
	"DiagnosisCode11_Name",
	"DiagnosisCode11_Description",
	"DiagnosisCode11_DiagnosisCategoryId",
	"DiagnosisCategory11_Description",
	"DiagnosisCode12_Name",
	"DiagnosisCode12_Description",
	"DiagnosisCode12_DiagnosisCategoryId",
	"DiagnosisCategory12_Description",
	"_Customer"
) as
select DISTINCT "OrderExamDetail"."OrderId"
              , "OrderExamDetail"."ItemId"
              , "InvoiceSummary"."InvoiceSummaryId"
              , ifnull("OrderExamDetail"."DiagnosisCodeId1", 0)  AS "OrderExamDetail_DiagnosisCodeId1"
              , ifnull("OrderExamDetail"."DiagnosisCodeId2", 0)  AS "OrderExamDetail_DiagnosisCodeId2"
              , ifnull("OrderExamDetail"."DiagnosisCodeId3", 0)  AS "OrderExamDetail_DiagnosisCodeId3"
              , ifnull("OrderExamDetail"."DiagnosisCodeId4", 0)  AS "OrderExamDetail_DiagnosisCodeId4"
              , ifnull("OrderExamDetail"."DiagnosisCodeId5", 0)  AS "OrderExamDetail_DiagnosisCodeId5"
              , ifnull("OrderExamDetail"."DiagnosisCodeId6", 0)  AS "OrderExamDetail_DiagnosisCodeId6"
              , ifnull("OrderExamDetail"."DiagnosisCodeId7", 0)  AS "OrderExamDetail_DiagnosisCodeId7"
              , ifnull("OrderExamDetail"."DiagnosisCodeId8", 0)  AS "OrderExamDetail_DiagnosisCodeId8"
              , ifnull("OrderExamDetail"."DiagnosisCodeId9", 0)  AS "OrderExamDetail_DiagnosisCodeId9"
              , ifnull("OrderExamDetail"."DiagnosisCodeId10", 0) AS "OrderExamDetail_DiagnosisCodeId10"
              , ifnull("OrderExamDetail"."DiagnosisCodeId11", 0) AS "OrderExamDetail_DiagnosisCodeId11"
              , ifnull("OrderExamDetail"."DiagnosisCodeId12", 0) AS "OrderExamDetail_DiagnosisCodeId12"

              , DiagnosisCode1."Name"                 AS "DiagnosisCode1_Name"
              , DiagnosisCode1."Description"          AS "DiagnosisCode1_Description"
              , DiagnosisCode1."DiagnosisCategoryId"  AS "DiagnosisCode1_DiagnosisCategoryId"
              , DiagnosisCategory1."Description"      as "DiagnosisCategory1_Description"

              , DiagnosisCode2."Name"                 AS "DiagnosisCode2_Name"
              , DiagnosisCode2."Description"          AS "DiagnosisCode2_Description"
              , DiagnosisCode2."DiagnosisCategoryId"  AS "DiagnosisCode2_DiagnosisCategoryId"
              , DiagnosisCategory2."Description"      as "DiagnosisCategory2_Description"

              , DiagnosisCode3."Name"                 AS "DiagnosisCode3_Name"
              , DiagnosisCode3."Description"          AS "DiagnosisCode3_Description"
              , DiagnosisCode3."DiagnosisCategoryId"  AS "DiagnosisCode3_DiagnosisCategoryId"
              , DiagnosisCategory3."Description"      as "DiagnosisCategory3_Description"

              , DiagnosisCode4."Name"                 AS "DiagnosisCode4_Name"
              , DiagnosisCode4."Description"          AS "DiagnosisCode4_Description"
              , DiagnosisCode4."DiagnosisCategoryId"  AS "DiagnosisCode4_DiagnosisCategoryId"
              , DiagnosisCategory4."Description"      as "DiagnosisCategory4_Description"

              , DiagnosisCode5."Name"                 AS "DiagnosisCode5_Name"
              , DiagnosisCode5."Description"          AS "DiagnosisCode5_Description"
              , DiagnosisCode5."DiagnosisCategoryId"  AS "DiagnosisCode5_DiagnosisCategoryId"
              , DiagnosisCategory5."Description"      as "DiagnosisCategory5_Description"

              , DiagnosisCode6."Name"                 AS "DiagnosisCode6_Name"
              , DiagnosisCode6."Description"          AS "DiagnosisCode6_Description"
              , DiagnosisCode6."DiagnosisCategoryId"  AS "DiagnosisCode6_DiagnosisCategoryId"
              , DiagnosisCategory6."Description"      as "DiagnosisCategory6_Description"

              , DiagnosisCode7."Name"                 AS "DiagnosisCode7_Name"
              , DiagnosisCode7."Description"          AS "DiagnosisCode7_Description"
              , DiagnosisCode7."DiagnosisCategoryId"  AS "DiagnosisCode7_DiagnosisCategoryId"
              , DiagnosisCategory7."Description"      as "DiagnosisCategory7_Description"

              , DiagnosisCode8."Name"                 AS "DiagnosisCode8_Name"
              , DiagnosisCode8."Description"          AS "DiagnosisCode8_Description"
              , DiagnosisCode8."DiagnosisCategoryId"  AS "DiagnosisCode8_DiagnosisCategoryId"
              , DiagnosisCategory8."Description"      as "DiagnosisCategory8_Description"

              , DiagnosisCode9."Name"                 AS "DiagnosisCode9_Name"
              , DiagnosisCode9."Description"          AS "DiagnosisCode9_Description"
              , DiagnosisCode9."DiagnosisCategoryId"  AS "DiagnosisCode9_DiagnosisCategoryId"
              , DiagnosisCategory9."Description"      as "DiagnosisCategory9_Description"

              , DiagnosisCode10."Name"                AS "DiagnosisCode10_Name"
              , DiagnosisCode10."Description"         AS "DiagnosisCode10_Description"
              , DiagnosisCode10."DiagnosisCategoryId" AS "DiagnosisCode10_DiagnosisCategoryId"
              , DiagnosisCategory10."Description"     as "DiagnosisCategory10_Description"

              , DiagnosisCode11."Name"                AS "DiagnosisCode11_Name"
              , DiagnosisCode11."Description"         AS "DiagnosisCode11_Description"
              , DiagnosisCode11."DiagnosisCategoryId" AS "DiagnosisCode11_DiagnosisCategoryId"
              , DiagnosisCategory11."Description"     as "DiagnosisCategory11_Description"

              , DiagnosisCode12."Name"                AS "DiagnosisCode12_Name"
              , DiagnosisCode12."Description"         AS "DiagnosisCode12_Description"
              , DiagnosisCode12."DiagnosisCategoryId" AS "DiagnosisCode12_DiagnosisCategoryId"
              , DiagnosisCategory12."Description"     as "DiagnosisCategory12_Description"
              , "OrderExamDetail"."_Customer"

FROM DATAWAREHOUSE."OrderExamDetail"

         LEFT JOIN DATAWAREHOUSE."Item"
                   on "Item"."ItemId" = "OrderExamDetail"."ItemId"
                       and "Item"."_Customer" = "OrderExamDetail"."_Customer"
                        AND "Item"."DELETED_FLAG" <> 'Y'
    
         LEFT JOIN DATAWAREHOUSE."DiagnosisCode" DiagnosisCode1
                   on DiagnosisCode1."DiagnosisCodeId" = "OrderExamDetail"."DiagnosisCodeId1"
                       and DiagnosisCode1."_Customer" = "OrderExamDetail"."_Customer"
                        AND DiagnosisCode1."DELETED_FLAG" <> 'Y'
    
         left join DATAWAREHOUSE."DiagnosisCategory" DiagnosisCategory1
                   on DiagnosisCategory1."DiagnosisCategoryId" = DiagnosisCode1."DiagnosisCategoryId"
                       and DiagnosisCategory1."_Customer" = DiagnosisCode1."_Customer"
                        AND DiagnosisCategory1."DELETED_FLAG" <> 'Y'
    
         LEFT JOIN DATAWAREHOUSE."DiagnosisCode" DiagnosisCode2
                   on DiagnosisCode2."DiagnosisCodeId" = "OrderExamDetail"."DiagnosisCodeId2"
                       and DiagnosisCode2."_Customer" = "OrderExamDetail"."_Customer"
                        AND DiagnosisCode2."DELETED_FLAG" <> 'Y'
    
         left join DATAWAREHOUSE."DiagnosisCategory" DiagnosisCategory2
                   on DiagnosisCategory2."DiagnosisCategoryId" = DiagnosisCode2."DiagnosisCategoryId"
                       and DiagnosisCategory2."_Customer" = DiagnosisCode2."_Customer"
                        AND DiagnosisCategory2."DELETED_FLAG" <> 'Y'
    
         LEFT JOIN DATAWAREHOUSE."DiagnosisCode" DiagnosisCode3
                   on DiagnosisCode3."DiagnosisCodeId" = "OrderExamDetail"."DiagnosisCodeId3"
                       and DiagnosisCode3."_Customer" = "OrderExamDetail"."_Customer"
                        AND DiagnosisCode3."DELETED_FLAG" <> 'Y'
    
         left join DATAWAREHOUSE."DiagnosisCategory" DiagnosisCategory3
                   on DiagnosisCategory3."DiagnosisCategoryId" = DiagnosisCode3."DiagnosisCategoryId"
                       and DiagnosisCategory3."_Customer" = DiagnosisCode3."_Customer"
                        AND DiagnosisCategory3."DELETED_FLAG" <> 'Y'
    
         LEFT JOIN DATAWAREHOUSE."DiagnosisCode" DiagnosisCode4
                   on DiagnosisCode4."DiagnosisCodeId" = "OrderExamDetail"."DiagnosisCodeId4"
                       and DiagnosisCode4."_Customer" = "OrderExamDetail"."_Customer"
                        AND DiagnosisCode4."DELETED_FLAG" <> 'Y'
    
         left join DATAWAREHOUSE."DiagnosisCategory" DiagnosisCategory4
                   on DiagnosisCategory4."DiagnosisCategoryId" = DiagnosisCode4."DiagnosisCategoryId"
                       and DiagnosisCategory4."_Customer" = DiagnosisCode4."_Customer"
                        AND DiagnosisCategory4."DELETED_FLAG" <> 'Y'
    
         LEFT JOIN DATAWAREHOUSE."DiagnosisCode" DiagnosisCode5
                   on DiagnosisCode5."DiagnosisCodeId" = "OrderExamDetail"."DiagnosisCodeId5"
                       and DiagnosisCode5."_Customer" = "OrderExamDetail"."_Customer"
                        AND DiagnosisCode5."DELETED_FLAG" <> 'Y'
    
         left join DATAWAREHOUSE."DiagnosisCategory" DiagnosisCategory5
                   on DiagnosisCategory5."DiagnosisCategoryId" = DiagnosisCode5."DiagnosisCategoryId"
                       and DiagnosisCategory5."_Customer" = DiagnosisCode5."_Customer"
                        AND DiagnosisCategory5."DELETED_FLAG" <> 'Y'
    
         LEFT JOIN DATAWAREHOUSE."DiagnosisCode" DiagnosisCode6
                   on DiagnosisCode6."DiagnosisCodeId" = "OrderExamDetail"."DiagnosisCodeId6"
                       and DiagnosisCode6."_Customer" = "OrderExamDetail"."_Customer"
                        AND DiagnosisCode6."DELETED_FLAG" <> 'Y'
    
         left join DATAWAREHOUSE."DiagnosisCategory" DiagnosisCategory6
                   on DiagnosisCategory6."DiagnosisCategoryId" = DiagnosisCode6."DiagnosisCategoryId"
                       and DiagnosisCategory6."_Customer" = DiagnosisCode6."_Customer"
                        AND DiagnosisCategory6."DELETED_FLAG" <> 'Y'
    
         LEFT JOIN DATAWAREHOUSE."DiagnosisCode" DiagnosisCode7
                   on DiagnosisCode7."DiagnosisCodeId" = "OrderExamDetail"."DiagnosisCodeId7"
                       and DiagnosisCode7."_Customer" = "OrderExamDetail"."_Customer"
                        AND DiagnosisCode7."DELETED_FLAG" <> 'Y'
    
         left join DATAWAREHOUSE."DiagnosisCategory" DiagnosisCategory7
                   on DiagnosisCategory7."DiagnosisCategoryId" = DiagnosisCode7."DiagnosisCategoryId"
                       and DiagnosisCategory7."_Customer" = DiagnosisCode7."_Customer"
                        AND DiagnosisCategory7."DELETED_FLAG" <> 'Y'
    
         LEFT JOIN DATAWAREHOUSE."DiagnosisCode" DiagnosisCode8
                   on DiagnosisCode8."DiagnosisCodeId" = "OrderExamDetail"."DiagnosisCodeId8"
                       and DiagnosisCode8."_Customer" = "OrderExamDetail"."_Customer"
                        AND DiagnosisCode8."DELETED_FLAG" <> 'Y'
    
         left join DATAWAREHOUSE."DiagnosisCategory" DiagnosisCategory8
                   on DiagnosisCategory8."DiagnosisCategoryId" = DiagnosisCode8."DiagnosisCategoryId"
                       and DiagnosisCategory8."_Customer" = DiagnosisCode8."_Customer"
                    AND DiagnosisCategory8."DELETED_FLAG" <> 'Y'
    
         LEFT JOIN DATAWAREHOUSE."DiagnosisCode" DiagnosisCode9
                   on DiagnosisCode9."DiagnosisCodeId" = "OrderExamDetail"."DiagnosisCodeId9"
                       and DiagnosisCode9."_Customer" = "OrderExamDetail"."_Customer"
                        AND DiagnosisCode9."DELETED_FLAG" <> 'Y'
    
         left join DATAWAREHOUSE."DiagnosisCategory" DiagnosisCategory9
                   on DiagnosisCategory9."DiagnosisCategoryId" = DiagnosisCode9."DiagnosisCategoryId"
                       and DiagnosisCategory9."_Customer" = DiagnosisCode9."_Customer"
                        AND DiagnosisCategory9."DELETED_FLAG" <> 'Y'
    
         LEFT JOIN DATAWAREHOUSE."DiagnosisCode" DiagnosisCode10
                   on DiagnosisCode10."DiagnosisCodeId" = "OrderExamDetail"."DiagnosisCodeId10"
                       and DiagnosisCode10."_Customer" = "OrderExamDetail"."_Customer"
                        AND DiagnosisCode10."DELETED_FLAG" <> 'Y'
    
         left join DATAWAREHOUSE."DiagnosisCategory" DiagnosisCategory10
                   on DiagnosisCategory10."DiagnosisCategoryId" = DiagnosisCode10."DiagnosisCategoryId"
                       and DiagnosisCategory10."_Customer" = DiagnosisCode10."_Customer"
                        AND DiagnosisCategory10."DELETED_FLAG" <> 'Y'
    
         LEFT JOIN DATAWAREHOUSE."DiagnosisCode" DiagnosisCode11
                   on DiagnosisCode11."DiagnosisCodeId" = "OrderExamDetail"."DiagnosisCodeId11"
                       and DiagnosisCode11."_Customer" = "OrderExamDetail"."_Customer"
                        AND DiagnosisCode11."DELETED_FLAG" <> 'Y'
    
         left join DATAWAREHOUSE."DiagnosisCategory" DiagnosisCategory11
                   on DiagnosisCategory11."DiagnosisCategoryId" = DiagnosisCode11."DiagnosisCategoryId"
                       and DiagnosisCategory11."_Customer" = DiagnosisCode11."_Customer"
                        AND DiagnosisCategory11."DELETED_FLAG" <> 'Y'
    
         LEFT JOIN DATAWAREHOUSE."DiagnosisCode" DiagnosisCode12
                   on DiagnosisCode12."DiagnosisCodeId" = "OrderExamDetail"."DiagnosisCodeId12"
                       and DiagnosisCode12."_Customer" = "OrderExamDetail"."_Customer"
                        AND DiagnosisCode12."DELETED_FLAG" <> 'Y'
    
         left join DATAWAREHOUSE."DiagnosisCategory" DiagnosisCategory12
                   on DiagnosisCategory12."DiagnosisCategoryId" = DiagnosisCode12."DiagnosisCategoryId"
                       and DiagnosisCategory12."_Customer" = DiagnosisCode12."_Customer"
                        AND DiagnosisCategory12."DELETED_FLAG" <> 'Y'
    
         LEFT JOIN DATAWAREHOUSE."InvoiceSummary"
                   on "InvoiceSummary"."OrderId" = "OrderExamDetail"."OrderId"
                       and "InvoiceSummary"."_Customer" = "OrderExamDetail"."_Customer"
                        AND "InvoiceSummary".DELETED_FLAG <> 'Y'
    
         LEFT JOIN DATAWAREHOUSE."InvoiceDetail"
                   ON "InvoiceDetail"."InvoiceSummaryId" = "InvoiceSummary"."InvoiceSummaryId"
                       and "InvoiceDetail"."_Customer" = "InvoiceSummary"."_Customer"
                        AND "InvoiceDetail".DELETED_FLAG <> 'Y'

        WHERE DATAWAREHOUSE."OrderExamDetail".DELETED_FLAG <> 'Y';

-- View: DATAMART.DashboardMartFactPatientMarketingDim_PatientInsurance
-- Columns: 5
create or replace view "DashboardMartFactPatientMarketingDim_PatientInsurance"(
	"PatientId",
	"Name",
	"CarrierKey",
	"CompanyInfoId",
	"_Customer"
) as
select distinct pi."PatientId"
              , ic."Name"
              , ic."CarrierKey"
              , p."CompanyInfoId"
              , pi."_Customer"

FROM DATAWAREHOUSE."PatientInsurance" pi

         inner join DATAWAREHOUSE."Patient" p
                    on p."PatientId" = pi."PatientId"
                        and p."_Customer" = pi."_Customer"
                        AND p.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."Plan" ip
                   on ip."PlanId" = pi."PlanId"
                       and ip."_Customer" = pi."_Customer"
                        AND ip.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."Carrier" ic
                   on ic."CarrierKey" = ip."CarrierCode"
                       and ic."_Customer" = ip."_Customer"
                    AND ic.DELETED_FLAG <> 'Y'
where pi.DELETED_FLAG <> 'Y';

-- View: DATAMART.DashboardMartFact_AccountsReceivable_Dev
-- Columns: 30
create or replace view "DashboardMartFact_AccountsReceivable_Dev"(
	"Source",
	"_Customer",
	"BalanceCategory",
	"CompanyId",
	"OfficeNum",
	"OfficeName",
	"PatientId",
	"PatientFirstName",
	"PatientLastName",
	"PatientFullName",
	"OrderNum",
	"OrderStatus",
	"OrderDate",
	"OrderBalance",
	"OrderBalanceDaysOutstanding",
	"CarrierId",
	"CarrierName",
	"PlanId",
	"PlanName",
	"ClaimId",
	"ClaimStatus",
	"ClaimLatestNote",
	"ClaimServiceDate",
	"ClaimInsuranceBalance",
	"ClaimPatientBalance",
	"ClaimTotalBalance",
	"ClaimBalanceDaysOutstanding",
	"BalanceDue",
	"BalanceType",
	"DaysOutstanding"
) as 

WITH 
	cte_ar_base AS (
      SELECT "Source",
          "_Customer",
          "BalanceCategory",
          "CompanyId",
          "OfficeNum",
          "OfficeName",
          "PatientId",
          "PatientFirstName",
          "PatientLastName",
          "PatientFullName",
          "OrderNum",
          "OrderStatus",
          "OrderDate",
          "OrderBalance",
          "OrderBalanceDaysOutstanding",
          "CarrierId",
          "CarrierName",
          "PlanId",
          "PlanName",
          "ClaimId",
          "ClaimStatus",
          "ClaimLatestNote",
          "ClaimServiceDate",
          "ClaimInsuranceBalance",
          "ClaimPatientBalance",
          "ClaimTotalBalance",
          "ClaimBalanceDaysOutstanding"
      FROM "DATAMART"."ARMartFct_POSBalanceDetails"
      
      UNION ALL
      
      SELECT
          "Source",
      	  "_Customer",
    	  "BalanceCategory",
    	  "CompanyInfoId" AS "CompanyId",
    	  "OfficeNum",
    	  "OfficeName",
    	  "PatientId",
    	  "PatientFirstName",
    	  "PatientLastName",
    	  "PatientFullName",
    	  "OrderNum",
    	  "OrderStatus",
    	  "OrderDate",
    	  "OrderBalance",
    	  "OrderBalanceDaysOutstanding",
    	  "CarrierId",
    	  "CarrierName",
    	  "PlanId",
    	  "PlanName",
    	  "ClaimId",
    	  "ClaimStatus",
    	  "ClaimLatestNote",
    	  "ClaimServiceDate",
    	  "ClaimInsuranceBalance",
    	  "ClaimPatientBalance",
    	  "ClaimTotalBalance",
    	  "ClaimBalanceDaysOutstanding"
      FROM "DATAMART"."ARMartFct_Billing"),
          
	cte_measures AS (
      SELECT "_Customer",
          "CompanyInfoId" 	AS "CompanyId",
          "OfficeKey",
          "BalanceType",
          "OrderId"			AS "OrderNum",
          "BalanceDue",
          "DaysOutstanding"
      FROM "DATAMART"."ARMartFct_MonthlyMeasures")
        
SELECT ar."Source",
	ar."_Customer",
    ar."BalanceCategory",
    ar."CompanyId",
    ar."OfficeNum",
    ar."OfficeName",
    ar."PatientId",
    ar."PatientFirstName",
    ar."PatientLastName",
    ar."PatientFullName",
    ar."OrderNum",
    ar."OrderStatus",
    ar."OrderDate",
    ar."OrderBalance",
    ar."OrderBalanceDaysOutstanding",
    ar."CarrierId",
    ar."CarrierName",
    ar."PlanId",
    ar."PlanName",
    ar."ClaimId",
    ar."ClaimStatus",
    ar."ClaimLatestNote",
    ar."ClaimServiceDate",
    ar."ClaimInsuranceBalance",
    ar."ClaimPatientBalance",
    ar."ClaimTotalBalance",
    ar."ClaimBalanceDaysOutstanding",
    m."BalanceDue",
    m."BalanceType",
    m."DaysOutstanding"
FROM cte_ar_base ar
    LEFT JOIN cte_measures m ON (
        ar."_Customer" = m."_Customer"
        	AND ar."CompanyId" = m."CompanyId"
        	AND ar."OfficeNum" = m."OfficeKey"
    		AND ar."OrderNum" = m."OrderNum");

-- View: DATAMART.DashboardMartFact_Billing
-- Columns: 39
create or replace view "DashboardMartFact_Billing"(
	"_Customer",
	"Amount",
	"DateTime",
	"BillingTransactionId",
	"MonthCloseId",
	"BillingTransactionTypeId",
	"InsuranceARDelta",
	"PatientARDelta",
	"BillingPaymentId",
	"PaidAmount",
	"IsOnHold",
	"CurrentState",
	"CurrentStateGroup",
	"IsBilled",
	"IsReadyToBill",
	"IsRejected",
	"BillingClaimDataId",
	"BillingClaimId",
	"OfficeKey",
	"PatientFirstName",
	"PatientLastName",
	"PatientName",
	"InsuredFirstName",
	"InsuredLastName",
	"InsuredName",
	"AuthorizationNumber",
	"IsCurrent",
	"PlanName",
	"CarrierId",
	"OrderId",
	"ServiceDate",
	"ServiceDate_DDMMYYYY",
	"CompanyInfoId",
	"DepositDate",
	"DepositDate_DDMMYYYY",
	"PaymentNumber",
	"Name",
	"CarrierName",
	"OfficeName"
) as
Select "BillingTransaction"."_Customer",
       ifnull("BillingTransaction"."Amount",0) as "Amount",
       "BillingTransaction"."DateTime",
       "BillingTransaction"."BillingTransactionId",
       "BillingTransaction"."MonthCloseId",
       "BillingTransaction"."BillingTransactionTypeId",
--       ifnull("BillingTransaction"."InsuranceAR",0) as "InsuranceAR",
       (IFF("BillingTransactionTypeId" = 14, - ifnull("InsuranceARDelta",0), ifnull("InsuranceARDelta",0))) AS "InsuranceARDelta",
--       ifnull("BillingTransaction"."PatientAR",0) as "PatientAR",
       ifnull("BillingTransaction"."PatientARDelta",0) as "PatientARDelta",
       "BillingTransaction"."BillingPaymentId",
       (CASE WHEN "BillingTransactionTypeId" = 2 THEN ifnull("InsuranceARDelta",0) * (-1)
             WHEN "BillingTransactionTypeId" = 3 THEN ifnull("PatientARDelta",0) * (-1)
             WHEN "BillingTransactionTypeId" = 11 THEN (ifnull("InsuranceARDelta",0) + ifnull("PatientARDelta",0)) * (-1)
             ELSE 0
           END) AS "PaidAmount",

       LOWER("BillingClaimLineItem"."CurrentState") = 'on hold' as "IsOnHold",
       "BillingClaimLineItem"."CurrentState",
       CASE WHEN trim(LOWER("BillingClaimLineItem"."CurrentState")) in ('billed', 'billed to patient', 'billed to pateint', 'billed to patient 1') THEN 'Billed'
            WHEN trim(LOWER("BillingClaimLineItem"."CurrentState")) in ('ready to bill', 'ready to bill patient', 'ready to bill patient 1') THEN 'Ready to Bill'
            ELSE "BillingClaimLineItem"."CurrentState"
       END as "CurrentStateGroup",
       trim(LOWER("BillingClaimLineItem"."CurrentState")) in ('billed', 'billed to patient', 'billed to pateint', 'billed to patient 1') as "IsBilled",
       trim(LOWER("BillingClaimLineItem"."CurrentState")) in ('ready to bill', 'ready to bill patient', 'ready to bill patient 1') as "IsReadyToBill",
       trim(LOWER("BillingClaimLineItem"."CurrentState")) = 'rejected' as "IsRejected", 
       "BillingClaimData"."BillingClaimDataId",
       "BillingClaimData"."BillingClaimId",
       "BillingClaimData"."OfficeKey",
       "BillingClaimData"."PatientFirstName",
       "BillingClaimData"."PatientLastName",
       CONCAT("BillingClaimData"."PatientLastName", ', ', "BillingClaimData"."PatientFirstName") as "PatientName",
       "BillingClaimData"."InsuredFirstName",
       "BillingClaimData"."InsuredLastName",
       CONCAT("BillingClaimData"."InsuredLastName",', ', "BillingClaimData"."InsuredFirstName") as "InsuredName",
--       "BillingClaimData"."InsuredId",
       "BillingClaimData"."AuthorizationNumber",
       "BillingClaimData"."IsCurrent",
--       "BillingClaimData"."RenderingProviderFirstName",
--       "BillingClaimData"."RenderingProviderLastName",
       "BillingClaimData"."PlanName",
--       "BillingClaimData"."PlanId",
       ltrim(rtrim("BillingClaimData"."CarrierKey")) as "CarrierId",
       "BillingClaimOrder"."OrderId",
       "BillingClaim"."ServiceDate",
        IFF("BillingClaim"."ServiceDate"::date is null, '', to_varchar("BillingClaim"."ServiceDate"::date, 'DD/MM/YYYY')) as "ServiceDate_DDMMYYYY",
--       "BillingClaim"."ExternalClaimNumber" as "ExternalClaimNum",
       "BillingClaim"."CompanyInfoId",
       "BillingPayment"."DepositDate",
       IFF("BillingPayment"."DepositDate"::date is null, '', to_varchar("BillingPayment"."DepositDate"::date, 'DD/MM/YYYY')) as "DepositDate_DDMMYYYY",
       "BillingPayment"."Number" as "PaymentNumber",
       "User"."Name",
       "Carrier"."Name" as "CarrierName",
       "Office"."Name" as "OfficeName"

From DATAWAREHOUSE."BillingTransaction"
         Left Join DATAWAREHOUSE."BillingClaimLineItem" on "BillingTransaction"."BillingClaimLineItemId" = "BillingClaimLineItem"."BillingClaimLineItemId"
    and  "BillingClaimLineItem"."_Customer" = "BillingTransaction"."_Customer"
    AND "BillingClaimLineItem".DELETED_FLAG <> 'Y'
         left join DATAWAREHOUSE."BillingClaimData" on "BillingClaimLineItem"."BillingClaimId" = "BillingClaimData"."BillingClaimId"
    AND "BillingClaimData"."IsCurrent" <> 'false'
    AND "BillingClaimData"."_Customer" = "BillingTransaction"."_Customer"
    AND "BillingClaimData".DELETED_FLAG <> 'Y'
         left join DATAWAREHOUSE."BillingClaimOrder" on "BillingClaimData"."BillingClaimId" = "BillingClaimOrder"."BillingClaimId"
    AND "BillingClaimOrder"."_Customer" = "BillingTransaction"."_Customer"
    AND "BillingClaimOrder".DELETED_FLAG <> 'Y'
         left join DATAWAREHOUSE."BillingClaim" on "BillingClaimData"."BillingClaimId" = "BillingClaim"."BillingClaimId"
    AND "BillingClaim"."_Customer" = "BillingTransaction"."_Customer"
    AND "BillingClaim".DELETED_FLAG <> 'Y'
         left join DATAWAREHOUSE."Office" on "BillingClaimData"."OfficeKey" = "Office"."OfficeKey"
    AND "Office"."_Customer" = "BillingTransaction"."_Customer"
    AND "Office".DELETED_FLAG <> 'Y'
         left join DATAWAREHOUSE."BillingPayment" on "BillingTransaction"."BillingPaymentId" = "BillingPayment"."BillingPaymentId"
    AND "BillingPayment"."_Customer" = "BillingTransaction"."_Customer"
    AND "BillingPayment".DELETED_FLAG <> 'Y'
         left join DATAWAREHOUSE."WorkflowActivity" on  "BillingTransaction"."WorkflowActivityId" = "WorkflowActivity"."WorkflowActivityId"
    AND "BillingTransaction"."_Customer" = "WorkflowActivity"."_Customer"
    AND "WorkflowActivity".DELETED_FLAG <> 'Y'
         left join DATAWAREHOUSE."WorkflowEvent" on "WorkflowEvent"."WorkflowEventId" = "WorkflowActivity"."WorkflowEventId"
    AND "WorkflowEvent"."_Customer" = "WorkflowActivity"."_Customer"
    AND "WorkflowEvent".DELETED_FLAG <> 'Y'
         left join DATAWAREHOUSE."User" on "User"."UserId" = "WorkflowEvent"."UserId"
    AND "User"."_Customer" = "WorkflowEvent"."_Customer"
    AND "User".DELETED_FLAG <> 'Y'
         Left Join DATAWAREHOUSE."Carrier" on "BillingClaimData"."CarrierKey" = "Carrier"."CarrierKey"
    AND "Carrier"."_Customer" = "BillingClaimData"."_Customer"
    AND "Carrier".DELETED_FLAG <> 'Y'
WHERE YEAR("BillingClaim"."ServiceDate") > YEAR(GetDate()) - 6
AND DATAWAREHOUSE."BillingTransaction".DELETED_FLAG <> 'Y'
--   AND DATAWAREHOUSE."BillingClaimLineItem".DELETED_FLAG <> 'Y'
--   AND DATAWAREHOUSE."BillingClaimData".DELETED_FLAG <> 'Y'
--   AND DATAWAREHOUSE."BillingClaimOrder".DELETED_FLAG <> 'Y'
--   AND DATAWAREHOUSE."BillingClaim".DELETED_FLAG <> 'Y'
--   AND DATAWAREHOUSE."Office".DELETED_FLAG <> 'Y'
--   AND DATAWAREHOUSE."WorkflowActivity".DELETED_FLAG <> 'Y'
--   AND DATAWAREHOUSE."User".DELETED_FLAG <> 'Y'
--   AND DATAWAREHOUSE."WorkflowEvent".DELETED_FLAG <> 'Y'
--   AND DATAWAREHOUSE."Carrier".DELETED_FLAG <> 'Y'
;

-- View: DATAMART.DashboardMartFact_BillingClaimAging
-- Columns: 15
create or replace view "DashboardMartFact_BillingClaimAging"(
	AGE,
	CUTOFFDATE,
	ARAMOUNT,
	"CompanyInfoId",
	"OfficeKey",
	"Office_Name",
	"BillingClaimId",
	"PatientFirstName",
	"PatientLastName",
	"PatientName",
	"CarrierName",
	"PlanName",
	"ServiceDate",
	"ServiceDate_DDMMYYYY",
	"_Customer"
) as
SELECT 
    CASE
        WHEN "ServiceDate" >= CAST(CONCAT(DATE_PART(year, CURRENT_DATE), '-', DATE_PART(month, CURRENT_DATE), '-01') AS DATE) THEN 'Current'
        WHEN "ServiceDate" >= DATEADD(MONTH, -1, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
            AND "ServiceDate" < CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE) THEN 'Last Month'
        WHEN "ServiceDate" >= DATEADD(MONTH, -2, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
            AND "ServiceDate" < DATEADD(MONTH, -1, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN '2 Months Ago'
        WHEN "ServiceDate" >= DATEADD(MONTH, -3, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
            AND "ServiceDate" < DATEADD(MONTH, -2, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN '3 Months Ago'
        WHEN "ServiceDate" >= DATEADD(MONTH, -4, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
            AND "ServiceDate" < DATEADD(MONTH, -3, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN '4 Months Ago'
        ELSE '> 4 Months'
        END AS age,

    CASE -- NEED SAMPLE OF WHAT IT DOES IN TABLEAU
        WHEN "ServiceDate" >= CAST(CONCAT(DATE_PART(year, CURRENT_DATE), '-', DATE_PART(month, CURRENT_DATE), '-01') AS DATE) THEN DATE_TRUNC(month,  CURRENT_DATE)
        WHEN "ServiceDate" >= DATEADD(MONTH, -1, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
            AND "ServiceDate" < CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE) THEN DATE_TRUNC(month, DATEADD(MONTH, -1, CURRENT_DATE))
        WHEN "ServiceDate" >= DATEADD(MONTH, -2, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
            AND "ServiceDate" < DATEADD(MONTH, -1, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN DATE_TRUNC(month, DATEADD(MONTH, -2, CURRENT_DATE))
        WHEN "ServiceDate" >= DATEADD(MONTH, -3, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
            AND "ServiceDate" < DATEADD(MONTH, -2, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN DATE_TRUNC(month, DATEADD(MONTH, -3, CURRENT_DATE))
        WHEN "ServiceDate" >= DATEADD(MONTH, -4, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
            AND "ServiceDate" < DATEADD(MONTH, -3, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN DATE_TRUNC(month, DATEADD(MONTH, -4, CURRENT_DATE))
        ELSE LAST_DAY(DATEADD(MONTH, -4, CURRENT_DATE))
        END AS CutoffDate,
    
--     CASE
--         WHEN "ServiceDate" >= CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE) THEN 1
--         WHEN "ServiceDate" >= DATEADD(MONTH, -1, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
--             AND "ServiceDate" < CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE) THEN 2
--         WHEN "ServiceDate" >= DATEADD(MONTH, -2, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
--             AND "ServiceDate" < DATEADD(MONTH, -1, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN 3
--         WHEN "ServiceDate" >= DATEADD(MONTH, -3, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
--             AND "ServiceDate" < DATEADD(MONTH, -2, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN 4
--         WHEN "ServiceDate" >= DATEADD(MONTH, -4, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
--             AND "ServiceDate" < DATEADD(MONTH, -3, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN 5
--         ELSE 6
--         END AS ageOrder,
    SUM(ifnull("TotalBalance",0)) AS aramount,
    "CompanyInfoId",
    "OfficeKey",
    "Office_Name",
--    "PatientId",
    "BillingClaimId",
--    "CarrierKey",
--    "PlanId",
    "PatientFirstName",
    "PatientLastName",
    "PatientLastName" || ', ' || "PatientFirstName" as "PatientName",
    "CarrierName",
    "PlanName",
    "ServiceDate",
    to_varchar("ServiceDate"::date, 'DD/MM/YYYY') as "ServiceDate_DDMMYYYY",
    "_Customer"
FROM
    (SELECT sum(BillingTransaction_HUB."InsuranceAR" + BillingTransaction_HUB."PatientAR") as "TotalBalance",
            "BillingClaim_HUB"."ServiceDate",
            "BillingClaim_HUB"."CompanyInfoId",
            "BillingClaimData_HUB"."OfficeKey",
            "Office_HUB"."Name" as "Office_Name",
            "BillingClaim_HUB"."PatientId",
            "BillingClaimData_HUB"."BillingClaimId",
            "BillingClaimData_HUB"."CarrierKey",
            "BillingClaimData_HUB"."PlanId",
            "BillingClaimData_HUB"."PatientFirstName",
            "BillingClaimData_HUB"."PatientLastName",
            "Carrier_HUB"."Name" as "CarrierName",
            "Plan"."Name" as "PlanName",
            BillingTransaction_HUB."_Customer"
     FROM DATAWAREHOUSE."BillingTransaction_HUB" BillingTransaction_HUB
              join DATAWAREHOUSE."BillingClaimLineItem"
                   on BillingTransaction_HUB."BillingClaimLineItemId" = "BillingClaimLineItem"."BillingClaimLineItemId"
                       AND ifnull(BillingTransaction_HUB."InsuranceAR",0) + ifnull(BillingTransaction_HUB."PatientAR",0) <> 0
                       AND "DateTime" < dateadd(d, 1, CURRENT_TIMESTAMP(2))
                       AND "BillingClaimLineItem"."_Customer" = BillingTransaction_HUB."_Customer"
                        AND "BillingClaimLineItem"."DELETED_FLAG" <> 'Y'
              join DATAWAREHOUSE."BillingClaim_HUB"
                   on "BillingClaimLineItem"."BillingClaimId" = "BillingClaim_HUB"."BillingClaimId"
                       AND "ServiceDate" < DATEADD(d, 1, CURRENT_TIMESTAMP(2))
                       AND "BillingClaim_HUB"."_Customer" = "BillingClaimLineItem"."_Customer"
                        AND "BillingClaim_HUB"."DELETED_FLAG" <> 'Y'
              join DATAWAREHOUSE."BillingClaimData_HUB"
                   on "BillingClaim_HUB"."BillingClaimId" = "BillingClaimData_HUB"."BillingClaimId"
                       AND "BillingClaimData_HUB"."_Customer" = "BillingClaim_HUB"."_Customer"
                        AND "BillingClaimData_HUB"."DELETED_FLAG" <> 'Y'
              join DATAWAREHOUSE."Carrier_HUB"
                   on "BillingClaimData_HUB"."CarrierKey" = "Carrier_HUB"."CarrierKey"
                       AND "Carrier_HUB"."IsPrepaid" = 'false'
                       AND "Carrier_HUB"."_Customer" = "BillingClaimData_HUB"."_Customer"
                        AND "Carrier_HUB"."DELETED_FLAG" <> 'Y'
              join DATAWAREHOUSE."Plan"
                   on "BillingClaimData_HUB"."PlanId" = "Plan"."PlanId"
                       AND "Plan"."_Customer" = "BillingClaimData_HUB"."_Customer"
                        AND "Plan"."DELETED_FLAG" <> 'Y'
              join DATAWAREHOUSE."Office_HUB"
                   on DATAWAREHOUSE."Office_HUB"."OfficeKey" = DATAWAREHOUSE."BillingClaimData_HUB"."OfficeKey"
                       and DATAWAREHOUSE."Office_HUB"."_Customer" = "BillingClaimData_HUB"."_Customer"
                        AND DATAWAREHOUSE."Office_HUB"."DELETED_FLAG" <> 'Y'
     WHERE
         "BillingClaimData_HUB"."IsCurrent" = 'true'
       AND BillingTransaction_HUB."BillingTransactionId" =
           (select max(bt."BillingTransactionId") from DATAWAREHOUSE."BillingTransaction_HUB" bt where bt."BillingClaimLineItemId"  = BillingTransaction_HUB."BillingClaimLineItemId" and bt."DateTime" < dateadd(d,1,CURRENT_DATE ))
       AND YEAR("ServiceDate") > YEAR(GetDate()) - 6
       AND BillingTransaction_HUB.DELETED_FLAG <> 'Y'
--        AND DATAWAREHOUSE."BillingClaimLineItem".DELETED_FLAG <> 'Y'
--        AND DATAWAREHOUSE."BillingClaim_HUB".DELETED_FLAG <> 'Y'
--        AND DATAWAREHOUSE."BillingClaimData_HUB".DELETED_FLAG <> 'Y'
--        AND DATAWAREHOUSE."Carrier_HUB".DELETED_FLAG <> 'Y'
--        AND DATAWAREHOUSE."Plan".DELETED_FLAG <> 'Y'
--        AND DATAWAREHOUSE."Office_HUB".DELETED_FLAG <> 'Y'
     group by
         BillingTransaction_HUB."_Customer",
         "BillingClaimData_HUB"."CarrierKey",
         "Carrier_HUB"."Name",
         "BillingClaimData_HUB"."PlanId",
         "Plan"."Name",
         "BillingClaimData_HUB"."OfficeKey",
         "Office_HUB"."Name",
         "BillingClaimData_HUB"."BillingClaimId",
         "BillingClaim_HUB"."PatientId",
         "BillingClaimData_HUB"."PatientFirstName",
         "BillingClaimData_HUB"."PatientLastName",
         "BillingClaim_HUB"."ServiceDate",
         "BillingClaim_HUB"."CompanyInfoId"
     having sum(ifnull(BillingTransaction_HUB."InsuranceAR",0) + ifnull(BillingTransaction_HUB."PatientAR",0)) <> 0) BillingClaimAging


GROUP BY
    "CompanyInfoId",
    "OfficeKey",
    "Office_Name",
    "PatientId",
    "PatientFirstName",
    "PatientLastName",
    "PlanId",
    "PlanName",
    "BillingClaimId",
    "CarrierName",
    "CarrierKey",
    "ServiceDate",
    "_Customer",
    CASE
        WHEN "ServiceDate" >= CAST(CONCAT(DATE_PART(year, CURRENT_DATE), '-', DATE_PART(month, CURRENT_DATE), '-01') AS DATE) THEN 'Current'
        WHEN "ServiceDate" >= DATEADD(MONTH, -1, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
            AND "ServiceDate" < CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE) THEN 'Last Month'
        WHEN "ServiceDate" >= DATEADD(MONTH, -2, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
            AND "ServiceDate" < DATEADD(MONTH, -1, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN '2 Months Ago'
        WHEN "ServiceDate" >= DATEADD(MONTH, -3, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
            AND "ServiceDate" < DATEADD(MONTH, -2, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN '3 Months Ago'
        WHEN "ServiceDate" >= DATEADD(MONTH, -4, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
            AND "ServiceDate" < DATEADD(MONTH, -3, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN '4 Months Ago'
        ELSE '> 4 Months'
        END;
--     ,CASE
--         WHEN "ServiceDate" >= CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE) THEN 1
--         WHEN "ServiceDate" >= DATEADD(MONTH, -1, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
--             AND "ServiceDate" < CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE) THEN 2
--         WHEN "ServiceDate" >= DATEADD(MONTH, -2, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
--             AND "ServiceDate" < DATEADD(MONTH, -1, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN 3
--         WHEN "ServiceDate" >= DATEADD(MONTH, -3, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
--             AND "ServiceDate" < DATEADD(MONTH, -2, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN 4
--         WHEN "ServiceDate" >= DATEADD(MONTH, -4, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
--             AND "ServiceDate" < DATEADD(MONTH, -3, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN 5
--         ELSE 6
--         END
-- ORDER BY
--     CASE
--         WHEN "ServiceDate" >= CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE) THEN 1
--         WHEN "ServiceDate" >= DATEADD(MONTH, -1, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
--             AND "ServiceDate" < CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE) THEN 2
--         WHEN "ServiceDate" >= DATEADD(MONTH, -2, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
--             AND "ServiceDate" < DATEADD(MONTH, -1, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN 3
--         WHEN "ServiceDate" >= DATEADD(MONTH, -3, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
--             AND "ServiceDate" < DATEADD(MONTH, -2, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN 4
--         WHEN "ServiceDate" >= DATEADD(MONTH, -4, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE))
--             AND "ServiceDate" < DATEADD(MONTH, -3, CAST(CONCAT(DATE_PART(YEAR, CURRENT_DATE), '-', DATE_PART(MONTH, CURRENT_DATE), '-01') AS DATE)) THEN 5
--         ELSE 6
--         END;

-- View: DATAMART.DashboardMartFact_NetCollections
-- Columns: 27
create or replace view "DashboardMartFact_NetCollections"(
	"Billing Id",
	"Carrier Key",
	"_Customer",
	"Company Id",
	"Transaction Date",
	"Deposit Date",
	"First Payment Date",
	"Fully Paid Date",
	"Order Date",
	"Item Type",
	"Item Type Grouping",
	"Order Type Grouping",
	"Item Id",
	"CPT Code",
	"OfficeKey",
	"Location",
	"Order Id",
	"Patient Id",
	"Patient",
	"Staff",
	"Doctor",
	"Source",
	"Transaction Type",
	"Payment Type",
	"View Type",
	"Payment Amount",
	"Tax"
) as
SELECT
    "Billing Id",
    "Carrier Key",
	"_Customer",
    "Company Id",
    "Transaction Date",
    "Deposit Date",
    "First Payment Date",
    "Fully Paid Date",
    "Order Date",
    "Item Type",
    "Item Type Grouping",
    "Order Type Grouping",
    "Item Id",
    "CPT Code",
    "OfficeKey",
    "Location",
    "Order Id",
    "Patient Id",
    "Patient",
    "Staff",
    "Doctor",
    "Source",
    "Transaction Type",
    "Payment Type",
    "View Type",
    "Payment Amount",
    "Tax"
FROM(
	-- Billing source (Commissions and Accounting View)
	SELECT
		c."BillingPaymentId"           AS "Billing Id",
		c."CarrierKey"                 AS "Carrier Key",
		c."CompanyId"                  AS "Company Id",
		c."BillingTransactionDateTime" AS "Transaction Date",
		IFNULL(p."DepositDate", c."BillingTransactionDateTime")               AS "Deposit Date",
		c."Order_DateTimeFirstPayment" AS "First Payment Date",
		c."Order_DateTimeFullyPaid"    AS "Fully Paid Date",
		c."Order_DateTime"             AS "Order Date",
		c."ItemTypeDescription"        AS "Item Type",
		c."ItemTypeGrouping"           AS "Item Type Grouping",
		c."OrderTypeGrouping"          AS "Order Type Grouping",
		c."ItemId"                     AS "Item Id",
		CASE 
			WHEN c."ItemTypeId" = 6 AND bld."ProcedureCode" IS NOT NULL AND bld."ProcedureCode" <> '' THEN bld."ProcedureCode" 
			WHEN c."ItemTypeId" = 6 AND c."CPTCode" IS NOT NULL AND c."CPTCode" <> '' THEN c."CPTCode"
			ELSE 'Unassigned'
		END                            AS "CPT Code",
  		c."OfficeKey"				   AS "OfficeKey",
		IFF(c."OfficeKey" IS NOT NULL AND c."Office_Name" IS NOT NULL, c."OfficeKey" || ' - ' || c."Office_Name", 'Unassigned')                AS "Location",
		c."OrderId"                    AS "Order Id",
		c."PatientId"                  AS "Patient Id",
		IFNULL(c."Patient_FullName"
			   ,'Unassigned')          AS "Patient",
		c."_Customer",
		IFNULL(c."Employee_FullName"
			   ,'Unassigned')          AS "Staff",
		IFNULL(c."Doctor_FullName"
			   ,'Unassigned')          AS "Doctor",
		c."Collections_Source"         AS "Source",
		c."BillingTransactionType_Description" AS "Transaction Type",
		c."Payment_Source"             AS "Payment Type",
	 'Both'                 AS "View Type",
		c."BillingTransactionAmount"   AS "Payment Amount",
		0                              AS "Tax"
	FROM DATAMART."BillingMartFact_Collections" c
	LEFT JOIN DATAMART."BillingMartDim_BillingPayment" p on c."BillingPaymentId" = p."BillingPaymentId" and c."_Customer" = p."_Customer"
	LEFT JOIN  DATAMART."Reference_Order" o on o."OrderId"=c."OrderId" and o."_Customer"=c."_Customer"
	LEFT JOIN  DATAMART."BillingMartDim_BillingLineDetail" bld on bld."BillingClaimLineItem_ItemId"=c."ItemId" 
		and c."ItemTypeId" = 6
		and bld."IsCurrent" = 'true'
		and bld."BillingClaimId" = c."BillingClaimId"
		and bld."BillingClaimLineItem_BillingClaimLineItemId" = c."BillingClaimLineItemId"
		and bld."_Customer"=c."_Customer"
	WHERE (
		   o."DateTime" >= CURRENT_DATE - INTERVAL '2 years'
		   or c."BillingTransactionDateTime" >= CURRENT_DATE - INTERVAL '2 years'
		   or p."DepositDate" >= CURRENT_DATE - INTERVAL '2 years'
		   )

	UNION ALL
  
	-- Billing source - External Claims (Accounting View)
	SELECT
		c."BillingPaymentId"           AS "Billing Id",
		c."CarrierKey"                 AS "Carrier Key",
		c."CompanyInfoId"              AS "Company Id",
		c."BillingTransactionDateTime" AS "Transaction Date",
		c."DepositDate"                AS "Deposit Date",
		NULL                           AS "First Payment Date",
		NULL                           AS "Fully Paid Date",
		NULL                           AS "Order Date",
		'Unassigned'                   AS "Item Type",
		'Uncategorized'                AS "Item Type Grouping",
		'Uncategorized'                AS "Order Type Grouping",
		NULL                           AS "Item Id",
		'Unassigned'                   AS "CPT Code",
  		'Unassigned'                   AS "OfficeKey",
		'Unassigned'                   AS "Location",
		c."OrderId"                    AS "Order Id",
		NULL                           AS "Patient Id",
		'Unassigned'                   AS "Patient",
		c."_Customer",
		'Unassigned'                   AS "Staff",
		'Unassigned'                   AS "Doctor",
		c."Collections_Source"         AS "Source",
		c."BillingTransactionType_Description" AS "Transaction Type",
		c."Payment_Source"             AS "Payment Type",
	    'Accounting View'                   AS "View Type",
		c."BillingTransactionAmount"   AS "Payment Amount",
		0                              AS "Tax"
	from DATAMART."BillingMartFact_CollectionsExternalClaims" c
  	WHERE c."BillingTransactionDateTime" >= CURRENT_DATE - INTERVAL '2 years'
		or c."DepositDate" >= CURRENT_DATE - INTERVAL '2 years'

	UNION ALL

	-- POS source - Uncategorized orders (Misc as both view types, non-misc as Accounting View)
	SELECT
		NULL                           AS "Billing Id",
		NULL                           AS "Carrier Key",
		ofc."CompanyInfoId"            AS "Company Id",
		COALESCE(pt."PosTransaction_DateTime",o."Order_DateTime") AS "Transaction Date",
		COALESCE(pt."PosTransaction_DateTime",o."Order_DateTime") AS "Deposit Date",
		o."Order_DateTimeFirstPayment" AS "First Payment Date",
		o."Order_DateTimeFullyPaid"    AS "Fully Paid Date",
		o."Order_DateTime"             AS "Order Date",
		pt."ItemTypeDescription"       AS "Item Type",
		pt."ItemTypeGrouping"          AS "Item Type Grouping",
		pt."OrderTypeGrouping"         AS "Order Type Grouping",
		NULL                           AS "ItemId",
		'Unassigned'                   AS "CPT Code",
  		ofc."OfficeKey"				   AS "OfficeKey",
		IFF(ofc."OfficeKey" IS NOT NULL AND  ofc."Name" IS NOT NULL, ofc."OfficeKey" || ' - ' || ofc."Name", 'Unassigned')                AS "Location",
		pt."OrderId"                   AS "Order Id",
		pt."PatientId"                 AS "Patient Id",
		IFNULL(pt."Patient_FullName"
			   ,'Unassigned')          AS "Patient",
		pt."_Customer",
		IFNULL(e."FullName"
			   ,'Unassigned')          AS "Staff",
		IFNULL(o."DoctorEmployee_FullName"
			   ,'Unassigned')          AS "Doctor",
		"Collections_Source"           AS "Source",
		pt."PosTransactionType_Description" AS "Transaction Type",
		"Payment_Source"               AS "Payment Type",
        CASE 
            WHEN pt."PosTransactionTypeId" = 11 and pt."MiscPaymentReason" not in ('Rejected Insurance Claim','Patient Payment','Billing Credit Transfer') THEN 'Both'
            WHEN pt."PosPaymentId" IS NOT NULL AND pt."PosTransactionTypeId" <> 11 THEN 'Accounting View'
            ELSE 'Other' 
        END AS "View Type",
		pt."Amount"                    AS "Payment Amount",
		0                              AS "Tax"
	FROM DATAMART."PosMartFact_CollectionsNet_PosTransactions" pt
	LEFT JOIN DATAMART."PosMartDim_Collections_PosTransactions_Order" o  on pt."OrderId" = o."OrderId"
																						 and pt."_Customer" = o."_Customer"
	LEFT JOIN DATAMART."Reference_Office" ofc on coalesce(o."Order_OfficeKey",pt."OfficeKey") = ofc."OfficeKey" 
															   and coalesce(o."_Customer",pt."_Customer")       = ofc."_Customer"
	LEFT JOIN DATAMART."Reference_Employee" e on pt."EmployeeId" = e."EmployeeId"
	and pt."_Customer" = e."_Customer"
	WHERE (
		   o."Order_DateTime" >= CURRENT_DATE - INTERVAL '2 years'
		   or pt."PosTransaction_DateTime" >= CURRENT_DATE - INTERVAL '2 years'
		   )
	AND (
        (pt."PosTransactionTypeId" = 11 and pt."MiscPaymentReason" not in ('Rejected Insurance Claim','Patient Payment','Billing Credit Transfer'))
        OR (pt."PosPaymentId" IS NOT NULL AND pt."PosTransactionTypeId" <> 11)
    )
	 
	UNION ALL

	-- POS source - Itemized orders (Commissions and Accounting View)
	SELECT
		NULL                         AS "Billing Id",
		NULL                         AS "Carrier Key",
		"CompanyInfoId"              AS "Company Id",
		"CollectionsDateTime"        AS "Transaction Date",
		"CollectionsDateTime"        AS "Deposit Date",
		"Order_DateTimeFirstPayment" AS "First Payment Date",
		"Order_DateTimeFullyPaid"    AS "Fully Paid Date",
		"Order_DateTime"             AS "Order Date",
		c."ItemType_Description"     AS "Item Type",
		c."ItemTypeGrouping"         AS "Item Type Grouping",
		c."OrderTypeGrouping"        AS "Order Type Grouping",
		c."ItemId"                   AS "Item Id",
		IFNULL(
			CASE WHEN c."ItemTypeId" = 6 THEN c."CPTCode" ELSE 'Unassigned' END
			,'Unassigned'
		)                            AS "CPT Code",
  		c."OfficeKey"				 AS "OfficeKey",
		IFF(c."OfficeKey" IS NOT NULL AND c."Office_Name" IS NOT NULL, c."OfficeKey" || ' - ' || c."Office_Name", 'Unassigned')                AS "Location",
		"OrderId"                    AS "Order Id",
		"PatientId"                  AS "Patient Id",
		IFNULL(c."Patient_FullName"
			   ,'Unassigned')        AS "Patient",
		c."_Customer",
		IFNULL(c."Employee_FullName"
			   ,'Unassigned')        AS "Staff",
		IFNULL(c."Doctor_FullName"
			   ,'Unassigned')        AS "Doctor",
		"Collections_Source"         AS "Source",
		"TransactionTypeDescription" AS "Transaction Type",
		"Payment_Source"             AS "Payment Type",
		'Both'            AS "View Type",
		"Amount"                     AS "Payment Amount",
		"Tax"
	FROM DATAMART."PosMartFact_Collections_ItemTypeAggregate" c
	LEFT JOIN  DATAMART."Reference_Item" i on i."ItemId"=c."ItemId" and i."_Customer"=c."_Customer"
		and c."ItemTypeId" = 6
	WHERE "Order_DateTime" >= CURRENT_DATE - INTERVAL '2 years' 
	OR "CollectionsDateTime" >= CURRENT_DATE - INTERVAL '2 years'
);

-- View: DATAMART.DashboardMartFact_PatientDemographics
-- Columns: 44
create or replace view "DashboardMartFact_PatientDemographics"(
	"PatientId",
	"CompanyInfoId",
	"PatientName",
	"NickName",
	"ReferralTypeId",
	"IsPatient",
	"ProviderEmployeeId",
	"Sex",
	"Email",
	"EmailType",
	"HomeOfficeKey",
	"IsAddressBad",
	"IsEmailBad",
	"IsInActive",
	"LastExamDate",
	"BirthDate",
	"ResponsiblePartyFullName",
	"_Customer",
	"OrderId",
	"OfficeKey",
	"DateTime",
	"InvoiceSummaryId",
	"Address_Line1",
	"Address_Line2",
	"Address_City",
	"Address_State",
	"Address_ZipCode",
	"IsPrimaryPhone",
	"Phone_Number",
	"Phone_Extension",
	"ReferralType",
	"CarrierName",
	"PlanName",
	"CarrierKey",
	"IsPrimaryInsurance",
	"FrameCollectionId",
	"FrameCollection_Description",
	"NextPatientRecall_Date",
	"NextPatientRecallType_Description",
	"Office_Name",
	"Is Referral",
	"Is Carrier",
	"OrderCarrier",
	"ActiveStatus"
) as
Select distinct "Patient"."PatientId",
                "Patient"."CompanyInfoId",
--                "Patient"."FirstName",
--                "Patient"."LastName",
                "Patient"."LastName" || ', ' || "Patient"."FirstName" as "PatientName",
                "Patient"."NickName",
                "Patient"."ReferralTypeId",
                "Patient"."IsPatient",
                "Patient"."ProviderEmployeeId",
                "Patient"."Sex",
                IFNULL("Patient"."Email_01",'')                                            as "Email",
                (CASE
                     WHEN "Patient"."Email_01" = '' OR "Patient"."Email_01" IS NULL THEN 'No'
                     WHEN "Patient"."Email_01" != '' THEN 'Yes' END
                    )                                                                  AS "EmailType",
                "Patient"."HomeOfficeKey",
                "Patient"."IsAddressBad",
                "Patient"."IsEmailBad",
                "Patient"."IsInActive",
                "Patient"."LastExamDate",
                try_cast("Patient"."BirthDate" as Date) as "BirthDate",
                concat(responsibleParty."FirstName", ' ', responsibleParty."LastName") as "ResponsiblePartyFullName",
                "Patient"."_Customer",
                "PosTransaction"."OrderId",
                "PosTransaction"."OfficeKey",
                "PosTransaction"."DateTime",
                "PosTransaction"."InvoiceSummaryId",
                "Reference_PatientAndAddress"."Address_Line1",
                "Reference_PatientAndAddress"."Address_Line2",
                "Reference_PatientAndAddress"."Address_City",
                "Reference_PatientAndAddress"."Address_State",
                "Reference_PatientAndAddress"."Address_ZipCode",
                "Phone"."IsPrimary"                                                    as "IsPrimaryPhone",
                "Phone"."Number"                                                       as "Phone_Number",    -- added
                "Phone"."Extension"                                                    as "Phone_Extension", -- added
                "ReferralType"."Value"                                                 as "ReferralType",
                "OrderInsurance"."CarrierName",
                "OrderInsurance"."PlanName",
                "OrderInsurance"."CarrierKey",
                "InvoiceInsuranceDetail"."IsPrimary" as "IsPrimaryInsurance",
                "FrameStyle"."FrameCollectionId",
                "FrameCollection"."Description"                                        as "FrameCollection_Description",
                "PatientRecall"."Date"               as "NextPatientRecall_Date",
                "PatientRecallType"."Description"    as "NextPatientRecallType_Description",
                "Office"."Name"                      as "Office_Name",
                IFF(lower("ReferralType"."Value") = 'non referral' OR "ReferralType"."Value" is null, 
                    'Non-Referral',
                    'Referral')                      as "Is Referral",
                IFF("OrderInsurance"."CarrierName" is null,'Non-Carrier', 'Carrier') as "Is Carrier",
                IFF("OrderInsurance"."CarrierName" is null, 
                    TO_VARCHAR("PosTransaction"."OrderId"), 
                    CONCAT(TO_VARCHAR("PosTransaction"."OrderId"),"OrderInsurance"."CarrierName")) as "OrderCarrier",
                IFF("Patient"."IsInActive" is null OR "Patient"."IsInActive" = 'false', 'Inactive', 'Active') as "ActiveStatus"

From DATAWAREHOUSE."Patient"

         left join DATAWAREHOUSE."ReferralType"
                   on "Patient"."ReferralTypeId" = "ReferralType"."ReferralTypeId"
                       and "Patient"."_Customer" = "ReferralType"."_Customer"
                        AND "ReferralType".DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."Patient" responsibleParty
                   on "Patient"."ResponsiblePatientId" = responsibleParty."PatientId"
                       and "Patient"."_Customer" = responsibleParty."_Customer"
                        AND responsibleParty.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."PosTransaction"
                   on "PosTransaction"."PatientId" = "Patient"."PatientId"
                       and ("PosTransactionTypeId" is null OR "PosTransactionTypeId" = 1)
                       and "PosTransaction"."_Customer" = "Patient"."_Customer"
                        AND "PosTransaction".DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."Office"
                   on "Office"."OfficeKey" = "PosTransaction"."OfficeKey"
                       and "Office"."_Customer" = "PosTransaction"."_Customer"
                        AND "Office".DELETED_FLAG <> 'Y'    
    
         left join DATAMART."Reference_PatientAndAddress"
                   on "Patient"."PatientId" = "Reference_PatientAndAddress"."PatientId"
                       and "Address_IsPrimary" = 'true'
                       and "Patient"."_Customer" = "Reference_PatientAndAddress"."_Customer"
                        AND "Reference_PatientAndAddress".DELETED_FLAG <> 'Y'

         left join DATAWAREHOUSE."PatientPhone"
                   on "Patient"."PatientId" = "PatientPhone"."PatientId"
                       and "Patient"."_Customer" = "PatientPhone"."_Customer"
                        AND "PatientPhone".DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."Phone"
                   on "PatientPhone"."PhoneId" = "Phone"."PhoneId"
                       and "PatientPhone"."_Customer" = "Phone"."_Customer"
                        AND "Phone".DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."InvoiceSummary"
                   on "PosTransaction"."InvoiceSummaryId" = "InvoiceSummary"."InvoiceSummaryId"
                       and "PosTransaction"."_Customer" = "InvoiceSummary"."_Customer"
                        AND "InvoiceSummary".DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."InvoiceDetail"
                   on "InvoiceSummary"."InvoiceSummaryId" = "InvoiceDetail"."InvoiceSummaryId"
                       and "InvoiceSummary"."_Customer" = "InvoiceDetail"."_Customer"
                        AND "InvoiceDetail".DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."InvoiceInsuranceDetail" -- this adds time
                   on "InvoiceDetail"."InvoiceDetailId" = "InvoiceInsuranceDetail"."InvoiceDetailId"
                       and "InvoiceDetail"."_Customer" = "InvoiceInsuranceDetail"."_Customer"
                        AND "InvoiceInsuranceDetail".DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."OrderInsurance"
                   on "InvoiceInsuranceDetail"."OrderInsuranceId" = "OrderInsurance"."OrderInsuranceId"
                       and "InvoiceInsuranceDetail"."_Customer" = "OrderInsurance"."_Customer"
                        AND "OrderInsurance".DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."ItemFrame"
                   on "ItemFrame"."ItemId" = "InvoiceDetail"."ItemId"
                       and "ItemFrame"."_Customer" = "InvoiceDetail"."_Customer"
                        AND "ItemFrame".DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."FrameStyle"
                   on "ItemFrame"."FrameStyleId" = "FrameStyle"."FrameStyleId"
                       and "ItemFrame"."_Customer" = "FrameStyle"."_Customer"
                        AND "FrameStyle".DELETED_FLAG <> 'Y'
    
         left join datawarehouse."FrameCollection"
                   on "FrameCollection"."FrameCollectionId" = "FrameStyle"."FrameCollectionId"
                       and "FrameCollection"."_Customer" = "FrameStyle"."_Customer"
                        AND "FrameCollection".DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."PatientRecall"
                   on "Patient"."PatientId" = "PatientRecall"."PatientId"
                       and ifnull("PatientRecall"."IsActive",'false') = 'true'
                       and "Patient"."_Customer" = "PatientRecall"."_Customer"
                        AND "PatientRecall".DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."PatientRecallType"
                   on "PatientRecall"."PatientRecallTypeId" = "PatientRecallType"."PatientRecallTypeId"
                       and "PatientRecall"."_Customer" = "PatientRecallType"."_Customer"
                        AND "PatientRecallType".DELETED_FLAG <> 'Y'

where       DATAWAREHOUSE."Patient".DELETED_FLAG <> 'Y'
--         AND DATAWAREHOUSE."ReferralType".DELETED_FLAG <> 'Y'
--         AND responsibleParty.DELETED_FLAG <> 'Y'
--         AND DATAWAREHOUSE."Office".DELETED_FLAG <> 'Y'
--         AND DATAMART."Reference_PatientAndAddress".DELETED_FLAG <> 'Y'
--         AND DATAWAREHOUSE."PatientPhone".DELETED_FLAG <> 'Y'
--         AND DATAWAREHOUSE."Phone".DELETED_FLAG <> 'Y'
--         AND DATAWAREHOUSE."InvoiceSummary".DELETED_FLAG <> 'Y'
--         AND DATAWAREHOUSE."InvoiceDetail".DELETED_FLAG <> 'Y'
--         AND DATAWAREHOUSE."InvoiceInsuranceDetail".DELETED_FLAG <> 'Y'
--         AND DATAWAREHOUSE."OrderInsurance".DELETED_FLAG <> 'Y'
--         AND DATAWAREHOUSE."ItemFrame".DELETED_FLAG <> 'Y'
--         AND DATAWAREHOUSE."FrameStyle".DELETED_FLAG <> 'Y'
--         AND DATAWAREHOUSE."FrameCollection".DELETED_FLAG <> 'Y'
--         AND DATAWAREHOUSE."PatientRecall".DELETED_FLAG <> 'Y'
--         AND DATAWAREHOUSE."PatientRecallType".DELETED_FLAG <> 'Y'
;

-- View: DATAMART.DashboardMartFact_SalesByProviderAndStaff
-- Columns: 39
create or replace view "DashboardMartFact_SalesByProviderAndStaff"(
	"InvoiceSummaryId",
	"InvoiceDetailId",
	"Quantity",
	"Tax",
	"Amount",
	"NetSales",
	"Receivable",
	"DoctorEmployeeId",
	"DoctorEmployeeName",
	"OfficeKey",
	"Office_Name",
	"DateTime",
	"PosTransactionId",
	"PosTransactionTypeId",
	"PosTransactionType_Description",
	"PatientId",
	"CompanyInfoId",
	"OrderId",
	"EmployeeId",
	"AssociateName",
	"ItemId",
	"Item_Name",
	"ItemTypeId",
	"UPCCode",
	"Item_Description",
	"SalesTypeName",
	"FrameCollection_Description",
	"Manufacturer_Name",
	"VendorName",
	"CPTCode",
	"DiagCode1_Code",
	"DiagCode2_Code",
	"DiagCode3_Code",
	"DiagCode1_Description",
	"DiagCode2_Description",
	"DiagCode3_Description",
	"ConcatDiagCodes",
	"Sales Type",
	"_Customer"
) as
select
    a."InvoiceSummaryId",
    a."InvoiceDetailId",
    a."Quantity",
    a."Tax",
    a."Amount",
    sum( ifnull(a."Amount", 0) + ifnull(a."Receivable", 0) - ifnull(a."Tax", 0) ) as "NetSales",
    a."Receivable",
    a."DoctorEmployeeId",
    a."DoctorEmployeeName",
    a."OfficeKey",
    a."Office_Name",
    a."DateTime",
    a."PosTransactionId",
    a."PosTransactionTypeId",
    a."PosTransactionType_Description",
    a."PatientId",
    a."CompanyInfoId",
    a."OrderId",
    a."EmployeeId",
    a."AssociateName",
    a."ItemId",
    a."Item_Name",
    a."ItemTypeId",
    a."UPCCode",
    a."Item_Description",
    a."SalesTypeName",
    a."FrameCollection_Description",
    a."Manufacturer_Name",
    a."VendorName",
    a."CPTCode",
    a."DiagCode1_Code",
    a."DiagCode2_Code",
    a."DiagCode3_Code",
    a."DiagCode1_Description",
    a."DiagCode2_Description",
    a."DiagCode3_Description",
    a."ConcatDiagCodes",
    a."Sales Type",
    a."_Customer"
from
    (
        select
            distinct id."InvoiceSummaryId",
            id."InvoiceDetailId",
            ifnull(id."Quantity", 0) as "Quantity",
            ifnull(id."Tax", 0) as "Tax",
            ifnull(id."Amount", 0) as "Amount",
            /*
      		sum(ifnull(id."Amount", 0) + ifnull(iid."Receivable", 0) - ifnull(id."Tax", 0)) as "NetSales",
            sum(ifnull(iid."Receivable", 0)) as "Receivable",
      		*/
            sum(ifnull(iid."Receivable", 0)) over (partition by iid."InvoiceDetailId") as "Receivable",
            ifnull(ins."DoctorEmployeeId", ins1."DoctorEmployeeId") as "DoctorEmployeeId",
            ifnull(
                ifnull(
                    concat(empDr."FirstName", ' ', empdr."LastName"),
                    concat(empDr1."FirstName", ' ', empDr1."LastName")
                ), 'OUTSIDE DOCTOR' ) as "DoctorEmployeeName",
            o."OfficeKey",
            o."Name" as "Office_Name",
            pt."DateTime",
            pt."PosTransactionId",
            pt."PosTransactionTypeId",
            pType."Description" as "PosTransactionType_Description",
            pt."PatientId",
            o."CompanyInfoId",
            ord."OrderId",
            ifnull(ins."EmployeeId", ord."EmployeeId") as "EmployeeId",
            concat(empAss."FirstName", ' ', empAss."LastName") as "AssociateName",
            i."ItemId",
            i."Name" as "Item_Name",
            i."ItemTypeId",
            i."UPCCode",
            it."Description" as "Item_Description",
            case
                when it."ItemTypeId" in (1, 13, 10, 15, 12, 11, 9, 14) then 'Eyeglasses'
                when it."ItemTypeId" in (17) then 'Contact Lenses'
                when it."ItemTypeId" in (6) then 'Services'
                else 'Other'
            end as "SalesTypeName",
            frmColl."Description" as "FrameCollection_Description",
            mfg."Name" as "Manufacturer_Name",
            clMfg."VendorName",
            cptLookup."Number" as "CPTCode",
            diagCode."DiagnosisCode1_Name" as "DiagCode1_Code",
            diagCode."DiagnosisCode2_Name" as "DiagCode2_Code",
            diagCode."DiagnosisCode3_Name" as "DiagCode3_Code",
            diagCode."DiagnosisCode1_Description" as "DiagCode1_Description",
            diagCode."DiagnosisCode2_Description" as "DiagCode2_Description",
            diagCode."DiagnosisCode3_Description" as "DiagCode3_Description",
            
            UPPER(
                CONCAT(
                    IFNULL(diagCode."DiagnosisCode1_Name", ''),
                    IFNULL(diagCode."DiagnosisCode2_Name", ''),
                    IFNULL(diagCode."DiagnosisCode3_Name", ''),
                    IFNULL(diagCode."DiagnosisCode1_Description", ''),
                    IFNULL(diagCode."DiagnosisCode2_Description", ''),
                    IFNULL(diagCode."DiagnosisCode3_Description", '')
                )
            ) as "ConcatDiagCodes",
            CASE
                WHEN lower(it."Description") in ('contact lens') then 'Contact Lens'
                WHEN lower(it."Description") in (
                    'coating',
                    'color/coat addon',
                    'edging',
                    'frames',
                    'lens base type',
                    'material addon',
                    'style addon',
                    'tint'
                ) then 'Eyeglass'
                WHEN lower(it."Description") in (
                    'accessory',
                    'gift certificate',
                    'misc fees',
                    'misc. extras',
                    'plans',
                    'repair',
                    'shipping'
                ) then 'Other'
                WHEN lower(it."Description") in ('exams') then 'Services'
                ELSE 'The Other'
            END as "Sales Type",
            id."_Customer"
        from
            DATAWAREHOUSE."InvoiceDetail_HUB" id
            inner join DATAWAREHOUSE."InvoiceSummary_HUB" ins 
                on ins."InvoiceSummaryId" = id."InvoiceSummaryId"
                and ins."_Customer" = id."_Customer"
                AND ins.DELETED_FLAG <> 'Y'
            left join DATAWAREHOUSE."InvoiceSummary_HUB" ins1 
                on ins1."OrderId" = ins."OrderId"
                and ins1."DoctorEmployeeId" is not null
                and ins1."_Customer" = ins."_Customer"
                AND ins1.DELETED_FLAG <> 'Y'
            left join DATAWAREHOUSE."InvoiceInsuranceDetail_HUB" iid 
                on iid."InvoiceDetailId" = id."InvoiceDetailId"
                and iid."_Customer" = id."_Customer"
                AND iid.DELETED_FLAG <> 'Y'
            inner join DATAWAREHOUSE."Item_HUB" i 
                on i."ItemId" = id."ItemId"
                and i."_Customer" = id."_Customer"
                AND i.DELETED_FLAG <> 'Y'
            inner join DATAWAREHOUSE."ItemType_HUB" it 
                on it."ItemTypeId" = i."ItemTypeId"
                and it."_Customer" = i."_Customer"
                AND it.DELETED_FLAG <> 'Y'
            left join DATAWAREHOUSE."PosTransaction_HUB" pt 
                on pt."InvoiceSummaryId" = ins."InvoiceSummaryId"
                and pt."_Customer" = ins."_Customer"
                AND pt.DELETED_FLAG <> 'Y'
            inner join DATAWAREHOUSE."Order_HUB" ord 
                on ord."OrderId" = pt."OrderId"
                and ord."_Customer" = pt."_Customer"
                AND ord.DELETED_FLAG <> 'Y'
            /*left join DATAWAREHOUSE."OrderExamDetail_HUB" oed 
                on oed."OrderId" = ins."OrderId"
                and oed."_Customer" = ins."_Customer"
                and oed."ItemId" = id."ItemId"
                and oed."_Customer" = id."_Customer"
                AND oed.DELETED_FLAG <> 'Y'
            */
            left join DATAWAREHOUSE."Item_HUB" cptLookup 
                on cptLookup."ItemId" = id."ItemId"
                and cptLookup."ItemTypeId" = 6
                and cptLookup."_Customer" = id."_Customer"
                AND cptLookup.DELETED_FLAG <> 'Y'
            inner join DATAWAREHOUSE."Office_HUB" o 
                on o."OfficeKey" = ord."OfficeKey"
                and o."_Customer" = ord."_Customer"
                AND o.DELETED_FLAG <> 'Y'
            left join DATAWAREHOUSE."PosTransactionType_HUB" pType 
                on pType."PosTransactionTypeId" = pt."PosTransactionTypeId"
                and pType."_Customer" = pt."_Customer"
                AND pType.DELETED_FLAG <> 'Y'
            left join DATAWAREHOUSE."Employee_HUB" empAss 
                on empAss."EmployeeId" = ins."EmployeeId"
                and empAss."_Customer" = ins."_Customer"
                AND empAss.DELETED_FLAG <> 'Y'
            left join DATAWAREHOUSE."Employee_HUB" empDr 
                on empDr."EmployeeId" = ins."DoctorEmployeeId"
                and empDr."_Customer" = ins."_Customer"
                AND empDr.DELETED_FLAG <> 'Y'
            left join DATAWAREHOUSE."Employee_HUB" empDr1 
                on empDr1."EmployeeId" = ins1."DoctorEmployeeId"
                and empDr1."_Customer" = ins1."_Customer"
                AND empDr1.DELETED_FLAG <> 'Y'
            left join DATAWAREHOUSE."ItemFrame_HUB" frm 
                on frm."ItemId" = i."ItemId"
                and frm."_Customer" = i."_Customer"
                AND frm.DELETED_FLAG <> 'Y'
            left join DATAWAREHOUSE."FrameStyle_HUB" frmStyle 
                on frmStyle."FrameStyleId" = frm."FrameStyleId"
                and frmStyle."_Customer" = frm."_Customer"
                AND frmStyle.DELETED_FLAG <> 'Y'
            left join DATAWAREHOUSE."FrameCollection_HUB" frmColl 
                on frmColl."FrameCollectionId" = frmStyle."FrameCollectionId"
                and frmColl."_Customer" = frmStyle."_Customer"
                AND frmColl.DELETED_FLAG <> 'Y'
            left join DATAWAREHOUSE."Manufacturer_HUB" mfg 
                on mfg."ManufacturerId" = frmColl."ManufacturerId"
                and o."IsSingleLocation" = true -- why is this here?  shouldn't this be on office?
                and mfg."_Customer" = frmColl."_Customer"
                AND mfg.DELETED_FLAG <> 'Y'
            left join DATAWAREHOUSE."ItemCL_HUB" itemCl 
                on itemCl."ItemId" = i."ItemId"
                and itemCl."_Customer" = i."_Customer"
                AND itemCl.DELETED_FLAG <> 'Y'
            left join DATAWAREHOUSE."CLStyle_HUB" clStyle 
                on clStyle."CLStyleId" = itemCl."CLStyleId"
                and clStyle."_Customer" = itemcl."_Customer"
                AND clStyle.DELETED_FLAG <> 'Y'
            left join DATAWAREHOUSE."CLManufacturer_HUB" clMfg 
                on clmfg."CLManufacturerId" = clStyle."CLManufacturerId"
                and clmfg."_Customer" = clStyle."_Customer"
                AND clMfg.DELETED_FLAG <> 'Y'
            left join "DATAMART"."DashboardMartDim_DiagnosisCode" diagCode 
                on diagCode."OrderId" = ord."OrderId"
                and diagCode."InvoiceSummaryId" = id."InvoiceSummaryId"
                and diagcode."ItemId" = i."ItemId"
                and diagCode."_Customer" = ord."_Customer"
        WHERE
            YEAR(pt."DateTime") > YEAR(GetDate()) - 6
            AND id.DELETED_FLAG <> 'Y'
            
            
    ) a
group by
    a."InvoiceSummaryId",
    a."InvoiceDetailId",
    a."Quantity",
    a."Tax",
    a."Amount",
    a."Receivable",
    a."DoctorEmployeeId",
    a."DoctorEmployeeName",
    a."OfficeKey",
    a."Office_Name",
    a."DateTime",
    a."PosTransactionId",
    a."PosTransactionTypeId",
    a."PosTransactionType_Description",
    a."PatientId",
    a."CompanyInfoId",
    a."OrderId",
    a."EmployeeId",
    a."AssociateName",
    a."ItemId",
    a."Item_Name",
    a."ItemTypeId",
    a."UPCCode",
    a."Item_Description",
    a."SalesTypeName",
    a."FrameCollection_Description",
    a."Manufacturer_Name",
    a."VendorName",
    a."CPTCode",
    a."DiagCode1_Code",
    a."DiagCode2_Code",
    a."DiagCode3_Code",
    a."DiagCode1_Description",
    a."DiagCode2_Description",
    a."DiagCode3_Description",
    a."ConcatDiagCodes",
    a."Sales Type",
    a."_Customer";

-- View: DATAMART.DashboardMartFact_SalesByProviderAndStaff_PRODReport
-- Columns: 40
create or replace view "DashboardMartFact_SalesByProviderAndStaff_PRODReport"(
	"InvoiceSummaryId",
	"InvoiceDetailId",
	"Retail",
	"Quantity",
	"Tax",
	"Amount",
	"NetSales",
	"Receivable",
	"DoctorEmployeeId",
	"DoctorEmployeeName",
	"OfficeKey",
	"Office_Name",
	"DateTime",
	"PosTransactionId",
	"PosTransactionTypeId",
	"PosTransactionType_Description",
	"PatientId",
	"CompanyInfoId",
	"OrderId",
	"EmployeeId",
	"AssociateName",
	"ItemId",
	"Item_Name",
	"ItemTypeId",
	"UPCCode",
	"Item_Description",
	"SalesTypeName",
	"FrameCollection_Description",
	"Manufacturer_Name",
	"VendorName",
	"CPTCode",
	"DiagCode1_Code",
	"DiagCode2_Code",
	"DiagCode3_Code",
	"DiagCode1_Description",
	"DiagCode2_Description",
	"DiagCode3_Description",
	"ConcatDiagCodes",
	"Sales Type",
	"_Customer"
) as
select distinct
    id."InvoiceSummaryId"
              , id."InvoiceDetailId"
              , ifnull(id."Price", 0.0)                  as "Retail"
              , ifnull(id."Quantity", 0)                 as "Quantity"
              , ifnull(id."Tax", 0)                      as "Tax"
              , ifnull(id."Amount", 0) as "Amount"
              , sum(ifnull(id."Amount", 0) + ifnull(iid."Receivable", 0) - ifnull(id."Tax", 0)) as "NetSales"
              , sum(ifnull(iid."Receivable", 0)) as "Receivable"
              , ifnull(ins."DoctorEmployeeId", ins1."DoctorEmployeeId") as "DoctorEmployeeId"
              , ifnull(
        ifnull(
                concat(empDr."FirstName", ' ', empdr."LastName"),
                concat(empDr1."FirstName", ' ', empDr1."LastName")
        ),
        'OUTSIDE DOCTOR') as "DoctorEmployeeName"
              , o."OfficeKey"
              , o."Name" as "Office_Name"
              , pt."DateTime"
              , pt."PosTransactionId"
              , pt."PosTransactionTypeId"
              , pType."Description" as "PosTransactionType_Description"
              , pt."PatientId"
              , o."CompanyInfoId"
              , ord."OrderId"
              , ifnull(ins."EmployeeId", ord."EmployeeId") as "EmployeeId"
              , concat(empAss."FirstName", ' ', empAss."LastName") as "AssociateName"
              , i."ItemId"
              , i."Name" as "Item_Name"
              , i."ItemTypeId"
              , i."UPCCode"
              , it."Description" as "Item_Description"
              , case
                    when it."ItemTypeId" in (1,13,10,15,12,11,9,14) then 'Eyeglasses'
                    when it."ItemTypeId" in (17) then 'Contact Lenses'
                    when it."ItemTypeId" in (6) then 'Services'
                    else 'Other' end as "SalesTypeName"
              , frmColl."Description" as "FrameCollection_Description"
              , mfg."Name" as "Manufacturer_Name"
              , clMfg."VendorName"
              , cptLookup."Number"                           as "CPTCode"
              , diagCode."DiagnosisCode1_Name"               as "DiagCode1_Code"
              , diagCode."DiagnosisCode2_Name"               as "DiagCode2_Code"
              , diagCode."DiagnosisCode3_Name"               as "DiagCode3_Code"
--               , diagCode."DiagnosisCode4_Name"               as "DiagCode4_Code"
--               , diagCode."DiagnosisCode5_Name"               as "DiagCode5_Code"
--               , diagCode."DiagnosisCode6_Name"               as "DiagCode6_Code"
--               , diagCode."DiagnosisCode7_Name"               as "DiagCode7_Code"
--               , diagCode."DiagnosisCode8_Name"               as "DiagCode8_Code"
--               , diagCode."DiagnosisCode9_Name"               as "DiagCode9_Code"
--               , diagCode."DiagnosisCode10_Name"              as "DiagCode10_Code"
--               , diagCode."DiagnosisCode11_Name"              as "DiagCode11_Code"
--               , diagCode."DiagnosisCode12_Name"              as "DiagCode12_Code"
              , diagCode."DiagnosisCode1_Description"        as "DiagCode1_Description"
              , diagCode."DiagnosisCode2_Description"        as "DiagCode2_Description"
              , diagCode."DiagnosisCode3_Description"        as "DiagCode3_Description"
--               , diagCode."DiagnosisCode4_Description"        as "DiagCode4_Description"
--               , diagCode."DiagnosisCode5_Description"        as "DiagCode5_Description"
--               , diagCode."DiagnosisCode6_Description"        as "DiagCode6_Description"
--               , diagCode."DiagnosisCode7_Description"        as "DiagCode7_Description"
--               , diagCode."DiagnosisCode8_Description"        as "DiagCode8_Description"
--               , diagCode."DiagnosisCode9_Description"        as "DiagCode9_Description"
--               , diagCode."DiagnosisCode10_Description"       as "DiagCode10_Description"
--               , diagCode."DiagnosisCode11_Description"       as "DiagCode11_Description"
--               , diagCode."DiagnosisCode12_Description"       as "DiagCode12_Description"
--               , diagCode."OrderExamDetail_DiagnosisCodeId1"  as "OrderExamDetail_DiagCode1"
--               , diagCode."OrderExamDetail_DiagnosisCodeId2"  as "OrderExamDetail_DiagCode2"
--               , diagCode."OrderExamDetail_DiagnosisCodeId3"  as "OrderExamDetail_DiagCode3"
--               , diagCode."OrderExamDetail_DiagnosisCodeId4"  as "OrderExamDetail_DiagCode4"
--               , diagCode."OrderExamDetail_DiagnosisCodeId5"  as "OrderExamDetail_DiagCode5"
--               , diagCode."OrderExamDetail_DiagnosisCodeId6"  as "OrderExamDetail_DiagCode6"
--               , diagCode."OrderExamDetail_DiagnosisCodeId7"  as "OrderExamDetail_DiagCode7"
--               , diagCode."OrderExamDetail_DiagnosisCodeId8"  as "OrderExamDetail_DiagCode8"
--               , diagCode."OrderExamDetail_DiagnosisCodeId9"  as "OrderExamDetail_DiagCode9"
--               , diagCode."OrderExamDetail_DiagnosisCodeId10" as "OrderExamDetail_DiagCode10"
--               , diagCode."OrderExamDetail_DiagnosisCodeId11" as "OrderExamDetail_DiagCode11"
--               , diagCode."OrderExamDetail_DiagnosisCodeId12" as "OrderExamDetail_DiagCode12"
    ,
    UPPER(CONCAT(
            IFNULL(diagCode."DiagnosisCode1_Name",''),IFNULL(diagCode."DiagnosisCode2_Name",''),IFNULL(diagCode."DiagnosisCode3_Name",''),
            IFNULL(diagCode."DiagnosisCode1_Description",''),IFNULL(diagCode."DiagnosisCode2_Description",''),IFNULL(diagCode."DiagnosisCode3_Description",'')
          )) as "ConcatDiagCodes"
            , CASE
                WHEN lower(it."Description") in ('contact lens') then 'Contact Lens'
                WHEN lower(it."Description") in ('coating', 'color/coat addon', 'edging', 'frames', 'lens base type', 'material addon', 'style addon', 'tint') then 'Eyeglass'
                WHEN lower(it."Description") in ('accessory', 'gift certificate', 'misc fees', 'misc. extras', 'plans', 'repair', 'shipping') then 'Other'
                WHEN lower(it."Description") in ('exams') then 'Services'
                ELSE 'The Other'
            END as "Sales Type"
              , id."_Customer"

from DATAWAREHOUSE."InvoiceDetail_HUB" id

         inner join DATAWAREHOUSE."InvoiceSummary_HUB" ins
                    on ins."InvoiceSummaryId" = id."InvoiceSummaryId"
                        and ins."_Customer" = id."_Customer"
                        AND ins.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."InvoiceSummary_HUB" ins1
                   on ins1."OrderId" = ins."OrderId"
                       and ins1."DoctorEmployeeId" is not null
                       and ins1."_Customer" = ins."_Customer"
                        AND ins1.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."InvoiceInsuranceDetail_HUB" iid
                   on iid."InvoiceDetailId" = id."InvoiceDetailId"
                       and iid."_Customer" = id."_Customer"
                        AND iid.DELETED_FLAG <> 'Y'
    
         inner join DATAWAREHOUSE."Item_HUB" i
                    on i."ItemId" = id."ItemId"
                        and i."_Customer" = id."_Customer"
                        AND i.DELETED_FLAG <> 'Y'
    
         inner join DATAWAREHOUSE."ItemType_HUB" it
                    on it."ItemTypeId" = i."ItemTypeId"
                        and it."_Customer" = i."_Customer"
                        AND it.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."PosTransaction_HUB" pt
                   on pt."InvoiceSummaryId" = ins."InvoiceSummaryId"
                       and pt."_Customer" = ins."_Customer"
                        AND pt.DELETED_FLAG <> 'Y'
    
         inner join DATAWAREHOUSE."Order_HUB" ord
                    on ord."OrderId" = pt."OrderId"
                        and ord."_Customer" = pt."_Customer"
                        AND ord.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."OrderExamDetail_HUB" oed
                   on oed."OrderId" = ins."OrderId"
                       and oed."_Customer" = ins."_Customer"
                       and oed."ItemId" = id."ItemId"
                       and oed."_Customer" = id."_Customer"
                        AND oed.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."Item_HUB" cptLookup
                   on cptLookup."ItemId" = oed."ItemId"
                       and cptLookup."ItemTypeId" = 6
                       and cptLookup."_Customer" = oed."_Customer"
                        AND cptLookup.DELETED_FLAG <> 'Y'
    
         inner join DATAWAREHOUSE."Office_HUB" o
                    on o."OfficeKey" = ord."OfficeKey"
                        and o."_Customer" = ord."_Customer"
                        AND o.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."PosTransactionType_HUB" pType
                   on pType."PosTransactionTypeId" = pt."PosTransactionTypeId"
                       and pType."_Customer" = pt."_Customer"
                        AND pType.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."Employee_HUB" empAss
                   on empAss."EmployeeId" = ins."EmployeeId"
                       and empAss."_Customer" = ins."_Customer"
                        AND empAss.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."Employee_HUB" empDr
                   on empDr."EmployeeId" = ins."DoctorEmployeeId"
                       and empDr."_Customer" = ins."_Customer"
                        AND empDr.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."Employee_HUB" empDr1
                   on empDr1."EmployeeId" = ins1."DoctorEmployeeId"
                       and empDr1."_Customer" = ins1."_Customer"
                        AND empDr1.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."ItemFrame_HUB" frm
                   on frm."ItemId" = i."ItemId"
                       and frm."_Customer" = i."_Customer"
                        AND frm.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."FrameStyle_HUB" frmStyle
                   on frmStyle."FrameStyleId" = frm."FrameStyleId"
                       and frmStyle."_Customer" = frm."_Customer"
                        AND frmStyle.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."FrameCollection_HUB" frmColl
                   on frmColl."FrameCollectionId" = frmStyle."FrameCollectionId"
                       and frmColl."_Customer" = frmStyle."_Customer"
                        AND frmColl.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."Manufacturer_HUB" mfg
                   on mfg."ManufacturerId" = frmColl."ManufacturerId"
                       and o."IsSingleLocation" = true                                -- why is this here?  shouldn't this be on office?
                       and mfg."_Customer" = frmColl."_Customer"
                        AND mfg.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."ItemCL_HUB" itemCl
                   on itemCl."ItemId" = i."ItemId"
                       and itemCl."_Customer" = i."_Customer"
                        AND itemCl.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."CLStyle_HUB" clStyle
                   on clStyle."CLStyleId" = itemCl."CLStyleId"
                       and clStyle."_Customer" = itemcl."_Customer"
                        AND clStyle.DELETED_FLAG <> 'Y'
    
         left join DATAWAREHOUSE."CLManufacturer_HUB" clMfg
                   on clmfg."CLManufacturerId" = clStyle."CLManufacturerId"
                       and clmfg."_Customer" = clStyle."_Customer"
                        AND clMfg.DELETED_FLAG <> 'Y'
    
         left join "DATAMART"."DashboardMartDim_DiagnosisCode" diagCode
                   on diagCode."OrderId" = ord."OrderId"
                       and diagCode."InvoiceSummaryId" = id."InvoiceSummaryId"
                       and diagcode."ItemId" = i."ItemId"
                       and diagCode."_Customer" = ord."_Customer"
WHERE YEAR(pt."DateTime") > YEAR(GetDate()) - 6
AND id.DELETED_FLAG <> 'Y'
-- AND ins.DELETED_FLAG <> 'Y'
-- AND ins1.DELETED_FLAG <> 'Y'
-- AND iid.DELETED_FLAG <> 'Y'
-- AND i.DELETED_FLAG <> 'Y'
-- AND it.DELETED_FLAG <> 'Y'
-- AND pt.DELETED_FLAG <> 'Y'
-- AND ord.DELETED_FLAG <> 'Y'
-- AND oed.DELETED_FLAG <> 'Y'
-- AND cptLookup.DELETED_FLAG <> 'Y'
-- AND o.DELETED_FLAG <> 'Y'
-- AND pType.DELETED_FLAG <> 'Y'
-- AND empAss.DELETED_FLAG <> 'Y'
-- AND empDr.DELETED_FLAG <> 'Y'
-- AND empDr1.DELETED_FLAG <> 'Y'
-- AND frm.DELETED_FLAG <> 'Y'
-- AND frmStyle.DELETED_FLAG <> 'Y'
-- AND frmColl.DELETED_FLAG <> 'Y'
-- AND mfg.DELETED_FLAG <> 'Y'
-- AND itemCl.DELETED_FLAG <> 'Y'
-- AND clStyle.DELETED_FLAG <> 'Y'
-- AND clMfg.DELETED_FLAG <> 'Y'

group by
    id."_Customer"
       , id."InvoiceSummaryId"
       , id."OrderId"
       , id."ItemId"
       , id."Quantity"
       , id."Price"
       , id."DiscountTypeId"
       , id."Tax"
       , id."Discount"
       , id."LineDiscount"
       , id."PackageDiscount"
       , id."Amount"
       , id."InvoiceDetailId"
       , i."Group"
       , ins."DoctorEmployeeId"
       , o."OfficeKey"
       , o."Name"
       , ins."EmployeeId"
       , pt."DateTime"
       , pt."PosTransactionId"
       , pt."PatientId"
       , o."CompanyInfoId"
       , ord."OrderId"
       , ord."EmployeeId"
       , i."ItemId"
       , i."Name"
       , i."Number"
       , i."ItemTypeId"
       , i."UPCCode"
       , it."ItemTypeId"
       , it."Description"
       , empAss."FirstName"
       , empAss."LastName"
       , pt."PosTransactionTypeId"
       , frmColl."Description"
       , mfg."Name"
       , clmfg."VendorName"
       , empDr."FirstName"
       , empDr."LastName"
       , cptLookup."Number"
       , o."IsSingleLocation"
       , ins."DoctorEmployeeId"
       , ins1."DoctorEmployeeId"
       , empDr1."FirstName"
       , empDr1."LastName"
       , pType."Description"
       , diagCode."DiagnosisCode1_Name"
       , diagCode."DiagnosisCode2_Name"
       , diagCode."DiagnosisCode3_Name"
       , diagCode."DiagnosisCode4_Name"
       , diagCode."DiagnosisCode5_Name"
       , diagCode."DiagnosisCode6_Name"
       , diagCode."DiagnosisCode7_Name"
       , diagCode."DiagnosisCode8_Name"
       , diagCode."DiagnosisCode9_Name"
       , diagCode."DiagnosisCode10_Name"
       , diagCode."DiagnosisCode11_Name"
       , diagCode."DiagnosisCode12_Name"
       , diagCode."DiagnosisCode1_Description"
       , diagCode."DiagnosisCode2_Description"
       , diagCode."DiagnosisCode3_Description"
       , diagCode."DiagnosisCode4_Description"
       , diagCode."DiagnosisCode5_Description"
       , diagCode."DiagnosisCode6_Description"
       , diagCode."DiagnosisCode7_Description"
       , diagCode."DiagnosisCode8_Description"
       , diagCode."DiagnosisCode9_Description"
       , diagCode."DiagnosisCode10_Description"
       , diagCode."DiagnosisCode11_Description"
       , diagCode."DiagnosisCode12_Description"
       , diagCode."OrderExamDetail_DiagnosisCodeId1"
       , diagCode."OrderExamDetail_DiagnosisCodeId2"
       , diagCode."OrderExamDetail_DiagnosisCodeId3"
       , diagCode."OrderExamDetail_DiagnosisCodeId4"
       , diagCode."OrderExamDetail_DiagnosisCodeId5"
       , diagCode."OrderExamDetail_DiagnosisCodeId6"
       , diagCode."OrderExamDetail_DiagnosisCodeId7"
       , diagCode."OrderExamDetail_DiagnosisCodeId8"
       , diagCode."OrderExamDetail_DiagnosisCodeId9"
       , diagCode."OrderExamDetail_DiagnosisCodeId10"
       , diagCode."OrderExamDetail_DiagnosisCodeId11"
       , diagCode."OrderExamDetail_DiagnosisCodeId12";

-- View: DATAMART.DashboardPatientMarketingDim_SalesRevenue
-- Columns: 51
create or replace view "DashboardPatientMarketingDim_SalesRevenue"(
	"PatientId",
	"FirstName",
	"LastName",
	"PatientName",
	"CompanyInfoId",
	"PosTransactionTypeId",
	"PosTransactionType_Description",
	"PosTransactionId",
	"PosTransaction_DateTime",
	"OfficeKey",
	"TransactionAssociate",
	"OrderAssociateName",
	"Associate",
	"CombinedInvoiceAssociateName",
	"OriginalAssociate",
	"ItemTypeId",
	"InvoiceSummaryId",
	"OrderId",
	"DoctorEmployeeId",
	"DoctorId",
	DOCTOREMPLOYEENAME,
	"ItemType_Description",
	"ItemTypeGroup",
	"Retail",
	"Tax",
	"Discount",
	"Amount",
	"Allowance",
	"Receivable",
	"Copay",
	"InvoiceCount",
	"DayCloseId",
	"NetSales",
	"OfficeType_Value",
	"Region_Value",
	"Office_Name",
	"SalesCategory",
	"IsFittingFee",
	"IsProcedure",
	"ItemId",
	"Quantity",
	"CLStyleId",
	"VendorName",
	LENSSTYLE,
	"LensName",
	"CLTypeId",
	"CLStyle_Style",
	"CLType_Value",
	"IsHard",
	"LensCategory",
	"_Customer"
) as
SELECT "PosTransaction"."PatientId"
     , "Patient"."FirstName"
     , "Patient"."LastName"
     , concat("Patient"."FirstName", ' ', "Patient"."LastName")                            AS "PatientName"
     , "Office"."CompanyInfoId"
     , "PosTransaction"."PosTransactionTypeId"
     , "PosTransactionType"."Description"                                                  as "PosTransactionType_Description"
     , "PosTransaction"."PosTransactionId"
     , "PosTransaction"."DateTime"                                                         as "PosTransaction_DateTime"
     , "PosTransaction"."OfficeKey"
     , ifnull("DashboardMartDim_CombinedInvoiceDetail"."EmployeeId",
              "Order"."EmployeeId")                                                        as "TransactionAssociate"
     , concat(orderAssociate."FirstName", ' ', orderAssociate."LastName")                  as "OrderAssociateName"
     , ifnull("Order"."EmployeeId", "DashboardMartDim_CombinedInvoiceDetail"."EmployeeId") as "Associate"
     , concat(combinedInvoiceDetailEmployee."FirstName", ' ',
              combinedInvoiceDetailEmployee."LastName")                                    as "CombinedInvoiceAssociateName"
     , ifnull("DashboardMartDim_CombinedInvoiceDetail"."EmployeeId",
              "Order"."EmployeeId")                                                        as "OriginalAssociate"
     , "ItemType"."ItemTypeId"
     , "DashboardMartDim_CombinedInvoiceDetail"."InvoiceSummaryId"
     , "Order"."OrderId"
     , invoiceSummary."DoctorEmployeeId"
     , ifnull(invoiceSummary."DoctorEmployeeId", invoiceSummaryOrder."DoctorEmployeeId")   as "DoctorId"
     , ifnull(
        ifnull(
                concat(invoiceSummaryDoctor."FirstName", ' ', invoiceSummaryDoctor."LastName"),
                concat(invoiceSummaryOrderDoctor."FirstName", ' ', invoiceSummaryOrderDoctor."LastName")
        ),
        'OUTSIDE DOCTOR')                                                                  as DoctorEmployeeName
     , "ItemType"."Description"                                                            as "ItemType_Description"
     , CASE
           WHEN LOWER("ItemType"."Description") in
                ('coating', 'color/coat addon', 'edging', 'frames', 'lens base type' , 'material addon', 'style addon', 'tint') THEN 'Eyeglasses'
           WHEN LOWER("ItemType"."Description") in ('contact lens') THEN 'Contact Lens'
           WHEN LOWER("ItemType"."Description") in ('exams') THEN 'Exams'
           WHEN LOWER("ItemType"."Description") in
                ('accessory', 'gift certificate', 'misc fees', 'misc. extras', 'plans', 'repairs', 'shipping') THEN 'Other'
           ELSE 'OTHER'
    END as "ItemTypeGroup"
     , sum(ifnull("DashboardMartDim_CombinedInvoiceDetail"."Price", 0.0))                  as "Retail"
     , sum(ifnull("DashboardMartDim_CombinedInvoiceDetail"."Tax", 0.0))                    as "Tax"
     , sum(ifnull("DashboardMartDim_CombinedInvoiceDetail"."Discount", 0.0))
    + sum(ifnull("DashboardMartDim_CombinedInvoiceDetail"."LineDiscount", 0.0))
    + sum(ifnull("DashboardMartDim_CombinedInvoiceDetail"."PackageDiscount", 0.0))
    + sum(ifnull("DashboardMartDim_CombinedInvoiceDetail"."InsuranceDiscount", 0.0))       as "Discount"
     , sum(ifnull("DashboardMartDim_CombinedInvoiceDetail"."Amount", 0.0))                 AS "Amount"
     , sum(ifnull("DashboardMartDim_CombinedInvoiceDetail"."Allowance", 0.0))              AS "Allowance"
     , sum(ifnull("DashboardMartDim_CombinedInvoiceDetail"."Receivable", 0.0))             AS "Receivable"
     , sum(ifnull("DashboardMartDim_CombinedInvoiceDetail"."Copay", 0.0))                  AS "Copay"

     , sum(CASE
               WHEN ifnull("PosTransaction"."Amount",0) < 0 THEN - 1
               WHEN ifnull("PosTransaction"."Amount",0) = 0
                   AND ("PosTransaction"."PosTransactionTypeId" <> 1
                       AND "PosTransaction"."PosTransactionTypeId" <> 13) THEN - 1
               ELSE 1 END)                                                                 AS "InvoiceCount"
     , "PosTransaction"."DayCloseId"
     , sum((ifnull("DashboardMartDim_CombinedInvoiceDetail"."Receivable", 0))
               + (ifnull("DashboardMartDim_CombinedInvoiceDetail"."Amount", 0))
    - (ifnull("DashboardMartDim_CombinedInvoiceDetail"."Tax", 0)))                         AS "NetSales"

     , "OfficeType"."Value"                                                                as "OfficeType_Value"
     , IFNULL("Region"."Value",'')                                                                    as "Region_Value"
     , "Office"."Name"                                                                     as "Office_Name"
     , ifnull("CompanyItemType"."SalesCategory", 1)                                        AS "SalesCategory"
     , "ItemExam"."IsFittingFee"
     , "ItemExam"."IsProcedure"
     , "ItemExam"."ItemId"
     , sum(ifnull("DashboardMartDim_CombinedInvoiceDetail"."Quantity", 0))                 AS "Quantity"
     , "ItemCL"."CLStyleId"
     , IFNULL("CLManufacturer"."VendorName",'')                                            AS "VendorName"
     , IFNULL("CLStyle"."Style",'')                                                                   AS LensStyle
     , CASE
           WHEN ("ItemCompany"."ItemCompanyId" IS NOT NULL) OR ("ItemCompany"."ItemId" IS NOT NULL)
               THEN "ItemCompany"."Name"
           ELSE "Item"."Name"
    END                                                                                    AS "LensName"
     , "CLStyle"."CLTypeId"
     , "CLStyle"."Style"                                                                    AS "CLStyle_Style"
     , IFNULL("CLType"."Value",'')                                                                    AS "CLType_Value"
     , "CLStyle"."IsHard"
     , CASE
           WHEN "CLStyle"."IsHard" = false THEN 'Soft Contact Lens Types'
           WHEN "CLStyle"."IsHard" = true THEN 'Hard/Special'
           ELSE NULL
    END                                                                                    AS "LensCategory"
     , "PosTransaction"."_Customer"

FROM DATAWAREHOUSE."PosTransaction"

         INNER JOIN DATAWAREHOUSE."Order"
                    on "Order"."OrderId" = "PosTransaction"."OrderId"
                        and "PosTransaction"."PosTransactionTypeId" <> 8
                        and "Order"."_Customer" = "PosTransaction"."_Customer"
                        AND DATAWAREHOUSE."Order".DELETED_FLAG <> 'Y'

         LEFT JOIN DATAWAREHOUSE."Employee" orderAssociate
                   on orderAssociate."EmployeeId" = "Order"."EmployeeId"
                       and orderAssociate."_Customer" = "Order"."_Customer"
                       AND orderAssociate.DELETED_FLAG <> 'Y'

         INNER JOIN DATAWAREHOUSE."Patient"
                    on "Patient"."PatientId" = "PosTransaction"."PatientId"
                        and "Patient"."_Customer" = "PosTransaction"."_Customer"
                        AND DATAWAREHOUSE."Patient".DELETED_FLAG <> 'Y'

         INNER JOIN DATAMART."DashboardMartDim_CombinedInvoiceDetail"
                    ON "DashboardMartDim_CombinedInvoiceDetail"."InvoiceSummaryId" = "PosTransaction"."InvoiceSummaryId"
                        and "DashboardMartDim_CombinedInvoiceDetail"."_Customer" = "PosTransaction"."_Customer"

         INNER JOIN DATAWAREHOUSE."InvoiceSummary" invoiceSummary
                    ON invoiceSummary."InvoiceSummaryId" = "DashboardMartDim_CombinedInvoiceDetail"."InvoiceSummaryId"
                        and invoiceSummary."_Customer" = "DashboardMartDim_CombinedInvoiceDetail"."_Customer"
                        AND invoiceSummary.DELETED_FLAG <> 'Y'

         LEFT JOIN DATAWAREHOUSE."InvoiceSummary" invoiceSummaryOrder
                   on invoiceSummaryOrder."OrderId" = invoiceSummary."OrderId"
                       and invoiceSummaryOrder."DoctorEmployeeId" is not null
                       and invoiceSummaryOrder."_Customer" = invoiceSummary."_Customer"
                       AND invoiceSummaryOrder.DELETED_FLAG <> 'Y'

         LEFT JOIN DATAWAREHOUSE."Employee" invoiceSummaryDoctor
                   on invoiceSummaryDoctor."EmployeeId" = invoiceSummary."DoctorEmployeeId"
                       and invoiceSummaryDoctor."_Customer" = invoiceSummary."_Customer"
                       AND invoiceSummaryDoctor.DELETED_FLAG <> 'Y'

         LEFT JOIN DATAWAREHOUSE."Employee" invoiceSummaryOrderDoctor
                   on invoiceSummaryOrderDoctor."EmployeeId" = invoiceSummaryOrder."DoctorEmployeeId"
                       and invoiceSummaryOrderDoctor."_Customer" = invoiceSummaryOrder."_Customer"
                       AND invoiceSummaryOrderDoctor.DELETED_FLAG <> 'Y'

         LEFT JOIN DATAWAREHOUSE."Employee" combinedInvoiceDetailEmployee
                   on combinedInvoiceDetailEmployee."EmployeeId" = "DashboardMartDim_CombinedInvoiceDetail"."EmployeeId"
                       and
                      combinedInvoiceDetailEmployee."_Customer" = "DashboardMartDim_CombinedInvoiceDetail"."_Customer"
                       AND combinedInvoiceDetailEmployee.DELETED_FLAG <> 'Y'

         INNER JOIN DATAWAREHOUSE."ItemType"
                    on "ItemType"."ItemTypeId" = "DashboardMartDim_CombinedInvoiceDetail"."ItemTypeId"
                        and "ItemType"."_Customer" = "DashboardMartDim_CombinedInvoiceDetail"."_Customer"
                        AND DATAWAREHOUSE."ItemType".DELETED_FLAG <> 'Y'

         LEFT JOIN DATAWAREHOUSE."ItemExam"
                   on "ItemExam"."ItemId" = "DashboardMartDim_CombinedInvoiceDetail"."ItemId"
                       and "ItemExam"."_Customer" = "DashboardMartDim_CombinedInvoiceDetail"."_Customer"
                       AND DATAWAREHOUSE."ItemExam".DELETED_FLAG <> 'Y'

         INNER JOIN DATAWAREHOUSE."Office"
                    on "Office"."OfficeKey" = "PosTransaction"."OfficeKey"
                        and "Office"."IsLive" = true
                        and "Office"."_Customer" = "PosTransaction"."_Customer"
                        AND DATAWAREHOUSE."Office".DELETED_FLAG <> 'Y'

         left join DATAWAREHOUSE."OfficeType"
                   on "OfficeType"."OfficeTypeId" = "Office"."OfficeTypeId"
                       and "OfficeType"."_Customer" = "Office"."_Customer"
                       AND DATAWAREHOUSE."OfficeType".DELETED_FLAG <> 'Y'

         left join DATAWAREHOUSE."Region"
                   on "Region"."RegionId" = "Office"."RegionId"
                       and "Region"."_Customer" = "Office"."_Customer"
                       AND DATAWAREHOUSE."Region".DELETED_FLAG <> 'Y'

         LEFT JOIN DATAWAREHOUSE."PosTransactionType"
                   on "PosTransactionType"."PosTransactionTypeId" = "PosTransaction"."PosTransactionTypeId"
                       and "PosTransactionType"."_Customer" = "PosTransaction"."_Customer"
                       AND DATAWAREHOUSE."PosTransactionType".DELETED_FLAG <> 'Y'

         LEFT JOIN DATAWAREHOUSE."CompanyItemType"
                   on "CompanyItemType"."ItemTypeId" = "ItemType"."ItemTypeId"
                       and "CompanyItemType"."CompanyInfoId" = "Office"."CompanyInfoId"
                       and "CompanyItemType"."_Customer" = "ItemType"."_Customer"
                       and "CompanyItemType"."_Customer" = "Office"."_Customer"
                       AND DATAWAREHOUSE."CompanyItemType".DELETED_FLAG <> 'Y'

         LEFT JOIN DATAWAREHOUSE."ItemCL"
                   on "ItemCL"."ItemId" = "DashboardMartDim_CombinedInvoiceDetail"."ItemId"
                       and "ItemCL"."_Customer" = "DashboardMartDim_CombinedInvoiceDetail"."_Customer"
                       AND DATAWAREHOUSE."ItemCL".DELETED_FLAG <> 'Y'

         LEFT JOIN DATAWAREHOUSE."Item"
                   on "Item"."ItemId" = "ItemCL"."ItemId"
                       and "Item"."_Customer" = "ItemCL"."_Customer"
                       AND DATAWAREHOUSE."Item".DELETED_FLAG <> 'Y'

         LEFT JOIN DATAWAREHOUSE."CLStyle"
                   on "CLStyle"."CLStyleId" = "ItemCL"."CLStyleId"
                       AND "CLStyle"."CLTypeId" = COALESCE(null, "CLStyle"."CLTypeId") -- this is stupid!!
                       and "CLStyle"."CLManufacturerId" = COALESCE(null, "CLStyle"."CLManufacturerId") -- so is this!!
                       and "CLStyle"."_Customer" = "ItemCL"."_Customer"
                       AND DATAWAREHOUSE."CLStyle".DELETED_FLAG <> 'Y'

         LEFT JOIN DATAWAREHOUSE."CLManufacturer"
                   on "CLManufacturer"."CLManufacturerId" = "CLStyle"."CLManufacturerId"
                       and "CLManufacturer"."_Customer" = "CLStyle"."_Customer"
                       AND "CLManufacturer".DELETED_FLAG <> 'Y'

         LEFT JOIN DATAWAREHOUSE."ItemCompany"
                   on "ItemCompany"."ItemId" = "Item"."ItemId"
                       and "ItemCompany"."CompanyInfoId" = "Office"."CompanyInfoId"
                       and "ItemCompany"."_Customer" = "Office"."_Customer"
                       AND DATAWAREHOUSE."ItemCompany".DELETED_FLAG <> 'Y'

         LEFT JOIN DATAWAREHOUSE."CLType"
                   on "CLType"."CLTypeId" = "CLStyle"."CLTypeId"
                       and "CLType"."_Customer" = "CLStyle"."_Customer"
                       AND DATAWAREHOUSE."CLStyle".DELETED_FLAG <> 'Y'

WHERE YEAR("PosTransaction"."DateTime") > YEAR(GetDate()) - 6
AND DATAWAREHOUSE."PosTransaction".DELETED_FLAG <> 'Y'
-- AND DATAWAREHOUSE."Order".DELETED_FLAG <> 'Y'
-- AND DATAWAREHOUSE."Patient".DELETED_FLAG <> 'Y'
-- AND DATAWAREHOUSE."ItemType".DELETED_FLAG <> 'Y'
-- AND DATAWAREHOUSE."Office".DELETED_FLAG <> 'Y'
-- AND DATAWAREHOUSE."PosTransactionType".DELETED_FLAG <> 'Y'
-- AND DATAWAREHOUSE."CompanyItemType".DELETED_FLAG <> 'Y'
-- AND DATAWAREHOUSE."ItemCL".DELETED_FLAG <> 'Y'
-- AND DATAWAREHOUSE."Item".DELETED_FLAG <> 'Y'
-- AND DATAWAREHOUSE."CLStyle".DELETED_FLAG <> 'Y'
-- AND DATAWAREHOUSE."CLManufacturer".DELETED_FLAG <> 'Y'
-- AND DATAWAREHOUSE."ItemCompany".DELETED_FLAG <> 'Y'
-- AND DATAWAREHOUSE."CLType".DELETED_FLAG <> 'Y'
-- AND DATAWAREHOUSE."Region".DELETED_FLAG <> 'Y'
-- AND DATAWAREHOUSE."OfficeType".DELETED_FLAG <> 'Y'
-- AND invoiceSummaryOrderDoctor.DELETED_FLAG <> 'Y'
--   AND invoiceSummaryDoctor.DELETED_FLAG <> 'Y'
--   AND combinedInvoiceDetailEmployee.DELETED_FLAG <> 'Y'
-- AND invoiceSummary.DELETED_FLAG <> 'Y'
-- AND invoiceSummaryOrder.DELETED_FLAG <> 'Y'

GROUP BY "PosTransaction"."_Customer"
       , "PosTransaction"."PosTransactionTypeId"
       , "PosTransactionType"."Description"
       , "PosTransaction"."DateTime"
       , "PosTransaction"."OfficeKey"
       , "PosTransaction"."EmployeeId"
       , "ItemType"."Description"
       , "ItemType"."ItemTypeId"
       , "PosTransaction"."DayCloseId"
       , "Office"."OfficeTypeId"
       , "OfficeType"."Value"
       , "Office"."RegionId" -- this is marketId
       , "Region"."Value"
       , "Office"."Name"
       , "Office"."CompanyInfoId"
       , "PosTransaction"."OrderId"
       , "Order"."EmployeeId"
       , "DashboardMartDim_CombinedInvoiceDetail"."EmployeeId"
       , "CompanyItemType"."SalesCategory"
       , "PosTransaction"."PatientId"
       , "ItemExam"."IsFittingFee"
       , "ItemExam"."IsProcedure"
       , "ItemExam"."ItemId"
       , "DashboardMartDim_CombinedInvoiceDetail"."InvoiceSummaryId"
       , "PosTransaction"."PosTransactionId"
       , "Patient"."FirstName"
       , "Patient"."LastName"
       , orderAssociate."FirstName"
       , orderAssociate."LastName"
       , "Order"."OrderId"
       , combinedInvoiceDetailEmployee."FirstName"
       , combinedInvoiceDetailEmployee."LastName"
       , invoiceSummary."DoctorEmployeeId"
       , invoiceSummaryDoctor."FirstName"
       , invoiceSummaryDoctor."LastName"
       , invoiceSummaryOrder."DoctorEmployeeId"
       , invoiceSummaryOrderDoctor."FirstName"
       , invoiceSummaryOrderDoctor."LastName"
       , "DashboardMartDim_CombinedInvoiceDetail"."ItemId"
       , "ItemCL"."CLStyleId"
       , "CLManufacturer"."VendorName"
       , "CLStyle"."Style"
       , "ItemCompany"."ItemCompanyId"
       , "ItemCompany"."Name"
       , "ItemCompany"."ItemId"
       , "Item"."Name"
       , "CLStyle"."CLTypeId"
       , "CLStyle"."IsHard"
       , "CLType"."Value";

-- View: DATAMART.ICARE_REFERENCE_PATIENT
-- Columns: 6
-- ERROR extracting DDL: 002043 (02000): SQL compilation error:
Object does not exist, or operation cannot be performed.

-- View: DATAMART.Reference_PatientAndAddress
-- Columns: 32
create or replace view "Reference_PatientAndAddress"(
	"PatientId",
	"AddressId",
	"_Customer",
	DELETED_FLAG,
	"Patient_LastName",
	"Patient_FirstName",
	"Patient_BirthDate",
	"Patient_Sex",
	"Patient_MiddleInitial",
	"Patient_Email",
	"Patient_ResponsiblePatientId",
	"Patient_ReferralTypeId",
	"Patient_LastExamDate",
	"Patient_HomeOfficeKey",
	"Patient_IsAddressBad",
	"Patient_IsEmailBad",
	"Patient_IsInactive",
	"Patient_NickName",
	"Patient_ProviderEmployeeId",
	"Patient_IsPatient",
	"Patient_ResponsiblePartyFullName",
	"Patient_CompanyInfoId",
	"Patient_DELETED_FLAG",
	"Address_Line1",
	"Address_Line2",
	"Address_City",
	"Address_State",
	"Address_ZipCode",
	"Address_IsPrimary",
	"Address_DELETED_FLAG",
	"ReferralType_Name",
	"ReferralType_DELETED_FLAG"
) as
SELECT
    "DATAWAREHOUSE"."PatientAddress_HUB"."PatientId"         AS "PatientId",
    "DATAWAREHOUSE"."PatientAddress_HUB"."AddressId"         AS "AddressId",
    "DATAWAREHOUSE"."PatientAddress_HUB"."_Customer"         AS "_Customer",
    "DATAWAREHOUSE"."PatientAddress_HUB"."DELETED_FLAG"      AS "DELETED_FLAG",
    "DATAWAREHOUSE"."Patient_HUB"."LastName"                 AS "Patient_LastName",
    "DATAWAREHOUSE"."Patient_HUB"."FirstName"                AS "Patient_FirstName",
    "DATAWAREHOUSE"."Patient_HUB"."BirthDate"                AS "Patient_BirthDate",
    "DATAWAREHOUSE"."Patient_HUB"."Sex"                      AS "Patient_Sex",
    "DATAWAREHOUSE"."Patient_HUB"."MiddleInitial"            AS "Patient_MiddleInitial",
    "DATAWAREHOUSE"."Patient_HUB"."Email_01"                 AS "Patient_Email",
    "DATAWAREHOUSE"."Patient_HUB"."ResponsiblePatientId"     AS "Patient_ResponsiblePatientId",
    "DATAWAREHOUSE"."Patient_HUB"."ReferralTypeId"           AS "Patient_ReferralTypeId",
    "DATAWAREHOUSE"."Patient_HUB"."LastExamDate"             AS "Patient_LastExamDate",
    "DATAWAREHOUSE"."Patient_HUB"."HomeOfficeKey"            AS "Patient_HomeOfficeKey",
    "DATAWAREHOUSE"."Patient_HUB"."IsAddressBad"             AS "Patient_IsAddressBad",
    "DATAWAREHOUSE"."Patient_HUB"."IsEmailBad"               AS "Patient_IsEmailBad",
    "DATAWAREHOUSE"."Patient_HUB"."IsInActive"               AS "Patient_IsInactive",
    "DATAWAREHOUSE"."Patient_HUB"."NickName"                 AS "Patient_NickName",
    "DATAWAREHOUSE"."Patient_HUB"."ProviderEmployeeId"       AS "Patient_ProviderEmployeeId",
    "DATAWAREHOUSE"."Patient_HUB"."IsPatient"                AS "Patient_IsPatient",
    "DATAWAREHOUSE"."Patient_HUB"."ResponsiblePartyFullName" AS "Patient_ResponsiblePartyFullName",
    "DATAWAREHOUSE"."Patient_HUB"."CompanyInfoId" AS "Patient_CompanyInfoId",
    "DATAWAREHOUSE"."Patient_HUB"."DELETED_FLAG" AS "Patient_DELETED_FLAG",
    "DATAWAREHOUSE"."Address_HUB"."Line1"         AS "Address_Line1",
    "DATAWAREHOUSE"."Address_HUB"."Line2"         AS "Address_Line2",
    "DATAWAREHOUSE"."Address_HUB"."City"          AS "Address_City",
    "DATAWAREHOUSE"."Address_HUB"."State"         AS "Address_State",
    "DATAWAREHOUSE"."Address_HUB"."ZipCode"       AS "Address_ZipCode",
    "DATAWAREHOUSE"."Address_HUB"."IsPrimary"     AS "Address_IsPrimary",
    "DATAWAREHOUSE"."Address_HUB"."DELETED_FLAG"  AS "Address_DELETED_FLAG",
    "DATAWAREHOUSE"."ReferralType_HUB"."Value"    AS "ReferralType_Name",
    "DATAWAREHOUSE"."ReferralType_HUB"."DELETED_FLAG"    AS "ReferralType_DELETED_FLAG"
FROM
    "DATAWAREHOUSE"."PatientAddress_HUB"
        INNER JOIN
    "DATAWAREHOUSE"."Patient_HUB"
    ON
        (
            "DATAWAREHOUSE"."PatientAddress_HUB"."PatientId" =
            "DATAWAREHOUSE"."Patient_HUB"."PatientId")
            AND
        (
            "DATAWAREHOUSE"."PatientAddress_HUB"."_Customer" =
            "DATAWAREHOUSE"."Patient_HUB"."_Customer")
        INNER JOIN
    "DATAWAREHOUSE"."Address_HUB"
    ON
        (
            "DATAWAREHOUSE"."PatientAddress_HUB"."AddressId" =
            "DATAWAREHOUSE"."Address_HUB"."AddressId")
            AND
        (
            "DATAWAREHOUSE"."PatientAddress_HUB"."_Customer" =
            "DATAWAREHOUSE"."Address_HUB"."_Customer")
        LEFT OUTER JOIN
    "DATAWAREHOUSE"."ReferralType_HUB"
    ON
        (
            "DATAWAREHOUSE"."Patient_HUB"."ReferralTypeId" =
            "DATAWAREHOUSE"."ReferralType_HUB"."ReferralTypeId")
            AND
        (
            "DATAWAREHOUSE"."Patient_HUB"."_Customer" = "DATAWAREHOUSE"."ReferralType_HUB"."_Customer"
            );

