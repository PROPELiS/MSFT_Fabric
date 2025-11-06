-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "298cd444-fb22-4489-a6f9-4843f505fceb",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "298cd444-fb22-4489-a6f9-4843f505fceb",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Params: ${is_full} (Boolean), '${BeginDate}', '${EndDate}'

DELETE FROM delta.`abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/UKG/Timecard_Metric`
WHERE ${is_full}
  AND ApplyDate BETWEEN to_date(${BeginDate},'yyyy-MM-dd') AND to_date(${EndDate},'yyyy-MM-dd');
  
WITH src_raw AS (
  SELECT
      `employeeId.qualifier`  AS EmployeeQualifier,
      `actualTotals.job.id`   AS JobId,
      element_at(split(`actualTotals.job.qualifier`, '/'), -1) AS employeeJobTitle,
      element_at(split(`actualTotals.job.qualifier`, '/'), -2) AS departmentName,
      element_at(split(`actualTotals.job.qualifier`, '/'), -3) AS plantName,
      element_at(split(`actualTotals.job.qualifier`, '/'), -4) AS location,
      element_at(split(`actualTotals.job.qualifier`, '/'), -5) AS orgLevel5,
      element_at(split(`actualTotals.job.qualifier`, '/'), -6) AS orgLevel6,
      `actualTotals.job.name` AS JobName,
      `actualTotals.payCode.id`    AS PayCodeId,
      `actualTotals.payCode.name`  AS PayCodeName,
      `actualTotals.amountType`    AS AmountType,
      `actualTotals.hoursAmount`   AS HoursAmount,
      `actualTotals.wagesCurrency.currencyCode` AS WagesCurrencyCode,
      CAST(`actualTotals.payPeriodNumber` AS INT) AS PayPeriodNumber,
      to_date(`actualTotals.applyDate`, 'yyyy-MM-dd') AS ApplyDate,
      `actualTotals.uniqueId` AS UniqueId,
      `actualTotals.laborCategories.entries.laborCategoryEntry.name` AS LaborCategoryName,
      `actualTotals.laborCategories.entries.laborCategoryEntryDescription` AS LaborCategoryDescription,
      `CurrentAsOfDate` as CurrentAsOfDate
  FROM delta.`abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/UKG/TimeCard`
  WHERE `actualTotals.hoursAmount` IS NOT NULL
    AND `actualTotals.hoursAmount` <> 0
    AND to_date(`actualTotals.applyDate`, 'yyyy-MM-dd')
        BETWEEN to_date(${BeginDate}, 'yyyy-MM-dd') AND to_date(${EndDate}, 'yyyy-MM-dd')
    AND lower(`actualTotals.laborCategories.entries.laborCategory.name`) = 'hr job'
),

src_dedup AS (
  SELECT *
  FROM (
    SELECT
      sr.*,
      ROW_NUMBER() OVER (
        PARTITION BY
          sr.UniqueId,
          sr.EmployeeQualifier,
          sr.ApplyDate
        ORDER BY
          sr.CurrentAsOfDate Desc
      ) AS rn
    FROM src_raw sr
  ) x
  WHERE rn = 1
)

MERGE INTO delta.`abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/UKG/Timecard_Metric` AS tgt

USING (
  SELECT
    EmployeeQualifier,
    JobId,
    employeeJobTitle,
    departmentName,
    plantName,
    location,
    orgLevel5,
    orgLevel6,
    JobName,
    PayCodeId,
    PayCodeName,
    AmountType,
    HoursAmount,
    WagesCurrencyCode,
    PayPeriodNumber,
    ApplyDate,
    UniqueId,
    LaborCategoryName,
    LaborCategoryDescription
  FROM src_dedup
) AS src
ON  tgt.UniqueId          = src.UniqueId
AND tgt.EmployeeQualifier = src.EmployeeQualifier
AND tgt.ApplyDate         = src.ApplyDate

WHEN MATCHED THEN UPDATE SET
  tgt.EmployeeQualifier        = src.EmployeeQualifier,
  tgt.JobId                    = src.JobId,
  tgt.employeeJobTitle         = src.employeeJobTitle,
  tgt.departmentName           = src.departmentName,
  tgt.plantName                = src.plantName,
  tgt.location                 = src.location,
  tgt.orgLevel5                = src.orgLevel5,
  tgt.orgLevel6                = src.orgLevel6,
  tgt.JobName                  = src.JobName,
  tgt.PayCodeId                = src.PayCodeId,
  tgt.PayCodeName              = src.PayCodeName,
  tgt.AmountType               = src.AmountType,
  tgt.HoursAmount              = src.HoursAmount,
  tgt.WagesCurrencyCode        = src.WagesCurrencyCode,
  tgt.PayPeriodNumber          = src.PayPeriodNumber,
  tgt.ApplyDate                = src.ApplyDate,
  tgt.LaborCategoryName        = src.LaborCategoryName,
  tgt.LaborCategoryDescription = src.LaborCategoryDescription

WHEN NOT MATCHED THEN INSERT (
  EmployeeQualifier, JobId, employeeJobTitle, departmentName, plantName, location,
  orgLevel5, orgLevel6, JobName, PayCodeId, PayCodeName, AmountType, HoursAmount,
  WagesCurrencyCode, PayPeriodNumber, ApplyDate, UniqueId, LaborCategoryName, LaborCategoryDescription
) VALUES (
  src.EmployeeQualifier, src.JobId, src.employeeJobTitle, src.departmentName, src.plantName, src.location,
  src.orgLevel5, src.orgLevel6, src.JobName, src.PayCodeId, src.PayCodeName, src.AmountType, src.HoursAmount,
  src.WagesCurrencyCode, src.PayPeriodNumber, src.ApplyDate, src.UniqueId, src.LaborCategoryName, src.LaborCategoryDescription
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
