-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Question 2 & 3: Doco and Profile tables in Silver Layer
-- MAGIC ## Question 4: Design Table structure

-- COMMAND ----------

Show tables in bronze

-- COMMAND ----------

/*Agent Code seems to be a suitable Key*/
SELECT * FROM bronze.agent_ref;

--Agent_Ref table is in 3rd Normal form, no changes needed, iterate for all tables.

DROP TABLE IF EXISTS silver.agent_ref;

CREATE TABLE silver.agent_ref AS SELECT * FROM bronze.agent_ref;


-- COMMAND ----------

/*lat & long -> useful for GIS MAP visual
--Zip Code is missing a leading Zero -> Probably because of Excel source

--Branch 12003 has missing address.
--'KIOSK UTC BSN' has 2 locations

SELECT brch_code, count(1) FROM bronze.branch_ref
Group by 1

SELECT brch_name, count(1) FROM bronze.branch_ref
WHERE BRCH_ZIPCODE IS NOT NULL
GROUP BY brch_name
Order by count(1)

select * from bronze.branch_ref
where brch_name = 'KIOSK UTC BSN' and BRCH_ZIPCODE IS NOT NULL

SELECT * FROM bronze.branch_ref
where brch_code = '40011'


--select distinct brch_code FROM bronze.branch_ref
--391 unique records
--It should be safe to exclude data that do not have ZipCode, 
--However, A question to the Data Engineer to explain why such data is like this would be necessary and how we can fix from source.

Similar to Select "*" INTO tbl from ... where... */

DROP TABLE IF EXISTS silver.branch_ref;
CREATE TABLE silver.branch_ref AS 
SELECT BRCH_CODE
,BRCH_NAME
,BRCH_ADD1
,BRCH_ADD2
,BRCH_ADD3
,BRCH_ADD4
,BRCH_CITY
,BRCH_STATE
,CASE WHEN LEN(CAST(BRCH_ZIPCODE AS VARCHAR(255))) = 6 then '0' + CAST(BRCH_ZIPCODE AS VARCHAR(255))
ELSE CAST(BRCH_ZIPCODE AS VARCHAR(255)) END AS BRCH_ZIPCODE
,BRCH_PHONE
,BRCH_FAX
,BRCH_LAT
,BRCH_LON FROM bronze.branch_ref
WHERE BRCH_ZIPCODE IS NOT NULL;

-- COMMAND ----------

SELECT * FROM bronze.branch_ref

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.table("bronze.credit_card")\
-- MAGIC   .withColumnRenamed("P/S", "P_S")\
-- MAGIC   .withColumnRenamed("O/S BAL", "O_S_BAL")\
-- MAGIC   .withColumnRenamed("BUMI/NBUMI", "BUMI_NBUMI")\
-- MAGIC   .write\
-- MAGIC   .format("delta")\
-- MAGIC   .mode("overwrite")\
-- MAGIC   .option("overwriteSchema", "true")\
-- MAGIC   .saveAsTable("silver.credit_card")\
-- MAGIC 
-- MAGIC #table names have invalid characters
-- MAGIC 
-- MAGIC #Data seems perfect, no null values
-- MAGIC #Contains 1 day's worth of data, transactional Table
-- MAGIC #Ready to be joined with dimensional tables.
-- MAGIC #Hash distributed table on Position_DT as it should be equally skewed.

-- COMMAND ----------

DROP TABLE IF EXISTS silver.customer;

CREATE TABLE silver.customer AS
SELECT * FROM bronze.customer


-- COMMAND ----------

DROP TABLE IF EXISTS silver.customer_contact;

CREATE TABLE silver.customer_contact AS 
WITH lead_tbl AS
(SELECT *, LEAD(seqno) OVER (PARTITION BY cust_id ORDER BY seqno) AS Flag FROM bronze.customer_contact)
SELECT CUST_ID,
SEQNO,
CHOMEP_NO,
COFFICE_NO,
COFFICE_EXT,
CMOBILE_NO,
CFAX_NO,
CEMAIL1,
CTIME_STAMP FROM lead_tbl
WHERE Flag is null


/*Show columns in bronze.customer_contact*/

/*
SELECT cust_id, count(1) from bronze.customer_contact
group by cust_id
order by count(1) desc
*/

--COFFICE_NO, COFFICE_EXT, CFAX_NO are missing. Need to reach engineers for explaination
--duplicates --SCD Type 6
--

-- COMMAND ----------


/*cast(BUMI_RACE_CD as varchar(255)) as test1,
cast(RELIGION_CD as varchar(255)) as test2,
cast(GENDER_CD as varchar(255)) as test5,
cast(OCCUPATION_TYPE_CD as varchar(255)) as test3,
cast(OCCUPATION_SECTOR_CD as varchar(255)) as test4*/

--Fix Datatype
DROP TABLE IF EXISTS silver.customer_demographic;

CREATE TABLE silver.customer_demographic AS
SELECT 
CUST_ID
,NRIC
,CUST_NAME
,BUMI_RACE_CD
,CAST(RELIGION_CD AS INT) AS RELIGION_CD
,GENDER_CD
,OCCUPATION_TYPE_CD
,OCCUPATION_SECTOR_CD
FROM bronze.customer_demographic



--Table looks good to be ingested to Silver
--However not sure why the profiler say that there are 4983 unique instead of 5124. hmm



-- COMMAND ----------

DROP TABLE IF EXISTS silver.deposits;

CREATE TABLE silver.deposits AS
SELECT
CUST_ID
,ACTP_TYPE
,BRCH_CODE
,CACC_NUM
,CACC_AVAIL_BAL
,CACC_OPENING_DATE
,CACC_STATUS
,APRP_CODE

FROM bronze.deposit

/*
Append in futher future
INSERT INTO silver.deposits
SELECT
CUST_ID
,ACTP_TYPE
,BRCH_CODE
,CACC_NUM
,CACC_AVAIL_BAL
,CACC_OPENING_DATE
,CACC_STATUS
,APRP_CODE

FROM bronze.deposit
*/

--CACC_OLD_NUM, CNAT_CODE can be removed

-- COMMAND ----------

/*Convert to Standard Col names*/
DROP TABLE
IF EXISTS silver.insurance

Create TABLE silver.insurance AS

SELECT 
POLICY
,ProposerIC_NO
,Proposer
,IC_NO
,Product
,`Assured Age` AS Assured_Age
,Race
,Gender
,Address
,`Proposal Dt` AS Proposal_Dt
,`HO Dt` AS HO_Dt
,`Entry Dt` AS Entry_Dt
,`Payment Mode` AS Payment_Mode
,`BASIC R(FYAP)` AS Basic_R_FYAP
,`Policy Commencement Date` AS Policy_Commencement_Date
,`Credit card number` AS Credit_card_number
,`Occupation description` AS Occupation_description

FROM bronze.insurance

--Remove for now Payment type, AutoDebit Number, Policyowner Phone 1, Policyowner Phone 2,Policyowner Phone 3

-- COMMAND ----------

DROP TABLE IF EXISTS silver.loan;

CREATE TABLE silver.loan AS
SELECT 
ILOM_SEQUENCE
,PRODUCT_CODE
,BRANCH_CODE
,CURRENCY_CD
,CUST_ID
,LOAN_REASON_CD
,LOAN_STATUS_CODE
,LOAN_AMOUNT
,APPLICATION_DT
,INT_RATE_AT_APPLICATION
,REPAYMENT_FREQUENCY
,MONTHLY_INSTALLMENT
,TENURE
,FIRST_PAYMENT_DATE
,LAST_PAYMENT_DATE
,SUBPRODUCT_CODE
,RATE_CD
,ACC_ADDR_SEQNO

FROM bronze.loan



--ICBS_ACCOUNT_NO, WRITE_OFF_DATE, WRITE_OFF_AMT, BILLING_ACCT
--LOAN = 0? PAID OFF RIGHT?, create 2 approach to this.
--18765712684609156 MISSING BRANCH
--INT_RATE HAS THE .00000..4 ISSUE. WILL AFFECT INTEREST RATES IF USED TO CALCULATE. WILL NEED TO CHECK WITH FINANCE
--Should have Branch_Code as Numeric, not the name


-- COMMAND ----------

DROP TABLE IF EXISTS silver.main_ref;

CREATE TABLE silver.main_ref AS 
SELECT * FROM bronze.main_ref;

--there are 2 methods, either preserve the structure, or split the table, since im not a Purist, I will not go with splitting.
--for Power BI, it would be necessary to split the table. as the Fact will not be able to recreate a matching key
--for joins, we will use a where clause on the variable


-- COMMAND ----------

DROP TABLE IF EXISTS silver.product_ref;

CREATE TABLE silver.product_ref AS 
SELECT * FROM bronze.product_ref;

--CODE1 -> for Deposit_Code, may need to change as the code does not make sense. use the rep function to remove decimal
--We shall see how it is being used when joining to fact tables. for now we will store it as it is.


-- COMMAND ----------

DROP TABLE IF EXISTS silver.takaful;

CREATE TABLE silver.takaful AS
SELECT 
POLICY_NO
,PROP_NO
,`AGENT CODE` AS AGENT_CODE
,TAKAFUL_SPECIALIST
,`CUSTOMER IC` AS CUSTOMER_IC
,`CUSTOMER NAME` AS CUSTOMER_NAME
,COMMENCED_DATE
,STATE
,OCCUPATION
,GENDER
,`MONTHLY CONTRIBUTION` AS MONTHLY_CONTRIBUTION
,`ANNUAL CONTRIBUTION` AS ANNUAL_CONTRIBUTION
,RACE
,`MARITAL STATUS` AS MARITAL_STATUS
,`SUM ASSURED` AS SUM_ASSURED
,`PAYMENT METHOD` AS PAYMENT_METHOD
,PRODUCT
FROM bronze.takaful;

--payment frequency can be removed for this case study

