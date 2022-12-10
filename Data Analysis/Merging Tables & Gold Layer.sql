-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Question 5: 
-- MAGIC ###a. Number of Insurance policy and sum assured by product type

-- COMMAND ----------

SHOW TABLES IN silver

/*
Dimension Tables:
    agent_ref
    branch_ref
    customer_contact
    customer_demographic
    main_ref
    product_ref
    
Fact Tables:
    loan
    insurance
    takaful
    deposits
    credit_card
*/

-- COMMAND ----------

/*
SELECT DISTINCT * FROM silver.product_ref
WHERE VARIABLE = 'TAKAFUL_CODE'
*/

--Normally Fact Tables should not have Description. Since Description is in the table, join is not necessary

DROP TABLE IF EXISTS gold.Question5_a;

--TAKAFUL, policy_no is distinct
CREATE TABLE gold.Question5_a AS 
SELECT Product AS PRODUCT_TYPE
,SUM(SUM_ASSURED) AS TOTAL_SUM_ASSURED
,COUNT(1) AS TOTAL_COUNT_OF_POLICY
,'TAKAFUL' AS TYPE_OF_INSURANCE
FROM silver.takaful
GROUP BY PRODUCT_TYPE

UNION

--CONVENTIONAL, policy_no is distinct, however there is no data for SUM_Assured
SELECT PRODUCT AS PRODUCT_TYPE
,0 AS TOTAL_SUM_ASSURED
,COUNT(1) AS TOTAL_COUNT_OF_POLICY
,'CONVENTIONAL' AS TYPE_OF_INSURANCE
FROM silver.insurance
GROUP BY PRODUCT


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Question 5: 
-- MAGIC ###b. Loans summary by branch, product type and sub-type

-- COMMAND ----------

DROP TABLE IF EXISTS gold.Question5_b;

CREATE TABLE gold.Question5_b AS 

WITH main_table as (
SELECT BRANCH_CODE, PRODUCT_CODE, SUBPRODUCT_CODE, SUM(LOAN_AMOUNT) AS TOTAL_LOAN_AMOUNT
FROM silver.loan
GROUP BY PRODUCT_CODE, SUBPRODUCT_CODE, BRANCH_CODE
ORDER BY BRANCH_CODE
)
SELECT T2.DESC1, T2.DESC2, T1.BRANCH_CODE, T1.TOTAL_LOAN_AMOUNT
FROM main_table T1

LEFT JOIN
(SELECT CODE1, CODE2, DESC1, DESC2 FROM silver.product_ref
WHERE VARIABLE = 'LOAN_CODE') T2 
ON T1.SUBPRODUCT_CODE = T2.CODE2

--REMOVE ZEROES MAYBE, UNLESS HISTORICAL RECORDS ARE NECESSARY
--IDENTIFY THE MISSING BRANCH CODE


-- COMMAND ----------

SELECT DISTINCT *, if(CODE1 = LEFT(CODE2,6), 'yes', 'no') as Codependency FROM silver.product_ref
where variable = 'LOAN_CODE' and
if(CODE1 = LEFT(CODE2,6), 'yes', 'no') = 'no'
;

--34 Distinct 

SELECT if(product_code = LEFT(subproduct_code,6), 'yes', 'no') from silver.loan
where if(product_code = LEFT(subproduct_code,6), 'yes', 'no') = 'no';


select * from silver.product_ref
--10 Types of product code
--33 Types of sub prod

--Does Code2 depend on Code1?



-- COMMAND ----------


