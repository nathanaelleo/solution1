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

--Normally Fact Tables should not have Desc. Since Desc is in the table, join is not necessary

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


