SELECT *
  FROM "BL_3NF"."CE_CUSTOMERS" AS cust
       LEFT JOIN "BL_3NF"."CE_COUNTRIES" AS cntr
       ON cust."COUNTRY_ID" = cntr."COUNTRY_ID"
 WHERE "CUSTOMER_NAME"    = 'Jacara'
   AND "CUSTOMER_SURNAME" = 'Louramore';
   
SELECT *
FROM (
    SELECT 
        *,
        COUNT(*) OVER (PARTITION BY "BUS_CUSTOMER_ID") AS duplicate_count
    FROM 
        "BL_3NF"."CE_CUSTOMERS"
) subquery
WHERE duplicate_count > 1
;
 
SELECT *
FROM (
    SELECT 
        *,
        COUNT(*) OVER (PARTITION BY "PRODUCT_CODE") AS duplicate_count
    FROM 
        "BL_3NF"."CE_PRODUCTS_SCD"
) subquery
WHERE duplicate_count > 1
   
SELECT *
  FROM "BL_CL"."WRK_PRODUCTS_SCD"
 WHERE "PRODUCT_CODE" IN ('10002', '10080', '10120', '10123C')
 ORDER BY "PRODUCT_CODE", "PRODUCT_SURR_ID", "START_DT";

SELECT *
  FROM "BL_3NF"."CE_PRODUCTS_SCD"
 WHERE "PRODUCT_CODE" IN ('10002', '10080', '10120', '10123C')
 ORDER BY "PRODUCT_CODE", "PRODUCT_ID", "START_DT";
 
SELECT *
  FROM "BL_DM"."DIM_PRODUCTS_SCD"
 WHERE "PRODUCT_CODE" IN ('10002', '10080', '10120', '10123C')
 ORDER BY "PRODUCT_CODE", "PRODUCT_SURR_ID", "START_DT";


CALL "BL_CL".prc_create_combined_source_data();
ANALYZE "BL_CL".combined_source_data;

SELECT *
  FROM "BL_CL".fnc_get_column_stats('BL_CL', 'combined_source_data');
 
/*
column_title       |unique_count|null_count|total_count|column_data_type |
-------------------+------------+----------+-----------+-----------------+
invoiceno          |     1073672|         0|    1073672|character varying|
invoicedate        |         622|         0|    1073672|character varying|
customerid         |        4334|         0|    1073672|character varying|
customername       |        3872|         0|    1073672|character varying|
customersurname    |        4237|         0|    1073672|character varying|
phonenumber        |        4335|         0|    1073672|character varying|
country            |          37|         0|    1073672|character varying|
countrysubregion   |          12|    360971|    1073672|character varying|
countryregion      |           6|    360971|    1073672|character varying|
stockcode          |        3626|         0|    1073672|character varying|
productname        |        3626|         0|    1073672|character varying|
description        |        3608|         0|    1073672|character varying|
productcategory    |         175|         0|    1073672|character varying|
mainproductcategory|           8|    712701|    1073672|character varying|
unitcost           |         683|         0|    1073672|character varying|
unitprice          |         227|         0|    1073672|character varying|
quantity           |         287|         0|    1073672|character varying|
salesplatform      |           3|         0|    1073672|character varying|
paymentmethod      |           3|         0|    1073672|character varying|
insertdateproduct  |           2|         0|    1073672|character varying|
enddateproduct     |           1|         0|    1073672|character varying|
 */
 
SELECT *
  FROM "BL_CL".fnc_get_column_stats('BL_DM', 'FCT_SALES_DD');
 
/*
column_title        |unique_count|null_count|total_count|column_data_type           |
--------------------+------------+----------+-----------+---------------------------+
SALE_SURR_ID        |     1073672|         0|    1073672|bigint                     |
SALE_SRC_ID         |     1073672|         0|    1073672|bigint                     |
BUS_SALE_ID         |     1073672|         0|    1073672|character varying          |
SALE_DT             |         622|         0|    1073672|date                       |
CUSTOMER_SURR_ID    |        4334|         0|    1073672|bigint                     |
PRODUCT_SURR_ID     |        3630|         0|    1073672|bigint                     |
PAYMENT_TYPE_SURR_ID|           3|         0|    1073672|integer                    |
PLATFORM_SURR_ID    |           3|         0|    1073672|integer                    |
FCT_RETAIL_COST_GBP |         683|         0|    1073672|numeric                    |
FCT_SALE_PRICE_GBP  |         227|         0|    1073672|numeric                    |
FCT_QUANTITY        |         287|         0|    1073672|integer                    |
FCT_REVENUE_GBP     |        4364|         0|    1073672|numeric                    |
FCT_PROFIT_GBP      |        4559|         0|    1073672|numeric                    |
SOURCE_SYSTEM       |           1|         0|    1073672|character varying          |
SOURCE_ENTITY       |           1|         0|    1073672|character varying          |
TA_INSERT_DT        |           2|         0|    1073672|timestamp without time zone|
TA_UPDATE_DT        |           2|         0|    1073672|timestamp without time zone|
 */ 
 
DROP TABLE IF EXISTS "BL_CL".combined_source_data;