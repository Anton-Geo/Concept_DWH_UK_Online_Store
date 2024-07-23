/*
---------------------------------------------------------------------- 
TRUNCATE TABLE "BL_DM"."FCT_SALES_DD" CASCADE;
ALTER SEQUENCE "BL_DM".fct_sales_seq RESTART WITH 1;
----------------------------------------------------------------------
 */ 

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_TO_FCT_SALES_DD"()
LANGUAGE plpgsql
AS
$$
DECLARE
    v_initial_rows  INT := 0;    
    v_inserted_rows INT := 0;
    v_updated_rows  INT := 0;
    v_error_stack   TEXT;
    v_success_flag  BOOLEAN := TRUE;
    v_start_time    TIMESTAMP;
    v_end_time      TIMESTAMP;
BEGIN
	SELECT COUNT(*) INTO v_initial_rows
	  FROM "BL_DM"."FCT_SALES_DD";
	 
	v_start_time := clock_timestamp();
    BEGIN
		MERGE INTO "BL_DM"."FCT_SALES_DD" AS target
		USING (
			SELECT DISTINCT
			       ce."SALE_ID"                AS "SALE_SRC_ID",
			       ce."SALE_DT"                AS "SALE_DT",
			       ce."BUS_SALE_ID"            AS "BUS_SALE_ID",
			       d_c."CUSTOMER_SURR_ID"      AS "CUSTOMER_SURR_ID",
			       d_p."PRODUCT_SURR_ID"       AS "PRODUCT_SURR_ID",
			       d_pt."PAYMENT_TYPE_SURR_ID" AS "PAYMENT_TYPE_SURR_ID",
			       d_pl."PLATFORM_SURR_ID"     AS "PLATFORM_SURR_ID",
			       d_p."RETAIL_COST"           AS "FCT_RETAIL_COST_GBP",
			       ce."SALES_PRICE"            AS "FCT_SALE_PRICE_GBP",
			       ce."QUANTITY"               AS "FCT_QUANTITY",
			       ce."QUANTITY" * (ce."SALES_PRICE")                     AS "FCT_REVENUE_GBP",
			       ce."QUANTITY" * (ce."SALES_PRICE" - d_p."RETAIL_COST") AS "FCT_PROFIT_GBP",
			       'BL_3NF'                    AS "SOURCE_SYSTEM",
			       'CE_SALES'                  AS "SOURCE_ENTITY"
			  FROM "BL_3NF"."CE_SALES"                   AS ce
			       LEFT JOIN "BL_DM"."DIM_CUSTOMERS"     AS d_c
			       ON ce."CUSTOMER_ID"     = d_c."CUSTOMER_SRC_ID"
			       
			       LEFT JOIN "BL_DM"."DIM_PRODUCTS_SCD"  AS d_p
			       ON ce."PRODUCT_ID"      = d_p."PRODUCT_SRC_ID"
			       
			       LEFT JOIN "BL_DM"."DIM_PAYMENT_TYPES" AS d_pt
			       ON ce."PAYMENT_TYPE_ID" = d_pt."PAYMENT_TYPE_SRC_ID"
			       
			       LEFT JOIN "BL_DM"."DIM_PLATFORMS"     AS d_pl
			       ON ce."PLATFORM_ID"     = d_pl."PLATFORM_SRC_ID"
			 WHERE ce."SALE_ID" != -1
			 ORDER BY "SALE_SRC_ID"
		) AS src
		ON  target."SALE_SRC_ID" = src."SALE_SRC_ID"
		WHEN NOT MATCHED THEN
		    INSERT (
			    "SALE_SURR_ID",
			    "SALE_SRC_ID",
			    "SALE_DT",
			    "BUS_SALE_ID",
			    "CUSTOMER_SURR_ID",
			    "PRODUCT_SURR_ID",
			    "PAYMENT_TYPE_SURR_ID",
			    "PLATFORM_SURR_ID",
			    "FCT_RETAIL_COST_GBP",
			    "FCT_SALE_PRICE_GBP",
			    "FCT_QUANTITY",
			    "FCT_REVENUE_GBP",
			    "FCT_PROFIT_GBP",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_DM".fct_sales_seq'),
		        src."SALE_SRC_ID",
		        src."SALE_DT",
		        src."BUS_SALE_ID",
		        src."CUSTOMER_SURR_ID",
		        src."PRODUCT_SURR_ID",
		        src."PAYMENT_TYPE_SURR_ID",
		        src."PLATFORM_SURR_ID",
		        src."FCT_RETAIL_COST_GBP",
		        src."FCT_SALE_PRICE_GBP",
		        src."FCT_QUANTITY",
		        src."FCT_REVENUE_GBP",
		        src."FCT_PROFIT_GBP",
		        src."SOURCE_SYSTEM",
		        src."SOURCE_ENTITY",
		        NOW() AT TIME ZONE 'UTC',
		        NOW() AT TIME ZONE 'UTC'
			)
		;
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "BL_DM"."FCT_SALES_DD";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;

    RAISE NOTICE 'Numbers of inserted/updated rows to FCT_SALES_DD: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_TO_FCT_SALES_DD',
	    'BL_DM.FCT_SALES_DD',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;