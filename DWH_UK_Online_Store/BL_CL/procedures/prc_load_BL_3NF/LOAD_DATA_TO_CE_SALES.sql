-- Population "BL_3NF"."CE_SALES":

/*
---------------------------------------------------------------------- 
TRUNCATE TABLE "BL_3NF"."CE_SALES" CASCADE;
ALTER SEQUENCE "BL_3NF".ce_sales_seq RESTART WITH 1;
----------------------------------------------------------------------
 */ 

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_TO_CE_SALES"()
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
	  FROM "BL_3NF"."CE_SALES";
	 
	v_start_time := clock_timestamp();
    BEGIN
		MERGE INTO "BL_3NF"."CE_SALES" AS target
		USING (
			SELECT DISTINCT
			       wrk."SALE_SURR_ID"        AS "SALE_SRC_ID",
			       wrk."SALE_DT"             AS "SALE_DT",
			       wrk."BUS_SALE_ID"         AS "BUS_SALE_ID",
			       ce_cust."CUSTOMER_ID"     AS "CUSTOMER_ID",
			       ce_prod."PRODUCT_ID"      AS "PRODUCT_ID",
			       ce_paym."PAYMENT_TYPE_ID" AS "PAYMENT_TYPE_ID",
			       ce_platf."PLATFORM_ID"    AS "PLATFORM_ID",
			       wrk."SALES_PRICE"         AS "SALES_PRICE",
			       wrk."QUANTITY"            AS "QUANTITY",
			       'BL_CL'                   AS "SOURCE_SYSTEM",
			       'WRK_SALES'               AS "SOURCE_ENTITY"
			  FROM "BL_CL"."WRK_SALES" AS wrk
			       LEFT JOIN "BL_3NF"."CE_CUSTOMERS" AS ce_cust
			       ON wrk."BUS_CUSTOMER_ID"   = ce_cust."BUS_CUSTOMER_ID"
			       
			       LEFT JOIN "BL_3NF"."CE_PRODUCTS_SCD" AS ce_prod
			       ON  wrk."PRODUCT_CODE"     = ce_prod."PRODUCT_CODE"
			       AND wrk."START_DT"         = ce_prod."START_DT"
			       
			       LEFT JOIN "BL_3NF"."CE_PAYMENT_TYPES" AS ce_paym
			       ON wrk."PAYMENT_TYPE_NAME" = ce_paym."PAYMENT_TYPE_NAME"
			       
			       LEFT JOIN "BL_3NF"."CE_PLATFORMS" AS ce_platf
			       ON wrk."PLATFORM_NAME"     = ce_platf."PLATFORM_NAME"
			 ORDER BY "SALE_SRC_ID"
		) AS src
		ON  target."SALE_SRC_ID"     = src."SALE_SRC_ID"
		AND target."SALE_DT"         = src."SALE_DT"
		AND target."CUSTOMER_ID"     = src."CUSTOMER_ID"
		AND target."PRODUCT_ID"      = src."PRODUCT_ID"
		AND target."PAYMENT_TYPE_ID" = src."PAYMENT_TYPE_ID"
		AND target."PLATFORM_ID"     = src."PLATFORM_ID"
		WHEN NOT MATCHED THEN
		    INSERT (
			    "SALE_ID",
			    "SALE_SRC_ID",
			    "SALE_DT",
			    "BUS_SALE_ID",
			    "CUSTOMER_ID",
			    "PRODUCT_ID",
			    "PAYMENT_TYPE_ID",
			    "PLATFORM_ID",
			    "SALES_PRICE",
			    "QUANTITY",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_3NF".ce_sales_seq'),
		        src."SALE_SRC_ID",
		        src."SALE_DT",
		        src."BUS_SALE_ID",
		        src."CUSTOMER_ID",
		        src."PRODUCT_ID",
		        src."PAYMENT_TYPE_ID",
		        src."PLATFORM_ID",
		        src."SALES_PRICE",
		        src."QUANTITY",
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
	  FROM "BL_3NF"."CE_SALES";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;

    RAISE NOTICE 'Numbers of inserted/updated rows to CE_SALES: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_TO_CE_SALES',
	    'BL_3NF.CE_SALES',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;