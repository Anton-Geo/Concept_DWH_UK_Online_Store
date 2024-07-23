-----------------------------------------------------
/*
TRUNCATE TABLE "BL_3NF"."CE_PRODUCT_SUBCATEGORIES" CASCADE;
ALTER SEQUENCE "BL_3NF".ce_product_subcategory_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the CE_PRODUCT_SUBCATEGORIES table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_TO_CE_PRODUCT_SUBCATEGORIES"()
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
	  FROM "BL_3NF"."CE_PRODUCT_SUBCATEGORIES";
	 
	v_start_time := clock_timestamp();
    BEGIN
		MERGE INTO "BL_3NF"."CE_PRODUCT_SUBCATEGORIES" AS target
		USING (
			SELECT DISTINCT
			       wrk."PRODUCT_SUBCATEGORY_SURR_ID" AS "PRODUCT_SUBCATEGORY_SRC_ID",
			       wrk."PRODUCT_SUBCATEGORY_NAME"    AS "PRODUCT_SUBCATEGORY_NAME",
			       COALESCE(ce_p."PRODUCT_CATEGORY_ID", -1 ) AS "PRODUCT_CATEGORY_ID",
			       'BL_CL'                           AS "SOURCE_SYSTEM",
			       'WRK_PRODUCT_SUBCATEGORIES'       AS "SOURCE_ENTITY"
			  FROM "BL_CL"."WRK_PRODUCT_SUBCATEGORIES" AS wrk
			       LEFT JOIN "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES" AS src_e
			       ON wrk."PRODUCT_SUBCATEGORY_SRC_ID" = src_e.productcategory
			       LEFT JOIN "BL_CL"."WRK_PRODUCT_CATEGORIES" AS wrk_p
			       ON src_e.mainproductcategory = wrk_p."PRODUCT_CATEGORY_SRC_ID"
			       LEFT JOIN "BL_3NF"."CE_PRODUCT_CATEGORIES" AS ce_p
			       ON wrk_p."PRODUCT_CATEGORY_SURR_ID" = ce_p."PRODUCT_CATEGORY_SRC_ID"
			 ORDER BY "PRODUCT_SUBCATEGORY_SRC_ID"
		) AS src
		ON target."PRODUCT_SUBCATEGORY_SRC_ID" = src."PRODUCT_SUBCATEGORY_SRC_ID"
		WHEN NOT MATCHED THEN
		    INSERT (
		    	"PRODUCT_SUBCATEGORY_ID",
			    "PRODUCT_SUBCATEGORY_SRC_ID",
			    "PRODUCT_SUBCATEGORY_NAME",
			    "PRODUCT_CATEGORY_ID",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_3NF".ce_product_subcategory_seq'),
		        src."PRODUCT_SUBCATEGORY_SRC_ID",
		        src."PRODUCT_SUBCATEGORY_NAME",
		        src."PRODUCT_CATEGORY_ID",
		        src."SOURCE_SYSTEM",
		        src."SOURCE_ENTITY",
		        NOW() AT TIME ZONE 'UTC',
		        NOW() AT TIME ZONE 'UTC'
			)
	    WHEN MATCHED THEN
	        DO NOTHING;
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "BL_3NF"."CE_PRODUCT_SUBCATEGORIES";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;

    RAISE NOTICE 'Numbers of inserted/updated rows to CE_PRODUCT_SUBCATEGORIES: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_TO_CE_PRODUCT_SUBCATEGORIES',
	    'BL_3NF.CE_PRODUCT_SUBCATEGORIES',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;