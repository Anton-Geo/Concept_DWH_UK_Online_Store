-----------------------------------------------------
/*
TRUNCATE TABLE "BL_CL"."WRK_PRODUCT_SUBCATEGORIES";
ALTER SEQUENCE "BL_CL".wrk_product_subcategory_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the WRK_PRODUCT_SUBCATEGORIES table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_FROM_EU_TO_WRK_PRODUCT_SUBCATEGORIES"()
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
	  FROM "BL_CL"."WRK_PRODUCT_SUBCATEGORIES";
	 
	v_start_time := clock_timestamp();
    BEGIN
		MERGE INTO "BL_CL"."WRK_PRODUCT_SUBCATEGORIES" AS target
		USING (
		    SELECT DISTINCT
		           FIRST_VALUE(ProductCategory) OVER w AS prod_subcateg,
		           FIRST_VALUE(ProductCategory) OVER w AS prod_subcateg_name,
		           'SA_EUROPE_SERVER_SALES'            AS source_system,
		           'SRC_EUROPE_SALES'                  AS source_entity
		      FROM "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES"
		    WINDOW w AS (PARTITION BY ProductCategory ORDER BY ProductCategory)
		) AS src
		ON  target."PRODUCT_SUBCATEGORY_SRC_ID" = src.prod_subcateg
		WHEN NOT MATCHED THEN
		    INSERT (
			    "PRODUCT_SUBCATEGORY_SURR_ID",
			    "PRODUCT_SUBCATEGORY_SRC_ID",
			    "PRODUCT_SUBCATEGORY_NAME",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_CL".wrk_product_subcategory_seq'),
		        src.prod_subcateg,
		        src.prod_subcateg_name,
		        src.source_system,
		        src.source_entity,
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
	  FROM "BL_CL"."WRK_PRODUCT_SUBCATEGORIES";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;

    RAISE NOTICE 'Numbers of inserted/updated rows to WRK_PRODUCT_SUBCATEGORIES: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_FROM_EU_TO_WRK_PRODUCT_SUBCATEGORIES',
	    'BL_CL.WRK_PRODUCT_SUBCATEGORIES',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;