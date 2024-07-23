-----------------------------------------------------
/*
TRUNCATE TABLE "BL_CL"."WRK_PRODUCTS_SCD" CASCADE;
ALTER SEQUENCE "BL_CL".wrk_products_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the WRK_PRODUCTS_SCD table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_FROM_GLOB_TO_WRK_PRODUCTS_SCD"()
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
	  FROM "BL_CL"."WRK_PRODUCTS_SCD";

    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CREATE MATERIALIZED VIEW "BL_CL".temp_view AS
	SELECT *
	  FROM "BL_CL"."WRK_PRODUCTS_SCD";
	 
	v_start_time := clock_timestamp();
    BEGIN
	    
		MERGE INTO "BL_CL"."WRK_PRODUCTS_SCD" AS target
		USING (
		    SELECT DISTINCT
		           FIRST_VALUE(StockCode)                           OVER w AS stock_code,
		           FIRST_VALUE(ProductName)                         OVER w AS product_name,
		           FIRST_VALUE(productcategory)                     OVER w AS productcategory,
		           FIRST_VALUE(Description)                         OVER w AS product_desc,
		           FIRST_VALUE(CASE WHEN Cost ~ '^[0-9.]*$' 
		                             AND LENGTH(REPLACE(Cost, '.', '')) <= 10 THEN Cost 
		                            ELSE '-1' END)                  OVER w AS retail_cost,
		           FIRST_VALUE(CASE WHEN (InsertDateProduct ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$') 
		                            THEN InsertDateProduct::TIMESTAMP
		                            ELSE '1900-01-01 00:00:00' END) OVER w AS start_dt,            
		           FIRST_VALUE(CASE WHEN (EndDateProduct ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$') 
		                            THEN EndDateProduct::TIMESTAMP
		                            ELSE '9999-12-12 23:59:59' END) OVER w AS end_dt,                             
		           FIRST_VALUE(CASE WHEN EndDateProduct LIKE '9999-12-31 23:59:59'
		                              OR EndDateProduct > to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
		                            THEN 'Y'
		                            ELSE 'N' END)                   OVER w AS is_active,     
		           'SA_GLOBAL_SERVER_SALES'                                AS source_system,
		           'SRC_GLOBAL_SALES'                                      AS source_entity
		      FROM "SA_GLOBAL_SERVER_SALES"."SRC_GLOBAL_SALES"
		    WINDOW w AS (PARTITION BY StockCode, InsertDateProduct ORDER BY InsertDateProduct)
		) AS src
		ON  target."PRODUCT_SRC_ID" = src.stock_code
		AND target."START_DT"       < src.start_dt
		AND target."IS_ACTIVE"      = 'Y'
		WHEN MATCHED THEN
	    	UPDATE
	    	   SET "END_DT"         = src.start_dt,
                   "IS_ACTIVE"      = 'N';
                  
        MERGE INTO "BL_CL"."WRK_PRODUCTS_SCD" AS target
		USING (
		    SELECT DISTINCT
		           FIRST_VALUE(StockCode)                           OVER w AS stock_code,
		           FIRST_VALUE(ProductName)                         OVER w AS product_name,
		           FIRST_VALUE(productcategory)                     OVER w AS productcategory,
		           FIRST_VALUE(Description)                         OVER w AS product_desc,
		           FIRST_VALUE(CASE WHEN Cost ~ '^[0-9.]*$' 
		                             AND LENGTH(REPLACE(Cost, '.', '')) <= 10 THEN Cost 
		                            ELSE '-1' END)                  OVER w AS retail_cost,
		           FIRST_VALUE(CASE WHEN (InsertDateProduct ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$') 
		                            THEN InsertDateProduct::TIMESTAMP
		                            ELSE '1900-01-01 00:00:00' END) OVER w AS start_dt,            
		           FIRST_VALUE(CASE WHEN (EndDateProduct ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$') 
		                            THEN EndDateProduct::TIMESTAMP
		                            ELSE '9999-12-12 23:59:59' END) OVER w AS end_dt,                             
		           FIRST_VALUE(CASE WHEN EndDateProduct LIKE '9999-12-31 23:59:59'
		                              OR EndDateProduct > to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
		                            THEN 'Y'
		                            ELSE 'N' END)                   OVER w AS is_active,     
		           'SA_GLOBAL_SERVER_SALES'                                AS source_system,
		           'SRC_GLOBAL_SALES'                                      AS source_entity
		      FROM "SA_GLOBAL_SERVER_SALES"."SRC_GLOBAL_SALES"
		    WINDOW w AS (PARTITION BY StockCode, InsertDateProduct ORDER BY InsertDateProduct)
		) AS src
		ON  target."PRODUCT_SRC_ID" = src.stock_code
		AND target."IS_ACTIVE"      = 'Y'
        WHEN NOT MATCHED THEN
		    INSERT (
			    "PRODUCT_SURR_ID",
			    "PRODUCT_SRC_ID",
			    "PRODUCT_CODE",
			    "PRODUCT_NAME",
			    "PRODUCT_SUBCATEGORY_NAME",
			    "PRODUCT_DESC",
			    "RETAIL_COST",
			    "START_DT",
			    "END_DT",
			    "IS_ACTIVE",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_CL".wrk_products_seq'),
		        src.stock_code,
		        src.stock_code,
		        src.product_name,
		        src.productcategory,
		        src.product_desc,
		        src.retail_cost::NUMERIC(10,2),
		        src.start_dt,
		        src.end_dt,
		        src.is_active,
		        src.source_system,
		        src.source_entity,
		        NOW() AT TIME ZONE 'UTC',
		        NOW() AT TIME ZONE 'UTC'
			);
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "BL_CL"."WRK_PRODUCTS_SCD";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;
    
    SELECT COUNT(*) INTO v_updated_rows
      FROM "BL_CL"."WRK_PRODUCTS_SCD" AS i
           INNER JOIN "BL_CL".temp_view AS t
           ON  i."PRODUCT_SURR_ID" = t."PRODUCT_SURR_ID"
           AND i."IS_ACTIVE"       = 'N' 
           AND t."IS_ACTIVE"       = 'Y';

    RAISE NOTICE 'Numbers of inserted/updated rows to WRK_PRODUCTS_SCD: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
                
    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_FROM_GLOB_TO_WRK_PRODUCTS_SCD',
	    'BL_CL.WRK_PRODUCTS_SCD',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;