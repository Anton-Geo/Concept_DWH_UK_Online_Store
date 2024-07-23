-----------------------------------------------------
/*
TRUNCATE TABLE "BL_DM"."DIM_PRODUCTS_SCD" CASCADE;
ALTER SEQUENCE "BL_DM".dm_products_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the "BL_DM"."DIM_PRODUCTS_SCD" table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_TO_DIM_PRODUCTS_SCD"()
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
	  FROM "BL_DM"."DIM_PRODUCTS_SCD";

    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CREATE MATERIALIZED VIEW "BL_CL".temp_view AS
	SELECT *
	  FROM "BL_DM"."DIM_PRODUCTS_SCD";
	 
	v_start_time := clock_timestamp();
    BEGIN
		MERGE INTO "BL_DM"."DIM_PRODUCTS_SCD" AS target
		USING (
			SELECT DISTINCT
			       ce."PRODUCT_ID"                   AS "PRODUCT_SRC_ID",
			       ce."PRODUCT_CODE"                 AS "PRODUCT_CODE",
			       ce."PRODUCT_NAME"                 AS "PRODUCT_NAME",
			       ce."RETAIL_COST"                  AS "RETAIL_COST",
			       ce."PRODUCT_DESC"                 AS "PRODUCT_DESC",
			       ce."PRODUCT_SUBCATEGORY_ID"       AS "PRODUCT_SUBCATEGORY_ID",
			       subcat."PRODUCT_SUBCATEGORY_NAME" AS "PRODUCT_SUBCATEGORY_NAME",
			       subcat."PRODUCT_CATEGORY_ID"      AS "PRODUCT_CATEGORY_ID",
			       cat."PRODUCT_CATEGORY_NAME"       AS "PRODUCT_CATEGORY_NAME",
			       ce."START_DT"                     AS "START_DT",
			       ce."END_DT"                       AS "END_DT",
			       ce."IS_ACTIVE"                    AS "IS_ACTIVE",
			       'BL_3NF'                          AS "SOURCE_SYSTEM",
			       'CE_PRODUCTS_SCD'                 AS "SOURCE_ENTITY"
			  FROM "BL_3NF"."CE_PRODUCTS_SCD"                    AS ce
			       LEFT JOIN "BL_3NF"."CE_PRODUCT_SUBCATEGORIES" AS subcat
			       ON ce."PRODUCT_SUBCATEGORY_ID"= subcat."PRODUCT_SUBCATEGORY_ID"
			       LEFT JOIN "BL_3NF"."CE_PRODUCT_CATEGORIES"    AS cat
			       ON subcat."PRODUCT_CATEGORY_ID"= cat."PRODUCT_CATEGORY_ID"
		     WHERE ce."PRODUCT_ID" != -1
			 ORDER BY "PRODUCT_SRC_ID"
		) AS src
		ON  target."PRODUCT_CODE"   = src."PRODUCT_CODE"
		AND target."START_DT"       < src."START_DT"
		AND target."IS_ACTIVE"      = 'Y'
		WHEN MATCHED THEN
	    	UPDATE
	    	   SET "END_DT"       = src."START_DT",
                   "IS_ACTIVE"    = 'N',
                   "TA_UPDATE_DT" = NOW() AT TIME ZONE 'UTC';

		MERGE INTO "BL_DM"."DIM_PRODUCTS_SCD" AS target
		USING (
			SELECT DISTINCT
			       ce."PRODUCT_ID"                   AS "PRODUCT_SRC_ID",
			       ce."PRODUCT_CODE"                 AS "PRODUCT_CODE",
			       ce."PRODUCT_NAME"                 AS "PRODUCT_NAME",
			       ce."RETAIL_COST"                  AS "RETAIL_COST",
			       ce."PRODUCT_DESC"                 AS "PRODUCT_DESC",
			       ce."PRODUCT_SUBCATEGORY_ID"       AS "PRODUCT_SUBCATEGORY_ID",
			       subcat."PRODUCT_SUBCATEGORY_NAME" AS "PRODUCT_SUBCATEGORY_NAME",
			       subcat."PRODUCT_CATEGORY_ID"      AS "PRODUCT_CATEGORY_ID",
			       cat."PRODUCT_CATEGORY_NAME"       AS "PRODUCT_CATEGORY_NAME",
			       ce."START_DT"                     AS "START_DT",
			       ce."END_DT"                       AS "END_DT",
			       ce."IS_ACTIVE"                    AS "IS_ACTIVE",
			       'BL_3NF'                          AS "SOURCE_SYSTEM",
			       'CE_PRODUCTS_SCD'                 AS "SOURCE_ENTITY"
			  FROM "BL_3NF"."CE_PRODUCTS_SCD"                    AS ce
			       LEFT JOIN "BL_3NF"."CE_PRODUCT_SUBCATEGORIES" AS subcat
			       ON ce."PRODUCT_SUBCATEGORY_ID"= subcat."PRODUCT_SUBCATEGORY_ID"
			       LEFT JOIN "BL_3NF"."CE_PRODUCT_CATEGORIES"    AS cat
			       ON subcat."PRODUCT_CATEGORY_ID"= cat."PRODUCT_CATEGORY_ID"
		     WHERE ce."PRODUCT_ID" != -1
			 ORDER BY "PRODUCT_SRC_ID"
		) AS src
		ON  target."PRODUCT_SRC_ID" = src."PRODUCT_SRC_ID"
		WHEN NOT MATCHED THEN
		    INSERT (
		    	"PRODUCT_SURR_ID",
			    "PRODUCT_SRC_ID",
			    "PRODUCT_CODE",
			    "PRODUCT_NAME",
			    "RETAIL_COST",
			    "PRODUCT_DESC",
			    "PRODUCT_SUBCATEGORY_ID",
			    "PRODUCT_SUBCATEGORY_NAME",
			    "PRODUCT_CATEGORY_ID",
			    "PRODUCT_CATEGORY_NAME",
			    "START_DT",
			    "END_DT",
			    "IS_ACTIVE",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_DM".dm_products_seq'),
		        src."PRODUCT_SRC_ID",
		        src."PRODUCT_CODE",
		        src."PRODUCT_NAME",
		        src."RETAIL_COST",
		        src."PRODUCT_DESC",
		        src."PRODUCT_SUBCATEGORY_ID",
		        src."PRODUCT_SUBCATEGORY_NAME",
		        src."PRODUCT_CATEGORY_ID",
		        src."PRODUCT_CATEGORY_NAME",
		        src."START_DT",
		        src."END_DT",
		        src."IS_ACTIVE",
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
	  FROM "BL_DM"."DIM_PRODUCTS_SCD";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;
    
    SELECT COUNT(*) INTO v_updated_rows
      FROM "BL_DM"."DIM_PRODUCTS_SCD" AS i
           INNER JOIN "BL_CL".temp_view AS t
           ON  i."PRODUCT_SURR_ID" = t."PRODUCT_SURR_ID"
           AND i."TA_UPDATE_DT"   != t."TA_UPDATE_DT";

    RAISE NOTICE 'Numbers of inserted/updated rows to DIM_PRODUCTS_SCD: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
                
    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_TO_DIM_PRODUCTS_SCD',
	    'BL_DM.DIM_PRODUCTS_SCD',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;