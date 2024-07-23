-----------------------------------------------------
/*
TRUNCATE TABLE "BL_3NF"."CE_PRODUCTS_SCD" CASCADE;
ALTER SEQUENCE "BL_3NF".ce_products_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the CE_PRODUCTS_SCD table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_TO_CE_PRODUCTS_SCD"()
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
	  FROM "BL_3NF"."CE_PRODUCTS_SCD";

    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CREATE MATERIALIZED VIEW "BL_CL".temp_view AS
	SELECT *
	  FROM "BL_3NF"."CE_PRODUCTS_SCD";
	 
	v_start_time := clock_timestamp();
    BEGIN

		MERGE INTO "BL_3NF"."CE_PRODUCTS_SCD" AS target
		USING (
			SELECT DISTINCT
			       wrk."PRODUCT_SURR_ID"  AS "PRODUCT_SRC_ID",
			       wrk."PRODUCT_CODE"     AS "PRODUCT_CODE",
			       wrk."RETAIL_COST"      AS "RETAIL_COST",
			       wrk."PRODUCT_DESC"     AS "PRODUCT_DESC",
			       COALESCE(ce_p."PRODUCT_SUBCATEGORY_ID", -1) AS "PRODUCT_SUBCATEGORY_ID",
			       wrk."START_DT"         AS "START_DT",
			       wrk."END_DT"           AS "END_DT",
			       wrk."IS_ACTIVE"        AS "IS_ACTIVE",
			       'BL_CL'                AS "SOURCE_SYSTEM",
			       'WRK_PRODUCTS_SCD'     AS "SOURCE_ENTITY"
			  FROM "BL_CL"."WRK_PRODUCTS_SCD" AS wrk			       
			       LEFT JOIN "BL_3NF"."CE_PRODUCT_SUBCATEGORIES" AS ce_p
			       ON wrk."PRODUCT_SUBCATEGORY_NAME" = ce_p."PRODUCT_SUBCATEGORY_NAME"
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

        MERGE INTO "BL_3NF"."CE_PRODUCTS_SCD" AS target
		USING (
			SELECT DISTINCT
			       wrk."PRODUCT_SURR_ID"  AS "PRODUCT_SRC_ID",
			       wrk."PRODUCT_CODE"     AS "PRODUCT_CODE",
			       wrk."PRODUCT_NAME"     AS "PRODUCT_NAME",
			       wrk."RETAIL_COST"      AS "RETAIL_COST",
			       wrk."PRODUCT_DESC"     AS "PRODUCT_DESC",
			       COALESCE(ce_p."PRODUCT_SUBCATEGORY_ID", -1) AS "PRODUCT_SUBCATEGORY_ID",
			       wrk."START_DT"         AS "START_DT",
			       wrk."END_DT"           AS "END_DT",
			       wrk."IS_ACTIVE"        AS "IS_ACTIVE",
			       'BL_CL'                AS "SOURCE_SYSTEM",
			       'WRK_PRODUCTS_SCD'     AS "SOURCE_ENTITY"
			  FROM "BL_CL"."WRK_PRODUCTS_SCD" AS wrk
			       LEFT JOIN "BL_3NF"."CE_PRODUCT_SUBCATEGORIES" AS ce_p
			       ON wrk."PRODUCT_SUBCATEGORY_NAME" = ce_p."PRODUCT_SUBCATEGORY_NAME"
			 ORDER BY "PRODUCT_SRC_ID"
		) AS src
		ON  target."PRODUCT_SRC_ID" = src."PRODUCT_SRC_ID"
		WHEN NOT MATCHED THEN
		    INSERT (
		    	"PRODUCT_ID",
			    "PRODUCT_SRC_ID",
			    "PRODUCT_CODE",
			    "PRODUCT_NAME",
			    "RETAIL_COST",
			    "PRODUCT_DESC",
			    "PRODUCT_SUBCATEGORY_ID",
			    "START_DT",
			    "END_DT",
			    "IS_ACTIVE",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_3NF".ce_products_seq'),
		        src."PRODUCT_SRC_ID",
		        src."PRODUCT_CODE",
		        src."PRODUCT_NAME",
		        src."RETAIL_COST",
		        src."PRODUCT_DESC",
		        src."PRODUCT_SUBCATEGORY_ID",
		        src."START_DT",
		        src."END_DT",
		        src."IS_ACTIVE",
		        src."SOURCE_SYSTEM",
		        src."SOURCE_ENTITY",
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
	  FROM "BL_3NF"."CE_PRODUCTS_SCD";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;
    
    SELECT COUNT(*) INTO v_updated_rows
      FROM "BL_3NF"."CE_PRODUCTS_SCD" AS i
           INNER JOIN "BL_CL".temp_view AS t
           ON  i."PRODUCT_ID"    = t."PRODUCT_ID"
           AND i."TA_UPDATE_DT" != t."TA_UPDATE_DT";

    RAISE NOTICE 'Numbers of inserted/updated rows to CE_PRODUCTS_SCD: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
                
    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'BL_CL.LOAD_DATA_TO_CE_PRODUCTS_SCD',
	    'BL_3NF.CE_PRODUCTS_SCD',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;


/*
		MERGE INTO "BL_3NF"."CE_PRODUCTS_SCD" AS target
		USING (
			SELECT DISTINCT
			       wrk."PRODUCT_SURR_ID"  AS "PRODUCT_SRC_ID",
			       wrk."PRODUCT_CODE"     AS "PRODUCT_CODE",
			       wrk."RETAIL_COST"      AS "RETAIL_COST",
			       wrk."PRODUCT_DESC"     AS "PRODUCT_DESC",
			       COALESCE(ce_p."PRODUCT_SUBCATEGORY_ID", -1) AS "PRODUCT_SUBCATEGORY_ID",
			       wrk."START_DT"         AS "START_DT",
			       wrk."END_DT"           AS "END_DT",
			       wrk."IS_ACTIVE"        AS "IS_ACTIVE",
			       'BL_CL'                AS "SOURCE_SYSTEM",
			       'WRK_PRODUCTS_SCD'     AS "SOURCE_ENTITY"
			  FROM "BL_CL"."WRK_PRODUCTS_SCD" AS wrk
			       LEFT JOIN "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES" AS src_e
			       ON  wrk."PRODUCT_SRC_ID" = src_e.stockcode
			       AND TO_CHAR(wrk."START_DT"::TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') = src_e.InsertDateProduct
			       AND wrk."SOURCE_SYSTEM"  = 'SA_EUROPE_SERVER_SALES'
			       AND wrk."SOURCE_ENTITY"  = 'SRC_EUROPE_SALES'
			       
			       LEFT JOIN "SA_GLOBAL_SERVER_SALES"."SRC_GLOBAL_SALES" AS src_g
			       ON  wrk."PRODUCT_SRC_ID" = src_g.stockcode
			       AND TO_CHAR(wrk."START_DT"::TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') = src_g.InsertDateProduct
			       AND wrk."SOURCE_SYSTEM"  = 'SA_GLOBAL_SERVER_SALES'
			       AND wrk."SOURCE_ENTITY"  = 'SRC_GLOBAL_SALES'
			       
			       LEFT JOIN "BL_CL"."WRK_PRODUCT_SUBCATEGORIES" AS wrk_p
			       ON src_e.productcategory = wrk_p."PRODUCT_SUBCATEGORY_SRC_ID"
			       
			       LEFT JOIN "BL_3NF"."CE_PRODUCT_SUBCATEGORIES" AS ce_p
			       ON wrk_p."PRODUCT_SUBCATEGORY_SURR_ID" = ce_p."PRODUCT_SUBCATEGORY_SRC_ID"
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

        MERGE INTO "BL_3NF"."CE_PRODUCTS_SCD" AS target
		USING (
			SELECT DISTINCT
			       wrk."PRODUCT_SURR_ID"  AS "PRODUCT_SRC_ID",
			       wrk."PRODUCT_CODE"     AS "PRODUCT_CODE",
			       wrk."PRODUCT_NAME"     AS "PRODUCT_NAME",
			       wrk."RETAIL_COST"      AS "RETAIL_COST",
			       wrk."PRODUCT_DESC"     AS "PRODUCT_DESC",
			       COALESCE(ce_p."PRODUCT_SUBCATEGORY_ID", -1) AS "PRODUCT_SUBCATEGORY_ID",
			       wrk."START_DT"         AS "START_DT",
			       wrk."END_DT"           AS "END_DT",
			       wrk."IS_ACTIVE"        AS "IS_ACTIVE",
			       'BL_CL'                AS "SOURCE_SYSTEM",
			       'WRK_PRODUCTS_SCD'     AS "SOURCE_ENTITY"
			  FROM "BL_CL"."WRK_PRODUCTS_SCD" AS wrk
			       LEFT JOIN "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES" AS src_e
			       ON  wrk."PRODUCT_SRC_ID" = src_e.stockcode
			       AND TO_CHAR(wrk."START_DT"::TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') = src_e.InsertDateProduct
			       AND wrk."SOURCE_SYSTEM"  = 'SA_EUROPE_SERVER_SALES'
			       AND wrk."SOURCE_ENTITY"  = 'SRC_EUROPE_SALES'
			       
			       LEFT JOIN "SA_GLOBAL_SERVER_SALES"."SRC_GLOBAL_SALES" AS src_g
			       ON  wrk."PRODUCT_SRC_ID" = src_g.stockcode
			       AND TO_CHAR(wrk."START_DT"::TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') = src_g.InsertDateProduct
			       AND wrk."SOURCE_SYSTEM"  = 'SA_GLOBAL_SERVER_SALES'
			       AND wrk."SOURCE_ENTITY"  = 'SRC_GLOBAL_SALES'
			       
			       LEFT JOIN "BL_CL"."WRK_PRODUCT_SUBCATEGORIES" AS wrk_p
			       ON src_e.productcategory = wrk_p."PRODUCT_SUBCATEGORY_SRC_ID"
			       
			       LEFT JOIN "BL_3NF"."CE_PRODUCT_SUBCATEGORIES" AS ce_p
			       ON wrk_p."PRODUCT_SUBCATEGORY_SURR_ID" = ce_p."PRODUCT_SUBCATEGORY_SRC_ID"
			 ORDER BY "PRODUCT_SRC_ID"
		) AS src
		ON  target."PRODUCT_SRC_ID" = src."PRODUCT_SRC_ID"
		WHEN NOT MATCHED THEN
		    INSERT (
		    	"PRODUCT_ID",
			    "PRODUCT_SRC_ID",
			    "PRODUCT_CODE",
			    "PRODUCT_NAME",
			    "RETAIL_COST",
			    "PRODUCT_DESC",
			    "PRODUCT_SUBCATEGORY_ID",
			    "START_DT",
			    "END_DT",
			    "IS_ACTIVE",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_3NF".ce_products_seq'),
		        src."PRODUCT_SRC_ID",
		        src."PRODUCT_CODE",
		        src."PRODUCT_NAME",
		        src."RETAIL_COST",
		        src."PRODUCT_DESC",
		        src."PRODUCT_SUBCATEGORY_ID",
		        src."START_DT",
		        src."END_DT",
		        src."IS_ACTIVE",
		        src."SOURCE_SYSTEM",
		        src."SOURCE_ENTITY",
		        NOW() AT TIME ZONE 'UTC',
		        NOW() AT TIME ZONE 'UTC'
			);
 */

