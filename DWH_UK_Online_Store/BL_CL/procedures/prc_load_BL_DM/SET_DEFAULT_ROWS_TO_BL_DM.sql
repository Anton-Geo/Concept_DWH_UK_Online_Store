--------- Procedure to loading default row in the DIM_PAYMENT_TYPES table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."SET_DEFAULT_ROW_TO_DIM_PAYMENT_TYPES"()
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
	  FROM "BL_DM"."DIM_PAYMENT_TYPES";

    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CREATE MATERIALIZED VIEW "BL_CL".temp_view AS
	SELECT *
	  FROM "BL_DM"."DIM_PAYMENT_TYPES";
	 
	v_start_time := clock_timestamp();
    BEGIN
		INSERT INTO "BL_DM"."DIM_PAYMENT_TYPES"(
		    "PAYMENT_TYPE_SURR_ID",
		    "PAYMENT_TYPE_SRC_ID",
		    "PAYMENT_TYPE_NAME",
		    "SOURCE_SYSTEM",
		    "SOURCE_ENTITY",
		    "TA_INSERT_DT",
		    "TA_UPDATE_DT"
		)
		SELECT -1,
		       -1,
		       'n. a.',
		       'MANUAL',
		       'MANUAL',
		       '1900-01-01 00:00:00',
		       '1900-01-01 00:00:00'
		WHERE NOT EXISTS (
		    SELECT 1
		      FROM "BL_DM"."DIM_PAYMENT_TYPES"
		     WHERE "PAYMENT_TYPE_SURR_ID" = -1
		);
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "BL_DM"."DIM_PAYMENT_TYPES";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;
    
    SELECT COUNT(*) INTO v_updated_rows
      FROM "BL_DM"."DIM_PAYMENT_TYPES" AS i
           INNER JOIN "BL_CL".temp_view AS t
           ON  i."PAYMENT_TYPE_SURR_ID" = t."PAYMENT_TYPE_SURR_ID"
           AND i."TA_UPDATE_DT"        != t."TA_UPDATE_DT";

    RAISE NOTICE 'Numbers of inserted/updated rows to DIM_PAYMENT_TYPES: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
                
    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'BL_CL"."SET_DEFAULT_ROW_TO_DIM_PAYMENT_TYPES',
	    'BL_CL.DIM_PAYMENT_TYPES',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;


--------- Procedure to loading default row in the DIM_PLATFORMS table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."SET_DEFAULT_ROW_TO_DIM_PLATFORMS"()
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
	  FROM "BL_DM"."DIM_PLATFORMS";

    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CREATE MATERIALIZED VIEW "BL_CL".temp_view AS
	SELECT *
	  FROM "BL_DM"."DIM_PLATFORMS";
	 
	v_start_time := clock_timestamp();
    BEGIN
		INSERT INTO "BL_DM"."DIM_PLATFORMS"(
		    "PLATFORM_SURR_ID",
		    "PLATFORM_SRC_ID",
		    "PLATFORM_NAME",
		    "SOURCE_SYSTEM",
		    "SOURCE_ENTITY",
		    "TA_INSERT_DT",
		    "TA_UPDATE_DT"
		)
		SELECT -1,
		       -1,
		       'n. a.',
		       'MANUAL',
		       'MANUAL',
		       '1900-01-01 00:00:00',
		       '1900-01-01 00:00:00'
		WHERE NOT EXISTS (
		    SELECT 1
		      FROM "BL_DM"."DIM_PLATFORMS"
		     WHERE "PLATFORM_SURR_ID" = -1
		);
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "BL_DM"."DIM_PLATFORMS";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;
    
    SELECT COUNT(*) INTO v_updated_rows
      FROM "BL_DM"."DIM_PLATFORMS" AS i
           INNER JOIN "BL_CL".temp_view AS t
           ON  i."PLATFORM_SURR_ID" = t."PLATFORM_SURR_ID"
           AND i."TA_UPDATE_DT"    != t."TA_UPDATE_DT";

    RAISE NOTICE 'Numbers of inserted/updated rows to DIM_PLATFORMS: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
                
    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'BL_CL"."SET_DEFAULT_ROW_TO_DIM_PLATFORMS',
	    'BL_CL.DIM_PLATFORMS',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;


--------- Procedure to loading default row in the DIM_CUSTOMERS table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."SET_DEFAULT_ROW_TO_DIM_CUSTOMERS"()
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
	  FROM "BL_DM"."DIM_CUSTOMERS";

    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CREATE MATERIALIZED VIEW "BL_CL".temp_view AS
	SELECT *
	  FROM "BL_DM"."DIM_CUSTOMERS";
	 
	v_start_time := clock_timestamp();
    BEGIN
		INSERT INTO "BL_DM"."DIM_CUSTOMERS"(
		    "CUSTOMER_SURR_ID",
		    "CUSTOMER_SRC_ID",
		    "BUS_CUSTOMER_ID",
		    "CUSTOMER_NAME",
		    "CUSTOMER_SURNAME",
		    "PHONE_NUM",
		    "COUNTRY_ID",
		    "COUNTRY_NAME",
		    "SUBREGION_ID",
		    "SUBREGION_NAME",	     
		    "REGION_ID",
		    "REGION_NAME",	
		    "SOURCE_SYSTEM",
		    "SOURCE_ENTITY",
		    "TA_INSERT_DT",
		    "TA_UPDATE_DT"
		)
		SELECT -1,
		   	   -1,
		       'n. a.',
		       'n. a.',
		       'n. a.',
		       -1,
		       -1,
		       'n. a.',
		       -1,
		       'n. a.',
		       -1,
		       'n. a.',
		       'MANUAL',
		       'MANUAL',
		       '1900-01-01 00:00:00',
		       '1900-01-01 00:00:00'
		WHERE NOT EXISTS (
		    SELECT 1
		      FROM "BL_DM"."DIM_CUSTOMERS"
		     WHERE "CUSTOMER_SURR_ID" = -1
		);
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "BL_DM"."DIM_CUSTOMERS";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;
    
    SELECT COUNT(*) INTO v_updated_rows
      FROM "BL_DM"."DIM_CUSTOMERS" AS i
           INNER JOIN "BL_CL".temp_view AS t
           ON  i."CUSTOMER_SURR_ID" = t."CUSTOMER_SURR_ID"
           AND i."TA_UPDATE_DT"    != t."TA_UPDATE_DT";

    RAISE NOTICE 'Numbers of inserted/updated rows to DIM_CUSTOMERS: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
                
    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'BL_CL"."SET_DEFAULT_ROW_TO_DIM_CUSTOMERS',
	    'BL_CL.DIM_CUSTOMERS',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;


--------- Procedure to loading default row in the DIM_CUSTOMERS table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."SET_DEFAULT_ROW_TO_DIM_PRODUCTS_SCD"()
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
		INSERT INTO "BL_DM"."DIM_PRODUCTS_SCD"(
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
		)
		SELECT -1,
		       -1,
		       'n. a.',
		       'n. a.',
		       -1,
		       'n. a.',
		       -1,
		       'n. a.',
		       -1,
		       'n. a.',
		       '1900-01-01 00:00:00',
		       '9999-12-31 23:59:59',
		       'Y',
		       'MANUAL',
		       'MANUAL',
		       '1900-01-01 00:00:00',
		       '1900-01-01 00:00:00'
		WHERE NOT EXISTS (
		    SELECT 1
		      FROM "BL_DM"."DIM_PRODUCTS_SCD"
		     WHERE "PRODUCT_SURR_ID" = -1
		);
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
    	'BL_CL"."SET_DEFAULT_ROW_TO_DIM_PRODUCTS_SCD',
	    'BL_CL.DIM_PRODUCTS_SCD',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;


CREATE OR REPLACE PROCEDURE "BL_CL"."SET_DEFAULT_ROWS_TO_BL_DM"()
LANGUAGE plpgsql
AS
$$
BEGIN

	CALL "BL_CL"."SET_DEFAULT_ROW_TO_DIM_PAYMENT_TYPES"();
	CALL "BL_CL"."SET_DEFAULT_ROW_TO_DIM_PLATFORMS"();
	CALL "BL_CL"."SET_DEFAULT_ROW_TO_DIM_CUSTOMERS"();
	CALL "BL_CL"."LOAD_DATA_TO_DIM_TIME_DAY"('1900-01-01', '1900-01-01');
	CALL "BL_CL"."SET_DEFAULT_ROW_TO_DIM_PRODUCTS_SCD"();
   
END;
$$;