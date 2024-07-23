--------- Procedure to loading default row in the CE_REGIONS table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."SET_DEFAULT_ROW_TO_CE_REGIONS"()
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
	  FROM "BL_3NF"."CE_REGIONS";

    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CREATE MATERIALIZED VIEW "BL_CL".temp_view AS
	SELECT *
	  FROM "BL_3NF"."CE_REGIONS";

	v_start_time := clock_timestamp();
    BEGIN
		INSERT INTO "BL_3NF"."CE_REGIONS"(
		    "REGION_ID",
		    "REGION_SRC_ID",
		    "REGION_NAME",
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
		      FROM "BL_3NF"."CE_REGIONS"
		     WHERE "REGION_ID" = -1
		);
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "BL_3NF"."CE_REGIONS";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;

    SELECT COUNT(*) INTO v_updated_rows
      FROM "BL_3NF"."CE_REGIONS" AS i
           INNER JOIN "BL_CL".temp_view AS t
           ON  i."REGION_ID"     = t."REGION_ID"
           AND i."TA_UPDATE_DT" != t."TA_UPDATE_DT";

    RAISE NOTICE 'Numbers of inserted/updated rows to CE_REGIONS: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
                
    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'SET_DEFAULT_ROWS_TO_CE_REGIONS',
	    'BL_3NF.CE_REGIONS',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;


--------- Procedure to loading default row in the CE_SUBREGIONS table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."SET_DEFAULT_ROW_TO_CE_SUBREGIONS"()
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
	  FROM "BL_3NF"."CE_SUBREGIONS";

    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CREATE MATERIALIZED VIEW "BL_CL".temp_view AS
	SELECT *
	  FROM "BL_3NF"."CE_SUBREGIONS";
	 
	v_start_time := clock_timestamp();
    BEGIN
		INSERT INTO "BL_3NF"."CE_SUBREGIONS"(
		    "SUBREGION_ID",
		    "SUBREGION_SRC_ID",
		    "SUBREGION_NAME",
		    "REGION_ID",
		    "SOURCE_SYSTEM",
		    "SOURCE_ENTITY",
		    "TA_INSERT_DT",
		    "TA_UPDATE_DT"
		)
		SELECT -1,
		       -1,
		       'n. a.',
		       -1,
		       'MANUAL',
		       'MANUAL',
		       '1900-01-01 00:00:00',
		       '1900-01-01 00:00:00'
		WHERE NOT EXISTS (
		    SELECT 1
		      FROM "BL_3NF"."CE_SUBREGIONS"
		     WHERE "SUBREGION_ID" = -1
		);
	       
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "BL_3NF"."CE_SUBREGIONS";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;
    
    SELECT COUNT(*) INTO v_updated_rows
      FROM "BL_3NF"."CE_SUBREGIONS" AS i
           INNER JOIN "BL_CL".temp_view AS t
           ON  i."SUBREGION_ID"  = t."SUBREGION_ID"
           AND i."TA_UPDATE_DT" != t."TA_UPDATE_DT";

    RAISE NOTICE 'Numbers of inserted/updated rows to CE_SUBREGIONS: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
                
    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'SET_DEFAULT_ROWS_TO_CE_SUBREGIONS',
	    'BL_3NF.CE_SUBREGIONS',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;


--------- Procedure to loading default row in the CE_COUNTRIES table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."SET_DEFAULT_ROW_TO_CE_COUNTRIES"()
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
	  FROM "BL_3NF"."CE_COUNTRIES";

    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CREATE MATERIALIZED VIEW "BL_CL".temp_view AS
	SELECT *
	  FROM "BL_3NF"."CE_COUNTRIES";
	 
	v_start_time := clock_timestamp();
    BEGIN
		INSERT INTO "BL_3NF"."CE_COUNTRIES"(
		    "COUNTRY_ID",
		    "COUNTRY_SRC_ID",
		    "COUNTRY_NAME",
		    "SUBREGION_ID",
		    "SOURCE_SYSTEM",
		    "SOURCE_ENTITY",
		    "TA_INSERT_DT",
		    "TA_UPDATE_DT"
		)
		SELECT -1,
		       -1,
		       'n. a.',
		       -1,
		       'MANUAL',
		       'MANUAL',
		       '1900-01-01 00:00:00',
		       '1900-01-01 00:00:00'
		WHERE NOT EXISTS (
		    SELECT 1
		      FROM "BL_3NF"."CE_COUNTRIES"
		     WHERE "COUNTRY_ID" = -1
		);
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "BL_3NF"."CE_COUNTRIES";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;
    
    SELECT COUNT(*) INTO v_updated_rows
      FROM "BL_3NF"."CE_COUNTRIES" AS i
           INNER JOIN "BL_CL".temp_view AS t
           ON  i."COUNTRY_ID"    = t."COUNTRY_ID"
           AND i."TA_UPDATE_DT" != t."TA_UPDATE_DT";

    RAISE NOTICE 'Numbers of inserted/updated rows to CE_COUNTRIES: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
                
    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'SET_DEFAULT_ROWS_TO_CE_COUNTRIES',
	    'BL_3NF.CE_COUNTRIES',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;


--------- Procedure to loading default row in the CE_CUSTOMERS table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."SET_DEFAULT_ROW_TO_CE_CUSTOMERS"()
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
	  FROM "BL_3NF"."CE_CUSTOMERS";

    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CREATE MATERIALIZED VIEW "BL_CL".temp_view AS
	SELECT *
	  FROM "BL_3NF"."CE_CUSTOMERS";
	 
	v_start_time := clock_timestamp();
    BEGIN
		INSERT INTO "BL_3NF"."CE_CUSTOMERS"(
		    "CUSTOMER_ID",
		    "CUSTOMER_SRC_ID",
		    "BUS_CUSTOMER_ID",
		    "CUSTOMER_NAME",
		    "CUSTOMER_SURNAME",
		    "PHONE_NUM",
		    "COUNTRY_ID",
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
		       'MANUAL',
		       'MANUAL',
		       '1900-01-01 00:00:00',
		       '1900-01-01 00:00:00'
		WHERE NOT EXISTS (
		    SELECT 1
		      FROM "BL_3NF"."CE_CUSTOMERS"
		     WHERE "CUSTOMER_ID" = -1
		);
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "BL_3NF"."CE_CUSTOMERS";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;
    
    SELECT COUNT(*) INTO v_updated_rows
      FROM "BL_3NF"."CE_CUSTOMERS" AS i
           INNER JOIN "BL_CL".temp_view AS t
           ON  i."CUSTOMER_ID"   = t."CUSTOMER_ID"
           AND i."TA_UPDATE_DT" != t."TA_UPDATE_DT";

    RAISE NOTICE 'Numbers of inserted/updated rows to CE_CUSTOMERS: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
                
    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'SET_DEFAULT_ROWS_TO_CE_CUSTOMERS',
	    'BL_3NF.CE_CUSTOMERS',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;


--------- Procedure to loading default row in the CE_PLATFORMS table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."SET_DEFAULT_ROW_TO_CE_PLATFORMS"()
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
	  FROM "BL_3NF"."CE_PLATFORMS";

    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CREATE MATERIALIZED VIEW "BL_CL".temp_view AS
	SELECT *
	  FROM "BL_3NF"."CE_PLATFORMS";
	 
	v_start_time := clock_timestamp();
    BEGIN
		INSERT INTO "BL_3NF"."CE_PLATFORMS"(
		    "PLATFORM_ID",
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
		      FROM "BL_3NF"."CE_PLATFORMS"
		     WHERE "PLATFORM_ID" = -1
		);
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "BL_3NF"."CE_PLATFORMS";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;
    
    SELECT COUNT(*) INTO v_updated_rows
      FROM "BL_3NF"."CE_PLATFORMS" AS i
           INNER JOIN "BL_CL".temp_view AS t
           ON  i."PLATFORM_ID"   = t."PLATFORM_ID"
           AND i."TA_UPDATE_DT" != t."TA_UPDATE_DT";

    RAISE NOTICE 'Numbers of inserted/updated rows to CE_PLATFORMS: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
                
    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'SET_DEFAULT_ROWS_TO_CE_PLATFORMS',
	    'BL_3NF.CE_PLATFORMS',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;


--------- Procedure to loading default row in the CE_PAYMENT_TYPES table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."SET_DEFAULT_ROW_TO_CE_PAYMENT_TYPES"()
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
	  FROM "BL_3NF"."CE_PAYMENT_TYPES";

    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CREATE MATERIALIZED VIEW "BL_CL".temp_view AS
	SELECT *
	  FROM "BL_3NF"."CE_PAYMENT_TYPES";
	 
	v_start_time := clock_timestamp();
    BEGIN
		INSERT INTO "BL_3NF"."CE_PAYMENT_TYPES"(
		    "PAYMENT_TYPE_ID",
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
		      FROM "BL_3NF"."CE_PAYMENT_TYPES"
		     WHERE "PAYMENT_TYPE_ID" = -1
		);
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "BL_3NF"."CE_PAYMENT_TYPES";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;
    
    SELECT COUNT(*) INTO v_updated_rows
      FROM "BL_3NF"."CE_PAYMENT_TYPES" AS i
           INNER JOIN "BL_CL".temp_view AS t
           ON  i."PAYMENT_TYPE_ID" = t."PAYMENT_TYPE_ID"
           AND i."TA_UPDATE_DT"   != t."TA_UPDATE_DT";

    RAISE NOTICE 'Numbers of inserted/updated rows to CE_PAYMENT_TYPES: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
                
    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'SET_DEFAULT_ROWS_TO_CE_PAYMENT_TYPES',
	    'BL_3NF.CE_PAYMENT_TYPES',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;



--------- Procedure to loading default row in the CE_PRODUCT_CATEGORIES table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."SET_DEFAULT_ROW_TO_CE_PRODUCT_CATEGORIES"()
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
	  FROM "BL_3NF"."CE_PRODUCT_CATEGORIES";

    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CREATE MATERIALIZED VIEW "BL_CL".temp_view AS
	SELECT *
	  FROM "BL_3NF"."CE_PRODUCT_CATEGORIES";
	 
	v_start_time := clock_timestamp();
    BEGIN
		INSERT INTO "BL_3NF"."CE_PRODUCT_CATEGORIES"(
		    "PRODUCT_CATEGORY_ID",
		    "PRODUCT_CATEGORY_SRC_ID",
		    "PRODUCT_CATEGORY_NAME",
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
		      FROM "BL_3NF"."CE_PRODUCT_CATEGORIES"
		     WHERE "PRODUCT_CATEGORY_ID" = -1
		);
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "BL_3NF"."CE_PRODUCT_CATEGORIES";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;
    
    SELECT COUNT(*) INTO v_updated_rows
      FROM "BL_3NF"."CE_PRODUCT_CATEGORIES" AS i
           INNER JOIN "BL_CL".temp_view AS t
           ON  i."PRODUCT_CATEGORY_ID" = t."PRODUCT_CATEGORY_ID"
           AND i."TA_UPDATE_DT"       != t."TA_UPDATE_DT";

    RAISE NOTICE 'Numbers of inserted/updated rows to CE_PRODUCT_CATEGORIES: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
                
    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'SET_DEFAULT_ROWS_TO_CE_PRODUCT_CATEGORIES',
	    'BL_3NF.CE_PRODUCT_CATEGORIES',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;


--------- Procedure to loading default row in the CE_PRODUCT_SUBCATEGORIES table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."SET_DEFAULT_ROW_TO_CE_PRODUCT_SUBCATEGORIES"()
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

    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CREATE MATERIALIZED VIEW "BL_CL".temp_view AS
	SELECT *
	  FROM "BL_3NF"."CE_PRODUCT_SUBCATEGORIES";
	 
	v_start_time := clock_timestamp();
    BEGIN
		INSERT INTO "BL_3NF"."CE_PRODUCT_SUBCATEGORIES"(
		    "PRODUCT_SUBCATEGORY_ID",
		    "PRODUCT_SUBCATEGORY_SRC_ID",
		    "PRODUCT_SUBCATEGORY_NAME",
		    "PRODUCT_CATEGORY_ID",
		    "SOURCE_SYSTEM",
		    "SOURCE_ENTITY",
		    "TA_INSERT_DT",
		    "TA_UPDATE_DT"
		)
		SELECT -1,
		       -1,
		       'n. a.',
		       -1,
		       'MANUAL',
		       'MANUAL',
		       '1900-01-01 00:00:00',
		       '1900-01-01 00:00:00'
		WHERE NOT EXISTS (
		    SELECT 1
		      FROM "BL_3NF"."CE_PRODUCT_SUBCATEGORIES"
		     WHERE "PRODUCT_SUBCATEGORY_ID" = -1
		);
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
    
    SELECT COUNT(*) INTO v_updated_rows
      FROM "BL_3NF"."CE_PRODUCT_SUBCATEGORIES" AS i
           INNER JOIN "BL_CL".temp_view AS t
           ON  i."PRODUCT_SUBCATEGORY_ID" = t."PRODUCT_SUBCATEGORY_ID"
           AND i."TA_UPDATE_DT"          != t."TA_UPDATE_DT";

    RAISE NOTICE 'Numbers of inserted/updated rows to CE_PRODUCT_SUBCATEGORIES: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
                
    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'SET_DEFAULT_ROWS_TO_CE_PRODUCT_SUBCATEGORIES',
	    'BL_3NF.CE_PRODUCT_SUBCATEGORIES',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;


--------- Procedure to loading default row in the CE_PRODUCTS_SCD table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."SET_DEFAULT_ROW_TO_CE_PRODUCTS_SCD"()
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
		INSERT INTO "BL_3NF"."CE_PRODUCTS_SCD"(
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
		)
		SELECT -1,
		       -1,
		       'n. a.',
		       'n. a.',
		       -1,
		       'n. a.',
		       -1,
		       '1900-01-01 00:00:00',
		       '9999-12-31 23:59:59',
		       'Y',
		       'MANUAL',
		       'MANUAL',
		       '1900-01-01 00:00:00',
		       '1900-01-01 00:00:00'
		WHERE NOT EXISTS (
		    SELECT 1
		      FROM "BL_3NF"."CE_PRODUCTS_SCD"
		     WHERE "PRODUCT_ID" = -1
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
    	'SET_DEFAULT_ROWS_TO_CE_PRODUCTS_SCD',
	    'BL_3NF.CE_PRODUCTS_SCD',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;

CREATE OR REPLACE PROCEDURE "BL_CL"."SET_DEFAULT_ROWS_TO_BL_3NF"()
LANGUAGE plpgsql
AS
$$
BEGIN

	CALL "BL_CL"."SET_DEFAULT_ROW_TO_CE_REGIONS"();
	CALL "BL_CL"."SET_DEFAULT_ROW_TO_CE_SUBREGIONS"();
	CALL "BL_CL"."SET_DEFAULT_ROW_TO_CE_COUNTRIES"();
	CALL "BL_CL"."SET_DEFAULT_ROW_TO_CE_CUSTOMERS"();
	CALL "BL_CL"."SET_DEFAULT_ROW_TO_CE_PLATFORMS"();
	CALL "BL_CL"."SET_DEFAULT_ROW_TO_CE_PAYMENT_TYPES"();
	CALL "BL_CL"."SET_DEFAULT_ROW_TO_CE_PRODUCT_CATEGORIES"();
	CALL "BL_CL"."SET_DEFAULT_ROW_TO_CE_PRODUCT_SUBCATEGORIES"();
	CALL "BL_CL"."SET_DEFAULT_ROW_TO_CE_PRODUCTS_SCD"();
   
END;
$$;