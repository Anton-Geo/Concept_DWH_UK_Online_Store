-----------------------------------------------------
/*
TRUNCATE TABLE "BL_3NF"."CE_CUSTOMERS" CASCADE;
ALTER SEQUENCE "BL_3NF".ce_customers_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the CE_CUSTOMERS table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_TO_CE_CUSTOMERS"()
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
		MERGE INTO "BL_3NF"."CE_CUSTOMERS" AS target
		USING (
			SELECT DISTINCT
			       wrk."CUSTOMER_SURR_ID"        AS "CUSTOMER_SRC_ID",
			       wrk."BUS_CUSTOMER_ID"         AS "BUS_CUSTOMER_ID",
			       wrk."CUSTOMER_NAME"           AS "CUSTOMER_NAME",
			       wrk."CUSTOMER_SURNAME"        AS "CUSTOMER_SURNAME",
			       wrk."PHONE_NUM"               AS "PHONE_NUM",
			       COALESCE(ce."COUNTRY_ID", -1) AS "COUNTRY_ID",
			       'BL_CL'                       AS "SOURCE_SYSTEM",
			       'WRK_CUSTOMERS'               AS "SOURCE_ENTITY"
			  FROM "BL_CL"."WRK_CUSTOMERS" AS wrk
			       LEFT JOIN "BL_3NF"."CE_COUNTRIES" AS ce
			       ON wrk."COUNTRY_NAME" = ce."COUNTRY_NAME"
			 ORDER BY "CUSTOMER_SRC_ID"
		) AS src
		ON target."BUS_CUSTOMER_ID" = src."BUS_CUSTOMER_ID"
		WHEN NOT MATCHED THEN
		    INSERT (
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
		    ) VALUES (
		        nextval('"BL_3NF".ce_customers_seq'),
		        src."CUSTOMER_SRC_ID",
		        src."BUS_CUSTOMER_ID",
		        src."CUSTOMER_NAME",
		        src."CUSTOMER_SURNAME",
		        src."PHONE_NUM",
		        src."COUNTRY_ID",
		        src."SOURCE_SYSTEM",
		        src."SOURCE_ENTITY",
		        NOW() AT TIME ZONE 'UTC',
		        NOW() AT TIME ZONE 'UTC'
			)
		WHEN MATCHED AND (
		       target."CUSTOMER_NAME"    <> src."CUSTOMER_NAME"
		    OR target."CUSTOMER_SURNAME" <> src."CUSTOMER_SURNAME"
		    OR target."PHONE_NUM"        <> src."PHONE_NUM"
		    OR target."COUNTRY_ID"       <> src."COUNTRY_ID"
		) THEN
		    UPDATE SET
		        "CUSTOMER_NAME"    = src."CUSTOMER_NAME",
		        "CUSTOMER_SURNAME" = src."CUSTOMER_SURNAME",
		        "PHONE_NUM"        = src."PHONE_NUM",
		        "COUNTRY_ID"       = src."COUNTRY_ID",
		        "TA_UPDATE_DT"     = NOW() AT TIME ZONE 'UTC'
		;
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
    	'LOAD_DATA_TO_CE_CUSTOMERS',
	    'BL_3NF.CE_CUSTOMERS',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;
