-----------------------------------------------------
/*
TRUNCATE TABLE "BL_DM"."DIM_CUSTOMERS" CASCADE;
ALTER SEQUENCE "BL_DM".dm_customers_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the "BL_DM"."DIM_CUSTOMERS" table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_TO_DIM_CUSTOMERS"()
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
		MERGE INTO "BL_DM"."DIM_CUSTOMERS" AS target
		USING (
			SELECT DISTINCT
			       ce."CUSTOMER_ID"        AS "CUSTOMER_SRC_ID",
			       ce."BUS_CUSTOMER_ID"    AS "BUS_CUSTOMER_ID",
			       ce."CUSTOMER_NAME"      AS "CUSTOMER_NAME",
			       ce."CUSTOMER_SURNAME"   AS "CUSTOMER_SURNAME",
			       ce."PHONE_NUM"          AS "PHONE_NUM",
			       ce."COUNTRY_ID"         AS "COUNTRY_ID",
			       ctr."COUNTRY_NAME"      AS "COUNTRY_NAME",
			       ctr."SUBREGION_ID"      AS "SUBREGION_ID",
			       subreg."SUBREGION_NAME" AS "SUBREGION_NAME",	     
			       subreg."REGION_ID"      AS "REGION_ID",
			       reg."REGION_NAME"       AS "REGION_NAME",	
			       'BL_3NF'                AS "SOURCE_SYSTEM",
			       'CE_CUSTOMERS'          AS "SOURCE_ENTITY"
			  FROM "BL_3NF"."CE_CUSTOMERS"            AS ce
			       LEFT JOIN "BL_3NF"."CE_COUNTRIES"  AS ctr
			       ON ce."COUNTRY_ID"    = ctr."COUNTRY_ID"
			       LEFT JOIN "BL_3NF"."CE_SUBREGIONS" AS subreg
			       ON ctr."SUBREGION_ID" = subreg."SUBREGION_ID"
			       LEFT JOIN "BL_3NF"."CE_REGIONS"    AS reg
			       ON subreg."REGION_ID" = reg."REGION_ID"
			 WHERE ce."CUSTOMER_ID" != -1
			 ORDER BY "CUSTOMER_SRC_ID"
		) AS src
		ON target."CUSTOMER_SRC_ID" = src."CUSTOMER_SRC_ID"
		WHEN NOT MATCHED THEN
		    INSERT (
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
		    ) VALUES (
		        nextval('"BL_DM".dm_customers_seq'),
		        src."CUSTOMER_SRC_ID",
		        src."BUS_CUSTOMER_ID",
		        src."CUSTOMER_NAME",
		        src."CUSTOMER_SURNAME",
		        src."PHONE_NUM",
		        src."COUNTRY_ID",
		        src."COUNTRY_NAME",
		        src."SUBREGION_ID",
		        src."SUBREGION_NAME",
		        src."REGION_ID",
		        src."REGION_NAME",
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
		        "COUNTRY_NAME"     = src."COUNTRY_NAME",
		        "SUBREGION_ID"     = src."SUBREGION_ID",
		        "SUBREGION_NAME"   = src."SUBREGION_NAME",
		        "REGION_ID"        = src."REGION_ID",
		        "REGION_NAME"      = src."REGION_NAME",
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
    	'LOAD_DATA_TO_DIM_CUSTOMERS',
	    'BL_DM.DIM_CUSTOMERS',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;
