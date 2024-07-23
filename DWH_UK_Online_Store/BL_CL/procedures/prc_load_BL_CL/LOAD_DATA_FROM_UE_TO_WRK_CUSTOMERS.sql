-----------------------------------------------------
/*
TRUNCATE TABLE "BL_CL"."WRK_CUSTOMERS";
ALTER SEQUENCE "BL_CL".wrk_customer_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the WRK_CUSTOMERS table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_FROM_UE_TO_WRK_CUSTOMERS"()
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
	  FROM "BL_CL"."WRK_CUSTOMERS";

    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CREATE MATERIALIZED VIEW "BL_CL".temp_view AS
	SELECT *
	  FROM "BL_CL"."WRK_CUSTOMERS";
	 
	v_start_time := clock_timestamp();
    BEGIN
		MERGE INTO "BL_CL"."WRK_CUSTOMERS" AS target
		USING (
		    SELECT DISTINCT
		           LAST_VALUE(CustomerID) OVER w AS customerid,
		           LAST_VALUE(CASE WHEN CustomerName    ~ '^[a-zA-Z -]*$' THEN CustomerName    ELSE 'n. a.' END)
		                                   OVER w AS customer_name,
		           LAST_VALUE(CASE WHEN CustomerSurname ~ '^[a-zA-Z -]*$' THEN CustomerSurname ELSE 'n. a.' END)
		                                   OVER w AS customer_surname,
		           LAST_VALUE(CASE WHEN LENGTH(PhoneNumber) IN (10, 11) AND PhoneNumber     ~ '^[0-9]*$' THEN PhoneNumber ELSE '-1' END)
		                                   OVER w AS phone_num,
		           LAST_VALUE(CASE WHEN Country ~ '^[a-zA-Z -]*$' AND  Country != 'Unspecified'  THEN Country ELSE 'n. a.' END) 
		                                   OVER w AS country_name,
		           'SA_EUROPE_SERVER_SALES'       AS source_system,
		           'SRC_EUROPE_SALES'             AS source_entity
		      FROM "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES"
		    WINDOW w AS (PARTITION BY CustomerID ORDER BY CustomerID)      
		) AS src
		ON target."CUSTOMER_SRC_ID" = src.customerid
		WHEN NOT MATCHED THEN
		    INSERT (
			    "CUSTOMER_SURR_ID",
			    "CUSTOMER_SRC_ID",
			    "BUS_CUSTOMER_ID",
			    "CUSTOMER_NAME",
			    "CUSTOMER_SURNAME",
			    "PHONE_NUM",
			    "COUNTRY_NAME",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_CL".wrk_customer_seq'),
		        src.customerid,
		        src.customerid,
		        src.customer_name,
		        src.customer_surname,
		        src.phone_num::NUMERIC(11, 0),
		        src.country_name,
		        src.source_system,
		        src.source_entity,
		        NOW() AT TIME ZONE 'UTC',
		        NOW() AT TIME ZONE 'UTC'
			)
		WHEN MATCHED AND (
		       target."CUSTOMER_NAME"    <> src.customer_name
		    OR target."CUSTOMER_SURNAME" <> src.customer_surname
		    OR target."PHONE_NUM"        <> src.phone_num::NUMERIC(11, 0)
		    OR target."COUNTRY_NAME"     <> src.country_name
		) THEN
		    UPDATE SET
		        "CUSTOMER_NAME"    = src.customer_name,
		        "CUSTOMER_SURNAME" = src.customer_surname,
		        "PHONE_NUM"        = src.phone_num::NUMERIC(11, 0),
		        "COUNTRY_NAME"     = src.country_name,
			    "SOURCE_SYSTEM"    = src.source_system,
			    "SOURCE_ENTITY"    = src.source_entity,
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
	  FROM "BL_CL"."WRK_CUSTOMERS";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;
    
    SELECT COUNT(*) INTO v_updated_rows
      FROM "BL_CL"."WRK_CUSTOMERS" AS i
           INNER JOIN "BL_CL".temp_view AS t
           ON  i."CUSTOMER_SURR_ID" = t."CUSTOMER_SURR_ID"
           AND i."TA_UPDATE_DT"    != t."TA_UPDATE_DT";

    RAISE NOTICE 'Numbers of inserted/updated rows to WRK_CUSTOMERS: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
                
    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_FROM_UE_TO_WRK_CUSTOMERS',
	    'BL_CL.WRK_CUSTOMERS',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;