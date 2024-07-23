-----------------------------------------------------
/*
TRUNCATE TABLE "BL_CL"."WRK_COUNTRIES";
ALTER SEQUENCE "BL_CL".wrk_countries_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the WRK_COUNTRIES table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_FROM_GLOB_TO_WRK_COUNTRIES"()
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
	  FROM "BL_CL"."WRK_COUNTRIES";
	 
	v_start_time := clock_timestamp();
    BEGIN
		MERGE INTO "BL_CL"."WRK_COUNTRIES" AS target
		USING (
		    SELECT DISTINCT
		           FIRST_VALUE(Country)               OVER w AS country,
		           FIRST_VALUE(CASE WHEN Country ~ '^[a-zA-Z -]*$' AND  Country != 'Unspecified'  THEN Country 
		                            ELSE 'n. a.' END) OVER w AS country_name,
		           'SA_GLOBAL_SERVER_SALES'                  AS source_system,
		           'SRC_GLOBAL_SALES'                        AS source_entity
		      FROM "SA_GLOBAL_SERVER_SALES"."SRC_GLOBAL_SALES"
		     WHERE Country != 'Unspecified'
		    WINDOW w AS (PARTITION BY Country ORDER BY Country)
		) AS src
		ON target."COUNTRY_SRC_ID" = src.country
		WHEN NOT MATCHED THEN
		    INSERT (
		    	"COUNTRY_SURR_ID",
			    "COUNTRY_SRC_ID",
			    "COUNTRY_NAME",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_CL".wrk_countries_seq'),
		        src.country,
		        src.country_name,
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
	  FROM "BL_CL"."WRK_COUNTRIES";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;

    RAISE NOTICE 'Numbers of inserted/updated rows to WRK_COUNTRIES: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_FROM_GLOB_TO_WRK_COUNTRIES',
	    'BL_CL.WRK_COUNTRIES',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;