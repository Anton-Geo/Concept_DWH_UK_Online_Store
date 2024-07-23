-----------------------------------------------------
/*
TRUNCATE TABLE "BL_CL"."WRK_PLATFORMS";
ALTER SEQUENCE "BL_CL".wrk_platforms_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the WRK_PLATFORMS table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_FROM_EU_TO_WRK_PLATFORMS"()
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
	  FROM "BL_CL"."WRK_PLATFORMS";
	 
	v_start_time := clock_timestamp();
    BEGIN
		MERGE INTO "BL_CL"."WRK_PLATFORMS" AS target
		USING (
		    SELECT DISTINCT
		           FIRST_VALUE(salesplatform)         OVER w AS platform,
		           FIRST_VALUE(CASE WHEN salesplatform ~ '^[a-zA-Z ]*$' THEN salesplatform 
		                            ELSE 'n. a.' END) OVER w AS platform_name,
		           'SA_EUROPE_SERVER_SALES'                  AS source_system,
		           'SRC_EUROPE_SALES'                        AS source_entity
		      FROM "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES"
		    WINDOW w AS (PARTITION BY salesplatform ORDER BY salesplatform)
		) AS src
		ON target."PLATFORM_SRC_ID" = src.platform
		WHEN NOT MATCHED THEN
		    INSERT (
			    "PLATFORM_SURR_ID",
			    "PLATFORM_SRC_ID",
			    "PLATFORM_NAME",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_CL".wrk_platforms_seq'),
		        src.platform,
		        src.platform_name,
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
	  FROM "BL_CL"."WRK_PLATFORMS";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;

    RAISE NOTICE 'Numbers of inserted/updated rows to WRK_PLATFORMS: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_FROM_EU_TO_WRK_PLATFORMS',
	    'BL_CL.WRK_PLATFORMS',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;