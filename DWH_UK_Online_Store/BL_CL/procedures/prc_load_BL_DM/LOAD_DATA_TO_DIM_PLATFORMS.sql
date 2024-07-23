-----------------------------------------------------
/*
TRUNCATE TABLE "BL_DM"."DIM_PLATFORMS" CASCADE;
ALTER SEQUENCE "BL_DM".dm_platforms_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the "BL_DM"."DIM_PLATFORMS" table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_TO_DIM_PLATFORMS"()
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
	 
	v_start_time := clock_timestamp();
    BEGIN
		MERGE INTO "BL_DM"."DIM_PLATFORMS" AS target
		USING (
			SELECT DISTINCT
			       ce."PLATFORM_ID"   AS "PLATFORM_SRC_ID",
			       ce."PLATFORM_NAME" AS "PLATFORM_NAME",
			       'BL_3NF'           AS "SOURCE_SYSTEM",
			       'CE_PLATFORMS'     AS "SOURCE_ENTITY"
			  FROM "BL_3NF"."CE_PLATFORMS" AS ce
			 WHERE ce."PLATFORM_ID" != -1
			 ORDER BY "PLATFORM_SRC_ID"
		) AS src
		ON target."PLATFORM_SRC_ID" = src."PLATFORM_SRC_ID"
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
		        nextval('"BL_DM".dm_platforms_seq'),
		        src."PLATFORM_SRC_ID",
		        src."PLATFORM_NAME",
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
	  FROM "BL_DM"."DIM_PLATFORMS";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;

    RAISE NOTICE 'Numbers of inserted/updated rows to DIM_PLATFORMS: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_TO_DIM_PLATFORMS',
	    'BL_DM.DIM_PLATFORMS',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;