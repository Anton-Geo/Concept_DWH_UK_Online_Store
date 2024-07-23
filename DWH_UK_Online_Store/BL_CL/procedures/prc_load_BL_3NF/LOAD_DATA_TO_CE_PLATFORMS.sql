-----------------------------------------------------
/*
TRUNCATE TABLE "BL_3NF"."CE_PLATFORMS" CASCADE;
ALTER SEQUENCE "BL_3NF".ce_platforms_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the CE_PLATFORMS table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_TO_CE_PLATFORMS"()
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
	 
	v_start_time := clock_timestamp();
    BEGIN
		MERGE INTO "BL_3NF"."CE_PLATFORMS" AS target
		USING (
			SELECT DISTINCT
			       wrk."PLATFORM_SURR_ID"  AS "PLATFORM_SRC_ID",
			       wrk."PLATFORM_NAME"     AS "PLATFORM_NAME",
			       'BL_CL'                 AS "SOURCE_SYSTEM",
			       'WRK_PLATFORMS'         AS "SOURCE_ENTITY"
			  FROM "BL_CL"."WRK_PLATFORMS" AS wrk
			 ORDER BY "PLATFORM_SRC_ID"
		) AS src
		ON target."PLATFORM_SRC_ID" = src."PLATFORM_SRC_ID"
		WHEN NOT MATCHED THEN
		    INSERT (
		    	"PLATFORM_ID",
			    "PLATFORM_SRC_ID",
			    "PLATFORM_NAME",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_3NF".ce_platforms_seq'),
		        src."PLATFORM_SRC_ID",
		        src."PLATFORM_NAME",
		        src."SOURCE_SYSTEM",
		        src."SOURCE_ENTITY",
		        NOW() AT TIME ZONE 'UTC',
		        NOW() AT TIME ZONE 'UTC'
			)
	    WHEN MATCHED THEN
	        DO NOTHING;
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

    RAISE NOTICE 'Numbers of inserted/updated rows to CE_PLATFORMS: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_TO_CE_PLATFORMS',
	    'BL_3NF.CE_PLATFORMS',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;
