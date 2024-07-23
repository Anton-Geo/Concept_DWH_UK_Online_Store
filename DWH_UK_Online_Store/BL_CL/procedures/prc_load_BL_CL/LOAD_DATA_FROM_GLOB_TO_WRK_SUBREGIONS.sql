-----------------------------------------------------
/*
TRUNCATE TABLE "BL_CL"."WRK_SUBREGIONS";
ALTER SEQUENCE "BL_CL".wrk_subregion_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the WRK_SUBREGIONS table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_FROM_GLOB_TO_WRK_SUBREGIONS"()
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
	  FROM "BL_CL"."WRK_SUBREGIONS";
	 
	v_start_time := clock_timestamp();
    BEGIN
		MERGE INTO "BL_CL"."WRK_SUBREGIONS" AS target
		USING (
		    SELECT DISTINCT
		           FIRST_VALUE(CountrySubRegion)      OVER w AS country_subreg,
		           FIRST_VALUE(CASE WHEN CountrySubRegion ~ '^[a-zA-Z -]*$' AND  CountrySubRegion != 'Unspecified'  THEN CountrySubRegion 
		                            ELSE 'n. a.' END) OVER w AS subregion_name,
		           'SA_GLOBAL_SERVER_SALES'                  AS source_system,
		           'SRC_GLOBAL_SALES'                        AS source_entity
		      FROM "SA_GLOBAL_SERVER_SALES"."SRC_GLOBAL_SALES"
		    WINDOW w AS (PARTITION BY CountrySubRegion ORDER BY CountrySubRegion)
		) AS src
		ON target."SUBREGION_SRC_ID" = src.country_subreg
		WHEN NOT MATCHED THEN
		    INSERT (
		    	"SUBREGION_SURR_ID",
			    "SUBREGION_SRC_ID",
			    "SUBREGION_NAME",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_CL".wrk_subregion_seq'),
		        src.country_subreg,
		        src.subregion_name,
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
	  FROM "BL_CL"."WRK_SUBREGIONS";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;

    RAISE NOTICE 'Numbers of inserted/updated rows to WRK_SUBREGIONS: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_FROM_GLOB_TO_WRK_SUBREGIONS',
	    'BL_CL.WRK_SUBREGIONS',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;