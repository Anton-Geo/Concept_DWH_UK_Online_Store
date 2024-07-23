-----------------------------------------------------
/*
TRUNCATE TABLE "BL_3NF"."CE_SUBREGIONS" CASCADE;
ALTER SEQUENCE "BL_3NF".ce_subregions_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the CE_SUBREGIONS table ---------
CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_TO_CE_SUBREGIONS"()
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
	 
	v_start_time := clock_timestamp();
    BEGIN
	    MERGE INTO "BL_3NF"."CE_SUBREGIONS" AS target
		USING (
		    SELECT DISTINCT
			       wrk."SUBREGION_SURR_ID" AS "SUBREGION_SRC_ID",
			       wrk."SUBREGION_NAME"    AS "SUBREGION_NAME",
			       ce_p."REGION_ID"        AS "REGION_ID",
			       'BL_CL'                 AS "SOURCE_SYSTEM",
			       'WRK_SUBREGIONS'        AS "SOURCE_ENTITY"
		      FROM "BL_CL"."WRK_SUBREGIONS" AS wrk
		           LEFT JOIN "SA_GLOBAL_SERVER_SALES"."SRC_GLOBAL_SALES" AS src_g
			       ON wrk."SUBREGION_SRC_ID" = src_g.countrysubregion
			       LEFT JOIN "BL_CL"."WRK_REGIONS" AS wrk_p
			       ON src_g.countryregion = wrk_p."REGION_SRC_ID"
			       LEFT JOIN "BL_3NF"."CE_REGIONS" AS ce_p
			       ON wrk_p."REGION_SURR_ID" = ce_p."REGION_SRC_ID"
			 ORDER BY "SUBREGION_SRC_ID"
		) AS src
		ON target."SUBREGION_SRC_ID" = src."SUBREGION_SRC_ID"
		WHEN NOT MATCHED THEN
		    INSERT (
		    	"SUBREGION_ID",
			    "SUBREGION_SRC_ID",
			    "SUBREGION_NAME",
			    "REGION_ID",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_3NF".ce_subregions_seq'),
		        src."SUBREGION_SRC_ID",
		        src."SUBREGION_NAME",
		        src."REGION_ID",
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
	  FROM "BL_3NF"."CE_SUBREGIONS";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;

    RAISE NOTICE 'Numbers of inserted/updated rows to CE_SUBREGIONS: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_TO_CE_SUBREGIONS',
	    'BL_3NF.CE_SUBREGIONS',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;