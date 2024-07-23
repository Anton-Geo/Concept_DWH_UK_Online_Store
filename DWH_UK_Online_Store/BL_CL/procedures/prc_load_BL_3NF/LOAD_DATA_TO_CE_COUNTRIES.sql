-----------------------------------------------------
/*
TRUNCATE TABLE "BL_3NF"."CE_COUNTRIES" CASCADE;
ALTER SEQUENCE "BL_3NF".ce_countries_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the CE_COUNTRIES table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_TO_CE_COUNTRIES"()
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
	 
	v_start_time := clock_timestamp();
    BEGIN
		MERGE INTO "BL_3NF"."CE_COUNTRIES" AS target
		USING (
		    SELECT DISTINCT
			       wrk."COUNTRY_SURR_ID"   AS "COUNTRY_SRC_ID",
			       wrk."COUNTRY_NAME"      AS "COUNTRY_NAME",
			       COALESCE(ce_p."SUBREGION_ID", -1) AS "SUBREGION_ID",
			       'BL_CL'                 AS "SOURCE_SYSTEM",
			       'WRK_COUNTRIES'         AS "SOURCE_ENTITY"
			  FROM "BL_CL"."WRK_COUNTRIES" AS wrk
			       LEFT JOIN "SA_GLOBAL_SERVER_SALES"."SRC_GLOBAL_SALES" AS src_g
			       ON wrk."COUNTRY_SRC_ID" = src_g.country
			       LEFT JOIN "BL_CL"."WRK_SUBREGIONS" AS wrk_p
			       ON src_g.countrysubregion = wrk_p."SUBREGION_SRC_ID"
			       LEFT JOIN "BL_3NF"."CE_SUBREGIONS" AS ce_p
			       ON wrk_p."SUBREGION_SURR_ID" = ce_p."SUBREGION_SRC_ID"
			 ORDER BY "COUNTRY_SRC_ID"
		) AS src
		ON target."COUNTRY_SRC_ID" = src."COUNTRY_SRC_ID"
		WHEN NOT MATCHED THEN
		    INSERT (
		    	"COUNTRY_ID",
			    "COUNTRY_SRC_ID",
			    "COUNTRY_NAME",
			    "SUBREGION_ID",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_3NF".ce_countries_seq'),
		        src."COUNTRY_SRC_ID",
		        src."COUNTRY_NAME",
		        src."SUBREGION_ID",
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
	  FROM "BL_3NF"."CE_COUNTRIES";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;

    RAISE NOTICE 'Numbers of inserted/updated rows to CE_COUNTRIES: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_TO_CE_COUNTRIES',
	    'BL_3NF.CE_COUNTRIES',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;