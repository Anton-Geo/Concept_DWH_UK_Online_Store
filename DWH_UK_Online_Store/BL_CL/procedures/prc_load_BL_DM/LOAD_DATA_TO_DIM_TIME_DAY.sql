-----------------------------------------------------
/*
TRUNCATE TABLE "BL_DM"."DIM_TIME_DAY" CASCADE;
 */
--------- Procedure to loading data in the "BL_DM"."DIM_TIME_DAY" table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_TO_DIM_TIME_DAY"(
    IN start_date DATE DEFAULT '2022-01-01', 
    IN end_date   DATE DEFAULT CURRENT_DATE
)
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
	  FROM "BL_DM"."DIM_TIME_DAY";

    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CREATE MATERIALIZED VIEW "BL_CL".temp_view AS
	SELECT *
	  FROM "BL_DM"."DIM_TIME_DAY";
	 
	v_start_time := clock_timestamp();
    BEGIN
	    INSERT INTO "BL_DM"."DIM_TIME_DAY"(
	        "DATE_KEY", "DAY_OF_WEEK_NUM", "DAY_OF_WEEK_DESC", "WEEKEND_FLAG", "ISO_WEEK_NUM", 
	        "DAY_OF_MONTH_NUM", "MONTH_VALUE", "MONTH_DESC", 
	        "CALENDAR_QUARTER_VALUE", "CALENDAR_QUARTER_DESC", 
	        "CALENDAR_YEAR_VALUE", "FISCAL_QUARTER_VALUE", "FISCAL_QUARTER_DESC"
	    )
	    SELECT DATE_SERIES::DATE                AS DATE_KEY,
	           EXTRACT(ISODOW FROM DATE_SERIES) AS DAY_OF_WEEK_NUM,
	           TO_CHAR(DATE_SERIES, 'Day')      AS DAY_OF_WEEK_DESC,
	           CASE 
	               WHEN EXTRACT(ISODOW FROM DATE_SERIES) IN (6, 7) THEN '1' 
	               ELSE '0' 
	           END AS WEEKEND_FLAG,
	           EXTRACT(WEEK FROM DATE_SERIES)   AS ISO_WEEK_NUM,
	           EXTRACT(DAY FROM DATE_SERIES)    AS DAY_OF_MONTH_NUM,
	           TO_CHAR(DATE_SERIES, 'MM')       AS MONTH_VALUE,
	           TO_CHAR(DATE_SERIES, 'Month')    AS MONTH_DESC,
	           TO_CHAR(DATE_SERIES, 'Q')        AS CALENDAR_QUARTER_VALUE,
	           'Q' || TO_CHAR(DATE_SERIES, 'Q') AS CALENDAR_QUARTER_DESC,
	           TO_CHAR(DATE_SERIES, 'YYYY')     AS CALENDAR_YEAR_VALUE,
	           CASE 
	               WHEN EXTRACT(MONTH FROM DATE_SERIES) BETWEEN 1  AND 3  THEN '1'
	               WHEN EXTRACT(MONTH FROM DATE_SERIES) BETWEEN 4  AND 6  THEN '2'
	               WHEN EXTRACT(MONTH FROM DATE_SERIES) BETWEEN 7  AND 9  THEN '3'
	               WHEN EXTRACT(MONTH FROM DATE_SERIES) BETWEEN 10 AND 12 THEN '4'
	           END AS FISCAL_QUARTER_VALUE,
	           'Q' || CASE 
	               WHEN EXTRACT(MONTH FROM DATE_SERIES) BETWEEN 1  AND 3  THEN '1'
	               WHEN EXTRACT(MONTH FROM DATE_SERIES) BETWEEN 4  AND 6  THEN '2'
	               WHEN EXTRACT(MONTH FROM DATE_SERIES) BETWEEN 7  AND 9  THEN '3'
	               WHEN EXTRACT(MONTH FROM DATE_SERIES) BETWEEN 10 AND 12 THEN '4'
	           END AS FISCAL_QUARTER_DESC
	      FROM GENERATE_SERIES(start_date, end_date, '1 day'::INTERVAL) AS DATE_SERIES
	     WHERE NOT EXISTS (SELECT 1 
	                         FROM "BL_DM"."DIM_TIME_DAY" 
	                        WHERE "DATE_KEY" = DATE_SERIES)
	    ;
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "BL_DM"."DIM_TIME_DAY";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;
    
    SELECT COUNT(*) INTO v_updated_rows
      FROM "BL_DM"."DIM_TIME_DAY" AS i
           INNER JOIN "BL_CL".temp_view AS t
           ON  i."DATE_KEY"      = t."DATE_KEY"
           AND i."TA_UPDATE_DT" != t."TA_UPDATE_DT";

    RAISE NOTICE 'Numbers of inserted/updated rows to DIM_TIME_DAY: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
                
    DROP MATERIALIZED VIEW IF EXISTS "BL_CL".temp_view;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_TO_DIM_TIME_DAY',
	    'BL_DM.DIM_TIME_DAY',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;