-- DML for population source table:

--------- Procedure to loading data in the LOAD_DATA_TO_SRC_EUROPE_SALES_ARCH ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_TO_SRC_EUROPE_SALES_ARCH"()
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
	  FROM "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES_ARCH";
	 
	v_start_time := clock_timestamp();
    BEGIN
	    INSERT INTO "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES_ARCH"
		SELECT *, 
		       NOW() AT TIME ZONE 'UTC',
		       NOW() AT TIME ZONE 'UTC'
		  FROM "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES"
		;
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES_ARCH";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;


    RAISE NOTICE 'Numbers of inserted/updated rows to SRC_EUROPE_SALES_ARCH: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_TO_SRC_EUROPE_SALES_ARCH',
	    'SA_EUROPE_SERVER_SALES.SRC_EUROPE_SALES_ARCH',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;
