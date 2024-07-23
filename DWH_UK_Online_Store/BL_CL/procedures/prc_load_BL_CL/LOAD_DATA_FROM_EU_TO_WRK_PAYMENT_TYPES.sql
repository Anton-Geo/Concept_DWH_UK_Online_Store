-----------------------------------------------------
/*
TRUNCATE TABLE "BL_CL"."WRK_PAYMENT_TYPES";
ALTER SEQUENCE "BL_CL".wrk_payment_types_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the WRK_PAYMENT_TYPES table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_FROM_EU_TO_WRK_PAYMENT_TYPES"()
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
	  FROM "BL_CL"."WRK_PAYMENT_TYPES";
	 
	v_start_time := clock_timestamp();
    BEGIN
		MERGE INTO "BL_CL"."WRK_PAYMENT_TYPES" AS target
		USING (
		    SELECT DISTINCT
		       	   FIRST_VALUE(paymentmethod)         OVER w AS payment_type,
		       	   FIRST_VALUE(CASE WHEN paymentmethod ~ '^[a-zA-Z ]*$' THEN paymentmethod 
		                            ELSE 'n. a.' END) OVER w AS payment_type_name,
		           'SA_EUROPE_SERVER_SALES'                  AS source_system,
		           'SRC_EUROPE_SALES'                        AS source_entity
		      FROM "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES"
		    WINDOW w AS (PARTITION BY StockCode, InsertDateProduct ORDER BY InsertDateProduct)
		) AS src
		ON target."PAYMENT_TYPE_SRC_ID" = src.payment_type
		WHEN NOT MATCHED THEN
		    INSERT (
			    "PAYMENT_TYPE_SURR_ID",
			    "PAYMENT_TYPE_SRC_ID",
			    "PAYMENT_TYPE_NAME",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_CL".wrk_payment_types_seq'),
		        src.payment_type,
		        src.payment_type_name,
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
	  FROM "BL_CL"."WRK_PAYMENT_TYPES";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;

    RAISE NOTICE 'Numbers of inserted/updated rows to WRK_PAYMENT_TYPES: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_FROM_EU_TO_WRK_PAYMENT_TYPES',
	    'BL_CL.WRK_PAYMENT_TYPES',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;