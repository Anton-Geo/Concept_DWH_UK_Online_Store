-- DML for population source table:

--------- Procedure to loading data in the SRC_GLOBAL_SALES ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_TO_SRC_GLOBAL_SALES"()
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
	  FROM "SA_GLOBAL_SERVER_SALES"."SRC_GLOBAL_SALES";
	 
	v_start_time := clock_timestamp();
    BEGIN
	    INSERT INTO "SA_GLOBAL_SERVER_SALES"."SRC_GLOBAL_SALES"
		SELECT * 
		  FROM "SA_GLOBAL_SERVER_SALES"."EXT_GLOBAL_SALES"
		;
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "SA_GLOBAL_SERVER_SALES"."SRC_GLOBAL_SALES";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;

    RAISE NOTICE 'Numbers of inserted/updated rows to SRC_GLOBAL_SALES: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_TO_SRC_GLOBAL_SALES',
	    'SA_GLOBAL_SERVER_SALES.SRC_GLOBAL_SALES',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;


/*
	    MERGE INTO "SA_GLOBAL_SERVER_SALES"."SRC_GLOBAL_SALES" AS target
			USING (SELECT *
			         FROM "SA_GLOBAL_SERVER_SALES"."EXT_GLOBAL_SALES"
			) AS src
			ON target.invoiceno = src.invoiceno
			WHEN MATCHED THEN
			    DO NOTHING 
			WHEN NOT MATCHED THEN
			    INSERT (
                	invoiceno,
	            	invoicedate,
	            	customerid,
	            	customername,
	            	customersurname,
	            	phonenumber,
	            	country,
	            	countrysubregion,
	            	countryregion,
	            	stockcode,
	            	productname,
	            	"cost",
	            	unitprice,
	            	quantity,
	            	salesplatform,
	            	paymentmethod,
	            	insertdateproduct,
	            	enddateproduct
	            	) 
	            VALUES (
                	src.invoiceno,
	            	src.invoicedate,
	            	src.customerid,
	            	src.customername,
	            	src.customersurname,
	            	src.phonenumber,
	            	src.country,
	            	src.countrysubregion,
	            	src.countryregion,
	            	src.stockcode,
	            	src.productname,
	            	src."cost",
	            	src.unitprice,
	            	src.quantity,
	            	src.salesplatform,
	            	src.paymentmethod,
	            	src.insertdateproduct,
	            	src.enddateproduct
	            	) 
		;
 */