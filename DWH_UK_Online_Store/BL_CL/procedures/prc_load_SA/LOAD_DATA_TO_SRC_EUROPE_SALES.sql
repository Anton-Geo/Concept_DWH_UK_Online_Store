-- DML for population source table:

--------- Procedure to loading data in the SRC_EUROPE_SALES ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_TO_SRC_EUROPE_SALES"()
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
	  FROM "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES";
	 
	v_start_time := clock_timestamp();
    BEGIN
	    INSERT INTO "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES"
		SELECT * 
		  FROM "SA_EUROPE_SERVER_SALES"."EXT_EUROPE_SALES"
		;
	    EXCEPTION WHEN OTHERS THEN
	        GET STACKED DIAGNOSTICS v_error_stack = PG_EXCEPTION_CONTEXT;
	        RAISE WARNING 'Call stack error: \"%\"', v_error_stack;
	        v_success_flag := FALSE;
	        ROLLBACK;
	END;
    v_end_time := clock_timestamp();
   
    SELECT COUNT(*) INTO v_inserted_rows
	  FROM "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;


    RAISE NOTICE 'Numbers of inserted/updated rows to SRC_EUROPE_SALES: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_TO_SRC_EUROPE_SALES',
	    'SA_EUROPE_SERVER_SALES.SRC_EUROPE_SALES',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;

/*
	    MERGE INTO "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES" AS target
			USING (SELECT *
			         FROM "SA_EUROPE_SERVER_SALES"."EXT_EUROPE_SALES"
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
					stockcode,
					productname,
					description,
					productcategory,
					mainproductcategory,
					unitcost,
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
					src.stockcode,
					src.productname,
					src.description,
					src.productcategory,
					src.mainproductcategory,
					src.unitcost,
					src.unitprice,
					src.quantity,
					src.salesplatform,
					src.paymentmethod,
					src.insertdateproduct,
					src.enddateproduct
	            	) 
		;
 */