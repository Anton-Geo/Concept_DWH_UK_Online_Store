-----------------------------------------------------
/*
TRUNCATE TABLE "BL_CL"."WRK_SALES" CASCADE;
ALTER SEQUENCE "BL_CL".wrk_sales_seq RESTART WITH 1;
 */
--------- Procedure to loading data in the WRK_SALES table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_FROM_UE_TO_WRK_SALES"()
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
	  FROM "BL_CL"."WRK_SALES";
	 
	v_start_time := clock_timestamp();
    BEGIN
		MERGE INTO "BL_CL"."WRK_SALES" AS target
		USING (
		    SELECT DISTINCT
		           FIRST_VALUE(InvoiceNo)                  OVER w AS invoice_num,
		           FIRST_VALUE(CASE WHEN (InvoiceDate ~ '^\d{4}-\d{2}-\d{2}') THEN TO_TIMESTAMP(InvoiceDate, 'YYYY-MM-DD')::DATE 
		                            ELSE '1900-01-01' END) OVER w AS sale_dt,
		           FIRST_VALUE(CustomerID)                 OVER w AS customerid,
		           FIRST_VALUE(StockCode)                  OVER w AS stock_code,
		           FIRST_VALUE(CASE WHEN (InsertDateProduct ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$') 
		                            THEN InsertDateProduct::TIMESTAMP
		                            ELSE '1900-01-01 00:00:00' END) 
		                                                   OVER w AS start_dt,  
		           FIRST_VALUE(paymentmethod)              OVER w AS payment_type,
		           FIRST_VALUE(salesplatform)              OVER w AS platform,
		           FIRST_VALUE(CASE WHEN UnitPrice ~ '^[0-9.]*$' AND LENGTH(REPLACE(UnitPrice, '.', '')) <= 10 THEN UnitPrice::NUMERIC(10,2)
		                            ELSE -1 END)           OVER w AS sales_price,
		           FIRST_VALUE(CASE WHEN Quantity ~ '^[0-9]*$' THEN Quantity::INT
		                            ELSE -1 END)           OVER w AS quantity,
		           'SA_EUROPE_SERVER_SALES'                       AS source_system,
		           'SRC_EUROPE_SALES'                             AS source_entity
		      FROM "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES"
		    WINDOW w AS (PARTITION BY InvoiceNo, InvoiceDate, CustomerID, StockCode, paymentmethod, salesplatform 
		                     ORDER BY InvoiceNo, InvoiceDate)
		) AS src
		ON  target."SALE_SRC_ID"         = src.invoice_num
		WHEN NOT MATCHED THEN
		    INSERT (
			    "SALE_SURR_ID",
			    "SALE_SRC_ID",
			    "SALE_DT",
			    "BUS_SALE_ID",
			    "BUS_CUSTOMER_ID",
			    "PRODUCT_CODE",
			    "START_DT",
			    "PAYMENT_TYPE_NAME",
			    "PLATFORM_NAME",
			    "SALES_PRICE",
			    "QUANTITY",
			    "SOURCE_SYSTEM",
			    "SOURCE_ENTITY",
			    "TA_INSERT_DT",
			    "TA_UPDATE_DT"
		    ) VALUES (
		        nextval('"BL_CL".wrk_sales_seq'),
		        src.invoice_num,
		        src.sale_dt,
		        src.invoice_num,
		        src.customerid,
		        src.stock_code,
		        src.start_dt,
		        src.payment_type,
		        src.platform,
		        src.sales_price,
		        src.quantity,
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
	  FROM "BL_CL"."WRK_SALES";
	 
	v_inserted_rows := v_inserted_rows - v_initial_rows;

    RAISE NOTICE 'Numbers of inserted/updated rows to WRK_SALES: %/%; Is success: %', 
                 v_inserted_rows, v_updated_rows, v_success_flag;
   
	CALL "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    	'LOAD_DATA_FROM_UE_TO_WRK_SALES',
	    'BL_CL.WRK_SALES',
	    v_inserted_rows,
	    v_updated_rows,
	    v_success_flag,
	    EXTRACT(EPOCH FROM v_end_time - v_start_time)
    );
   
	COMMIT;
END;
$$;