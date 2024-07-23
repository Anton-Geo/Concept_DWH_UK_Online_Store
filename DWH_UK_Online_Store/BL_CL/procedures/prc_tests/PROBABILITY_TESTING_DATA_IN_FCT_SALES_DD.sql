CREATE OR REPLACE PROCEDURE "BL_CL"."PROBABILITY_TESTING_DATA_IN_FCT_SALES_DD"(
    IN sampling_percentage INT DEFAULT 1
)
LANGUAGE plpgsql
AS
$$
DECLARE
    src_cursor CURSOR FOR 
                      (SELECT * 
                         FROM "BL_CL".combined_source_data 
                              TABLESAMPLE SYSTEM (sampling_percentage));
    src_row                   RECORD;
    trgt_count                INT;
    v_unique_values_in_source INT := 0;
    v_unique_values_in_target INT := 0;
    v_success_flag            BOOLEAN := TRUE;
BEGIN
    OPEN src_cursor;
    LOOP
        FETCH src_cursor INTO src_row;
        EXIT WHEN NOT FOUND;
       
        v_unique_values_in_source := v_unique_values_in_source + 1;
        
        SELECT COUNT(*) INTO trgt_count 
          FROM "BL_DM"."FCT_SALES_DD" AS s
               LEFT JOIN "BL_DM"."DIM_CUSTOMERS" AS c
               ON s."CUSTOMER_SURR_ID" = c."CUSTOMER_SURR_ID"
               LEFT JOIN "BL_DM"."DIM_PRODUCTS_SCD" AS p
               ON s."PRODUCT_SURR_ID"  = p."PRODUCT_SURR_ID"
               
         WHERE s."BUS_SALE_ID"         = src_row.invoiceno 
           AND c."BUS_CUSTOMER_ID"     = src_row.customerid
           AND p."PRODUCT_CODE"        = src_row.stockcode
           AND s."FCT_RETAIL_COST_GBP" = src_row.unitcost::NUMERIC(10,2)
           AND s."FCT_SALE_PRICE_GBP"  = src_row.unitprice::NUMERIC(10,2)
           AND s."FCT_QUANTITY"        = src_row.quantity::NUMERIC(10,2);
        
        v_unique_values_in_target := v_unique_values_in_target + trgt_count;
       
        IF    trgt_count = 0 THEN
            v_success_flag := FALSE;
            RAISE NOTICE 'Data mismatch found for source row: %', src_row;
        ELSIF trgt_count > 1 THEN
            v_success_flag := FALSE;
            RAISE NOTICE 'Multiple matches found for source row: %', src_row;
        END IF;
    END LOOP;
    CLOSE src_cursor;
   
    RAISE NOTICE 'PROBABILITY_TESTING_DATA_IN_FCT_SALES_DD: %/%; Is success: %', 
             v_unique_values_in_source, v_unique_values_in_target, v_success_flag;
                
	CALL "BL_CL"."LOG_TEST_EXECUTION"(
    	'PROBABILITY_TESTING_DATA_IN_FCT_SALES_DD',
	    'BL_DM.FCT_SALES_DD',
	    (v_unique_values_in_source, v_unique_values_in_target),
	    v_success_flag
    );
   COMMIT;
END;
$$;

/*
CALL "BL_CL".prc_create_combined_source_data();
CALL "BL_CL"."PROBABILITY_TESTING_DATA_IN_FCT_SALES_DD"(5);
 */