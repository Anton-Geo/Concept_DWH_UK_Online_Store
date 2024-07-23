--------- Procedure to test data in CE_PAYMENT_TYPES ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."TEST_DATA_IN_CE_PAYMENT_TYPES"()
LANGUAGE plpgsql
AS
$$
DECLARE
    v_unique_values_in_source INT := 0;
    v_unique_values_in_target INT := 0;
    v_success_flag            BOOLEAN := TRUE;
BEGIN
	
	WITH subq AS(
		SELECT DISTINCT(src.paymentmethod)                    AS source_values, 
		       trgt."PAYMENT_TYPE_NAME"                       AS target_values,
		       (src.paymentmethod = trgt."PAYMENT_TYPE_NAME") AS is_equal
		  FROM "BL_CL".combined_source_data AS src
		       FULL OUTER JOIN "BL_3NF"."CE_PAYMENT_TYPES" AS trgt
		       ON src.paymentmethod = trgt."PAYMENT_TYPE_NAME"
		 WHERE trgt."PAYMENT_TYPE_ID"  != -1
	)
	SELECT COUNT(source_values),      COUNT(target_values),      COALESCE(BOOL_AND(is_equal), TRUE)
	  INTO v_unique_values_in_source, v_unique_values_in_target, v_success_flag
	  FROM subq;
	
    RAISE NOTICE 'TEST_DATA_IN_CE_PAYMENT_TYPES: %/%; Is success: %', 
                 v_unique_values_in_source, v_unique_values_in_target, v_success_flag;
   
	CALL "BL_CL"."LOG_TEST_EXECUTION"(
    	'TEST_DATA_IN_CE_PAYMENT_TYPES',
	    'BL_3NF.CE_PAYMENT_TYPES',
	    (v_unique_values_in_source, v_unique_values_in_target),
	    v_success_flag
    );
   COMMIT;
END;
$$;