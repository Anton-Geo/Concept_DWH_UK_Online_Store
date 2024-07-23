--------- Procedure to test data in CE_PRODUCT_CATEGORIES ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."TEST_DATA_IN_CE_PRODUCT_CATEGORIES"()
LANGUAGE plpgsql
AS
$$
DECLARE
    v_unique_values_in_source INT := 0;
    v_unique_values_in_target INT := 0;
    v_success_flag            BOOLEAN := TRUE;
BEGIN
	
	WITH subq AS(
		SELECT DISTINCT(src.mainproductcategory)                        AS source_values, 
		       trgt."PRODUCT_CATEGORY_NAME"                             AS target_values,
		       (src.mainproductcategory = trgt."PRODUCT_CATEGORY_NAME") AS is_equal
		  FROM "BL_CL".combined_source_data AS src
		       FULL OUTER JOIN "BL_3NF"."CE_PRODUCT_CATEGORIES" AS trgt
		       ON src.mainproductcategory = trgt."PRODUCT_CATEGORY_NAME"
		 WHERE trgt."PRODUCT_CATEGORY_ID"  != -1
	)
	SELECT COUNT(source_values),      COUNT(target_values),      COALESCE(BOOL_AND(is_equal), TRUE)
	  INTO v_unique_values_in_source, v_unique_values_in_target, v_success_flag
	  FROM subq;
	
    RAISE NOTICE 'TEST_DATA_IN_CE_PRODUCT_CATEGORIES: %/%; Is success: %', 
                 v_unique_values_in_source, v_unique_values_in_target, v_success_flag;
   
	CALL "BL_CL"."LOG_TEST_EXECUTION"(
    	'TEST_DATA_IN_CE_PRODUCT_CATEGORIES',
	    'BL_3NF.CE_PRODUCT_CATEGORIES',
	    (v_unique_values_in_source, v_unique_values_in_target),
	    v_success_flag
    );
   COMMIT;
END;
$$;