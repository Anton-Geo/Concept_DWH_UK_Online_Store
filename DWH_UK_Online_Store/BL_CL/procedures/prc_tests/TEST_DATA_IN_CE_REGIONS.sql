--------- Procedure to test data in CE_REGIONS ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."TEST_DATA_IN_CE_REGIONS"()
LANGUAGE plpgsql
AS
$$
DECLARE
    v_unique_values_in_source INT := 0;
    v_unique_values_in_target INT := 0;
    v_success_flag            BOOLEAN := TRUE;
BEGIN
	
	WITH subq AS(
		SELECT DISTINCT(src.countryregion)              AS source_values, 
		       trgt."REGION_NAME"                       AS target_values,
		       (src.countryregion = trgt."REGION_NAME") AS is_equal
		  FROM "BL_CL".combined_source_data AS src
		       FULL OUTER JOIN "BL_3NF"."CE_REGIONS" AS trgt
		       ON src.countryregion = trgt."REGION_NAME"
		 WHERE src.countryregion != 'Unspecified'
		   AND trgt."REGION_ID" != -1
	)
	SELECT COUNT(source_values),      COUNT(target_values),      COALESCE(BOOL_AND(is_equal), TRUE)
	  INTO v_unique_values_in_source, v_unique_values_in_target, v_success_flag
	  FROM subq;
	
    RAISE NOTICE 'TEST_DATA_IN_CE_REGIONS: %/%; Is success: %', 
                 v_unique_values_in_source, v_unique_values_in_target, v_success_flag;
   
	CALL "BL_CL"."LOG_TEST_EXECUTION"(
    	'TEST_DATA_IN_CE_REGIONS',
	    'BL_3NF.CE_REGIONS',
	    (v_unique_values_in_source, v_unique_values_in_target),
	    v_success_flag
    );
   COMMIT;
END;
$$;