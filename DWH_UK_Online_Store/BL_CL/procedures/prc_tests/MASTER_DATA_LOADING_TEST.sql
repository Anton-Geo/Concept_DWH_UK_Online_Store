CREATE OR REPLACE PROCEDURE "BL_CL"."MASTER_DATA_LOADING_TEST"(
    IN sampling_percentage INT DEFAULT 1,    -- p%
    IN num_runs            INT DEFAULT 5     -- k, reliability = 1 - (1 - p) ** k
)
LANGUAGE plpgsql
AS
$$
DECLARE
    i INT;
BEGIN
	RAISE NOTICE 'Creating a combined source data table from archives.';
	CALL "BL_CL".prc_create_combined_source_data();
	ANALYZE "BL_CL".combined_source_data;

	RAISE NOTICE 'Checking for unique values ​​in Core entities in BL_3NF (exact match)';
    -- Default rows are not taken into account
	CALL "BL_CL"."TEST_DATA_IN_CE_REGIONS"();
	CALL "BL_CL"."TEST_DATA_IN_CE_SUBREGIONS"();
	CALL "BL_CL"."TEST_DATA_IN_CE_COUNTRIES"();
	CALL "BL_CL"."TEST_DATA_IN_CE_CUSTOMERS"();
	CALL "BL_CL"."TEST_DATA_IN_CE_PAYMENT_TYPES"();
	CALL "BL_CL"."TEST_DATA_IN_CE_PLATFORMS"();
	CALL "BL_CL"."TEST_DATA_IN_CE_PRODUCT_CATEGORIES"();
	CALL "BL_CL"."TEST_DATA_IN_CE_PRODUCT_SUBCATEGORIES"();
	CALL "BL_CL"."TEST_DATA_IN_CE_PRODUCTS_SCD"();
	CALL "BL_CL"."TEST_DATA_IN_CE_SALES"();

	RAISE NOTICE 'Probability testing FCT_SALES_DD via cursor tool.
	Sample: % %% of source data set, number of iterations: %.', 
                 sampling_percentage, num_runs;
    FOR i IN 1..num_runs LOOP
        CALL "BL_CL"."PROBABILITY_TESTING_DATA_IN_FCT_SALES_DD"(sampling_percentage);
    END LOOP;
   
	RAISE NOTICE 'Droppting a combined source data table.';
	DROP TABLE IF EXISTS "BL_CL".combined_source_data;
END;
$$;

/*
CALL "BL_CL"."MASTER_DATA_LOADING_TEST"(1, 5);
 */