CREATE OR REPLACE PROCEDURE "BL_CL"."MASTER_LOAD_DATA"()
LANGUAGE plpgsql
AS
$$
BEGIN
	RAISE NOTICE 'Extracting source data to Staging Area';
	CALL "BL_CL"."LOAD_DATA_TO_SA"();
	RAISE NOTICE 'Transforming and loading data to Cleansing Layer - BL_CL';
	CALL "BL_CL"."LOAD_DATA_TO_BL_CL"();
	RAISE NOTICE 'Transforming and loading data to Core Layer - BL_3NF';
	CALL "BL_CL"."LOAD_DATA_TO_BL_3NF"();
	RAISE NOTICE 'Transforming and loading data to Star-schema - BL_DM';
	CALL "BL_CL"."LOAD_DATA_TO_BL_DM"();
END;
$$;

