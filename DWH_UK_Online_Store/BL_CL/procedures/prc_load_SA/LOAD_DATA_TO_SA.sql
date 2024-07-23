------------- Master procedure to load date from all sources to SA -------------
CREATE OR REPLACE PROCEDURE "BL_CL"."LOAD_DATA_TO_SA"()
LANGUAGE plpgsql
AS
$$
BEGIN
	
	CALL "BL_CL".truncate_temp_tables_in_SA();

	-- EU server

	DROP INDEX IF EXISTS "SA_EUROPE_SERVER_SALES".btree_stockcode_src_eu_idx;
	CALL "BL_CL"."LOAD_DATA_TO_SRC_EUROPE_SALES"();
	ANALYZE;
	CREATE INDEX btree_stockcode_src_eu_idx
	    ON "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES" (StockCode, InsertDateProduct);
	ANALYZE;   

	CALL "BL_CL"."LOAD_DATA_TO_SRC_EUROPE_SALES_ARCH"();

	-- GLOBAL server

	DROP INDEX IF EXISTS "SA_GLOBAL_SERVER_SALES".btree_stockcode_src_glob_idx;
	CALL "BL_CL"."LOAD_DATA_TO_SRC_GLOBAL_SALES"();
	ANALYZE;
	CREATE INDEX btree_stockcode_src_glob_idx
	    ON "SA_GLOBAL_SERVER_SALES"."SRC_GLOBAL_SALES" (StockCode, InsertDateProduct);
	ANALYZE;

	CALL "BL_CL"."LOAD_DATA_TO_SRC_GLOBAL_SALES_ARCH"();

END;
$$;