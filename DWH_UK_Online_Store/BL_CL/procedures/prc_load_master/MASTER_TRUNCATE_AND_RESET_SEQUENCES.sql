CREATE OR REPLACE PROCEDURE "BL_CL"."MASTER_TRUNCATE_AND_RESET_SEQUENCES"()
LANGUAGE plpgsql
AS
$$
BEGIN
	RAISE NOTICE 'Truncating data and resetting sequences in tables from BL_DM';
	CALL "BL_CL".truncate_all_tables_in_BL_DM();
	CALL "BL_CL".reset_all_sequences_in_BL_DM();
	RAISE NOTICE 'Truncating data and resetting sequences in tables from BL_3NF';
	CALL "BL_CL".truncate_all_tables_in_BL_3NF();
	CALL "BL_CL".reset_all_sequences_in_BL_3NF();
	RAISE NOTICE 'Truncating data and resetting sequences in tables from BL_CL';
	CALL "BL_CL".truncate_all_tables_in_BL_CL();
	CALL "BL_CL".reset_all_sequences_in_BL_CL();
	RAISE NOTICE 'The database is ready for initial data loading';
END;
$$;
