CREATE OR REPLACE PROCEDURE "BL_CL".truncate_log_procedures()
LANGUAGE plpgsql
AS
$$
BEGIN
	-- !!! METADATA !!! Only super user can delete !!!
	TRUNCATE TABLE "BL_CL"."LOG_PROCEDURES"  CASCADE;
END;
$$;