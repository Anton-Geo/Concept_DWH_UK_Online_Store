----------- Loading data -----------
SET max_parallel_workers_per_gather = 16;

SET ROLE db_creator;
SELECT CURRENT_USER;

-- GRANT pg_analyze_all_tables TO db_creators;

CALL "BL_CL"."SET_DEFAULT_ROWS_TO_BL_3NF"();
CALL "BL_CL"."SET_DEFAULT_ROWS_TO_BL_DM"();
CALL "BL_CL"."MASTER_LOAD_DATA"();


-- Intial loading
-- Start time	Mon Jul 22 20:15:24 EEST 2024
-- Finish time	Mon Jul 22 20:20:14 EEST 2024

-- Incremental loading
-- Start time	Mon Jul 22 20:22:23 EEST 2024
-- Finish time	Mon Jul 22 20:23:17 EEST 2024
  
-- Incremental loading double
-- Start time	Mon Jul 22 20:25:05 EEST 2024
-- Finish time	Mon Jul 22 20:25:48 EEST 2024
/*