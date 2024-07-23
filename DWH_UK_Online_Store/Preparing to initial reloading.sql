----------- Preparing to initial loading data -----------

SET max_parallel_workers_per_gather = 8;

CALL "BL_CL"."MASTER_TRUNCATE_AND_RESET_SEQUENCES"();
CALL "BL_CL".truncate_arch_tables_in_SA();
CALL "BL_CL".truncate_log_procedures();

VACUUM FULL ANALYZE;
