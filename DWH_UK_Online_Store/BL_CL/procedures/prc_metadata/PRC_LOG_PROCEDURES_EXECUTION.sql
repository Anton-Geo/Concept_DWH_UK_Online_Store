--------- Procedure to loading data in the "BL_CL"."LOG_PROCEDURES" table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOG_PROCEDURE_EXECUTION"(
    IN p_procedure_name VARCHAR(255),
    IN p_table_name     VARCHAR(255),
    IN p_rows_inserted  BIGINT,
    IN p_rows_updated   BIGINT,
    IN p_success        BOOLEAN,
    IN p_executed_time  FLOAT
)
LANGUAGE plpgsql
AS
$$
BEGIN
    INSERT INTO "BL_CL"."LOG_PROCEDURES"(
        "LOG_ID",
        "PROCEDURE_NAME",
        "TABLE_NAME",
        "INSERT_ROW_CNT",
        "UPDATE_ROW_CNT",
        "TRANSACTION_NUM",
        "IS_SUCCESS",
        "EXECITED_TIME_SEC",
        "TA_INSERT_DT",
        "TA_UPDATE_DT"
    )
    VALUES (
        nextval('"BL_CL".log_procedures_seq'),
        p_procedure_name,
        p_table_name,
        p_rows_inserted,
        p_rows_updated,
        txid_current(),
        CASE WHEN p_success THEN 'Y' ELSE 'N' END,
        p_executed_time,
        NOW() AT TIME ZONE 'UTC',
        NOW() AT TIME ZONE 'UTC'
    );
END;
$$;