--------- Procedure to loading data in the "BL_CL"."LOG_TESTS" table ---------

CREATE OR REPLACE PROCEDURE "BL_CL"."LOG_TEST_EXECUTION"(
    IN p_test_name         VARCHAR(255),
    IN p_target_table_name VARCHAR(255),
    IN p_unique_value      "BL_CL".src_trgt_value_type,
    IN p_success           BOOLEAN
)
LANGUAGE plpgsql
AS
$$
BEGIN
    INSERT INTO "BL_CL"."LOG_TESTS"(
	    "LOG_TEST_ID",
	    "TEST_NAME",
	    "TARGET_TABLE_NAME",
	    "UNIQUE_VALUE",
	    "IS_SUCCESS",
	    "TA_INSERT_DT",
	    "TA_UPDATE_DT"
    )
    VALUES (
        nextval('"BL_CL".log_tests_seq'),
        p_test_name,
        p_target_table_name,
		p_unique_value,
        CASE WHEN p_success THEN 'Y' ELSE 'N' END,
        NOW() AT TIME ZONE 'UTC',
        NOW() AT TIME ZONE 'UTC'
    );
END;
$$;