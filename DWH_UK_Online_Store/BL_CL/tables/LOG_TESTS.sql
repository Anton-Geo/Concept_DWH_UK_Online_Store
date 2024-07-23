CREATE TYPE "BL_CL".src_trgt_value_type AS (
    source_count         BIGINT,
    target_count         BIGINT
);


CREATE TABLE "BL_CL"."LOG_TESTS" (
    "LOG_TEST_ID"        BIGINT       PRIMARY KEY,
    "TEST_NAME"          VARCHAR(255) NOT NULL,
    "TARGET_TABLE_NAME"  VARCHAR(255) NOT NULL,
    "UNIQUE_VALUE"       "BL_CL".src_trgt_value_type NOT NULL,
    "IS_SUCCESS"         VARCHAR(1)   NOT NULL,
    "TA_INSERT_DT"       TIMESTAMP    NOT NULL,
    "TA_UPDATE_DT"       TIMESTAMP    NOT NULL
);