CREATE TABLE IF NOT EXISTS "BL_CL"."WRK_PAYMENT_TYPES"(
    "PAYMENT_TYPE_SURR_ID" INT           PRIMARY KEY,
    "PAYMENT_TYPE_SRC_ID"  VARCHAR(255)  NOT NULL,
    "PAYMENT_TYPE_NAME"    VARCHAR(255)  NOT NULL,
    "SOURCE_SYSTEM"        VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"        VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"         TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"         TIMESTAMP     NOT NULL
);

-- Trigger for WRK_PAYMENT_TYPES:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'wrk_payment_types_ta_update_dt') THEN 
        CREATE TRIGGER wrk_payment_types_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_CL"."WRK_PAYMENT_TYPES"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;