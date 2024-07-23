--------- PAYMENT TYPES DIMENSION ---------

-- Dimension table - "BL_DM"."DIM_PAYMENT_TYPES":

CREATE TABLE IF NOT EXISTS "BL_DM"."DIM_PAYMENT_TYPES"(
    "PAYMENT_TYPE_SURR_ID" INT           PRIMARY KEY,
    "PAYMENT_TYPE_SRC_ID"  INT           NOT NULL,
    "PAYMENT_TYPE_NAME"    VARCHAR(255)  NOT NULL,
    "SOURCE_SYSTEM"        VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"        VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"         TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"         TIMESTAMP     NOT NULL
);

-- Trigger for DIM_PAYMENT_TYPES:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'de_payment_types_ta_update_dt') THEN 
        CREATE TRIGGER de_payment_types_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_DM"."DIM_PAYMENT_TYPES"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;