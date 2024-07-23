--------- PAYMENT TYPES ---------

-- Parent table - "BL_3NF"."CE_PAYMENT_TYPES":

CREATE TABLE IF NOT EXISTS "BL_3NF"."CE_PAYMENT_TYPES"(
    "PAYMENT_TYPE_ID"     INT           PRIMARY KEY,      -- SURR KEY
    "PAYMENT_TYPE_SRC_ID" INT           NOT NULL,
    "PAYMENT_TYPE_NAME"   VARCHAR(255)  NOT NULL,
    "SOURCE_SYSTEM"       VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"       VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"        TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"        TIMESTAMP     NOT NULL
);

CREATE INDEX IF NOT EXISTS btree_ce_payment_types_src_id_idx
    ON "BL_3NF"."CE_PAYMENT_TYPES" ("PAYMENT_TYPE_SRC_ID");
   
-- Trigger for CE_PAYMENT_TYPES:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'ce_payment_types_ta_update_dt') THEN 
        CREATE TRIGGER ce_payment_types_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_3NF"."CE_PAYMENT_TYPES"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;