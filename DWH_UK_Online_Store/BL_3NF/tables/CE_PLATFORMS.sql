--------- PLATFORMS ---------

-- Parent table - "BL_3NF"."CE_PLATFORMS":

CREATE TABLE IF NOT EXISTS "BL_3NF"."CE_PLATFORMS"(
    "PLATFORM_ID"     INT           PRIMARY KEY,      -- SURR KEY
    "PLATFORM_SRC_ID" INT           NOT NULL,
    "PLATFORM_NAME"   VARCHAR(255)  NOT NULL,
    "SOURCE_SYSTEM"   VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"   VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"    TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"    TIMESTAMP     NOT NULL
);

CREATE INDEX IF NOT EXISTS btree_ce_platforms_src_id_idx
    ON "BL_3NF"."CE_PLATFORMS" ("PLATFORM_SRC_ID");
   
-- Trigger for CE_PLATFORMS:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'ce_platforms_ta_update_dt') THEN 
        CREATE TRIGGER ce_platforms_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_3NF"."CE_PLATFORMS"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;