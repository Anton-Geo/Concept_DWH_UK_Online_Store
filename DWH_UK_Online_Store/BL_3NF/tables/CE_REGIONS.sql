--------- REGIONS ---------

-- Parent table - "BL_3NF"."CE_REGIONS":
CREATE TABLE IF NOT EXISTS "BL_3NF"."CE_REGIONS"(
    "REGION_ID"      INT           PRIMARY KEY,      -- SURR KEY
    "REGION_SRC_ID"  INT           NOT NULL,
    "REGION_NAME"    VARCHAR(255)  NOT NULL,
    "SOURCE_SYSTEM"  VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"  VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"   TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"   TIMESTAMP     NOT NULL
);

CREATE INDEX IF NOT EXISTS btree_ce_regions_src_id_idx
    ON "BL_3NF"."CE_REGIONS" ("REGION_SRC_ID");
   
-- Trigger for CE_REGIONS:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'ce_regions_ta_update_dt') THEN 
        CREATE TRIGGER ce_regions_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_3NF"."CE_REGIONS"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;