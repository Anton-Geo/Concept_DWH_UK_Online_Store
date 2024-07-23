--------- SUBREGIONS ---------

-- Table - "BL_3NF"."CE_SUBREGIONS":

CREATE TABLE IF NOT EXISTS "BL_3NF"."CE_SUBREGIONS"(
    "SUBREGION_ID"     INT           PRIMARY KEY,      -- SURR KEY
    "SUBREGION_SRC_ID" INT           NOT NULL,
    "SUBREGION_NAME"   VARCHAR(255)  NOT NULL,
    "REGION_ID"        INT           NOT NULL,         --FK
    "SOURCE_SYSTEM"    VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"    VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"     TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"     TIMESTAMP     NOT NULL,
    CONSTRAINT fk_subregion FOREIGN KEY ("REGION_ID") REFERENCES "BL_3NF"."CE_REGIONS" ("REGION_ID")
);

CREATE INDEX IF NOT EXISTS btree_ce_subregions_src_id_idx
    ON "BL_3NF"."CE_SUBREGIONS" ("SUBREGION_SRC_ID");
   
-- Trigger for CE_SUBREGIONS:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'ce_subregions_ta_update_dt') THEN 
        CREATE TRIGGER ce_subregions_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_3NF"."CE_SUBREGIONS"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;