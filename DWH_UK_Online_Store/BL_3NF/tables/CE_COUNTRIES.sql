--------- COUNTRIES ---------

-- Table - "BL_3NF"."CE_COUNTRIES":

CREATE TABLE IF NOT EXISTS "BL_3NF"."CE_COUNTRIES"(
    "COUNTRY_ID"     INT           PRIMARY KEY,      -- SURR KEY
    "COUNTRY_SRC_ID" INT           NOT NULL,
    "COUNTRY_NAME"   VARCHAR(255)  NOT NULL,
    "SUBREGION_ID"   INT           NOT NULL,         --FK
    "SOURCE_SYSTEM"  VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"  VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"   TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"   TIMESTAMP     NOT NULL,
    CONSTRAINT fk_country FOREIGN KEY ("SUBREGION_ID") REFERENCES "BL_3NF"."CE_SUBREGIONS" ("SUBREGION_ID")
);

CREATE INDEX IF NOT EXISTS btree_ce_countries_src_id_idx
    ON "BL_3NF"."CE_COUNTRIES" ("COUNTRY_SRC_ID");
   
-- Trigger for CE_COUNTRIES:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'ce_countries_ta_update_dt') THEN 
        CREATE TRIGGER ce_countries_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_3NF"."CE_COUNTRIES"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;