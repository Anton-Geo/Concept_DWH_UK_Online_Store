CREATE TABLE IF NOT EXISTS "BL_CL"."WRK_COUNTRIES"(
    "COUNTRY_SURR_ID" INT        PRIMARY KEY,
    "COUNTRY_SRC_ID"  VARCHAR(255)  NOT NULL,
    "COUNTRY_NAME"    VARCHAR(255)  NOT NULL,
    "SOURCE_SYSTEM"   VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"   VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"    TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"    TIMESTAMP     NOT NULL
);

CREATE INDEX btree_wrk_countries_src_id_idx
    ON "BL_CL"."WRK_COUNTRIES" ("COUNTRY_SRC_ID");
   
-- Trigger for WRK_COUNTRIES:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'wrk_countries_ta_update_dt') THEN 
        CREATE TRIGGER wrk_countries_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_CL"."WRK_COUNTRIES"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;