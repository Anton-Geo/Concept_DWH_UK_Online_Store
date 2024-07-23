CREATE TABLE IF NOT EXISTS "BL_CL"."WRK_PLATFORMS"(
    "PLATFORM_SURR_ID" INT           PRIMARY KEY,
    "PLATFORM_SRC_ID"  VARCHAR(255)  NOT NULL,
    "PLATFORM_NAME"    VARCHAR(255)  NOT NULL,
    "SOURCE_SYSTEM"    VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"    VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"     TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"     TIMESTAMP     NOT NULL
);
   
-- Trigger for WRK_PLATFORMS:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'wrk_platforms_ta_update_dt') THEN 
        CREATE TRIGGER wrk_platforms_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_CL"."WRK_PLATFORMS"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;