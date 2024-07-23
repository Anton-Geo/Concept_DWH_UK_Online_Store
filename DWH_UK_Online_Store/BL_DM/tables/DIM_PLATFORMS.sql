--------- PLATFORMS DIMENSION ---------

-- Dimension table - "BL_DM"."DM_PLATFORMS":

CREATE TABLE IF NOT EXISTS "BL_DM"."DIM_PLATFORMS"(
    "PLATFORM_SURR_ID" INT           PRIMARY KEY,
    "PLATFORM_SRC_ID"  INT           NOT NULL,
    "PLATFORM_NAME"    VARCHAR(255)  NOT NULL,
    "SOURCE_SYSTEM"    VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"    VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"     TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"     TIMESTAMP     NOT NULL
);

-- Trigger for DIM_PLATFORMS:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'dm_platforms_ta_update_dt') THEN 
        CREATE TRIGGER dm_platforms_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_DM"."DIM_PLATFORMS"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;