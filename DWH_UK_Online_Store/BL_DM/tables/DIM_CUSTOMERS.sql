--------- CUSTOMERS DIMENSION ---------

-- Dimension table - "BL_DM"."DIM_CUSTOMERS":

CREATE TABLE IF NOT EXISTS "BL_DM"."DIM_CUSTOMERS"(
    "CUSTOMER_SURR_ID" BIGINT        PRIMARY KEY,
    "CUSTOMER_SRC_ID"  BIGINT        NOT NULL,
    "BUS_CUSTOMER_ID"  VARCHAR(255)  NOT NULL,
    "CUSTOMER_NAME"    VARCHAR(255)  NOT NULL,
    "CUSTOMER_SURNAME" VARCHAR(255)  NOT NULL,
    "PHONE_NUM"        NUMERIC(11,0) NOT NULL,
    "COUNTRY_ID"       INT           NOT NULL,
    "COUNTRY_NAME"     VARCHAR(255)  NOT NULL,
    "SUBREGION_ID"     INT           NOT NULL,
    "SUBREGION_NAME"   VARCHAR(255)  NOT NULL,
    "REGION_ID"        INT           NOT NULL,
    "REGION_NAME"      VARCHAR(255)  NOT NULL,
    "SOURCE_SYSTEM"    VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"    VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"     TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"     TIMESTAMP     NOT NULL
);
   
-- Trigger for DIM_CUSTOMERS:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'dm_customers_ta_update_dt') THEN 
        CREATE TRIGGER dm_customers_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_DM"."DIM_CUSTOMERS"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;