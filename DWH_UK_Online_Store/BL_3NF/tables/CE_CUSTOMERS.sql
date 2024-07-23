--------- CUSTOMERS ---------

-- Table - "BL_3NF"."CE_CUSTOMERS":

CREATE TABLE IF NOT EXISTS "BL_3NF"."CE_CUSTOMERS"(
    "CUSTOMER_ID"      BIGINT        PRIMARY KEY,      -- SURR KEY
    "CUSTOMER_SRC_ID"  BIGINT        NOT NULL,
    "BUS_CUSTOMER_ID"  VARCHAR(255)  NOT NULL,
    "CUSTOMER_NAME"    VARCHAR(255)  NOT NULL,
    "CUSTOMER_SURNAME" VARCHAR(255)  NOT NULL,
    "PHONE_NUM"        NUMERIC(11,0) NOT NULL,
    "COUNTRY_ID"       INT           NOT NULL,         --FK
    "SOURCE_SYSTEM"    VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"    VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"     TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"     TIMESTAMP     NOT NULL,
    CONSTRAINT fk_region FOREIGN KEY ("COUNTRY_ID") REFERENCES "BL_3NF"."CE_COUNTRIES" ("COUNTRY_ID")
);

CREATE INDEX IF NOT EXISTS btree_ce_bus_customers_id_idx
    ON "BL_3NF"."CE_CUSTOMERS" ("BUS_CUSTOMER_ID");
   
-- Trigger for CE_CUSTOMERS:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'ce_customers_ta_update_dt') THEN 
        CREATE TRIGGER ce_customers_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_3NF"."CE_CUSTOMERS"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;