--------- PRODUCTS ---------

-- Table - "BL_3NF"."CE_PRODUCTS_SCD":

CREATE TABLE IF NOT EXISTS "BL_3NF"."CE_PRODUCTS_SCD"(
    "PRODUCT_ID"             BIGINT        PRIMARY KEY,      -- SURR KEY
    "PRODUCT_SRC_ID"         BIGINT        NOT NULL,
    "PRODUCT_CODE"           VARCHAR(255)  NOT NULL,
    "PRODUCT_NAME"           VARCHAR(255)  NOT NULL,
    "RETAIL_COST"            NUMERIC(10,2) NOT NULL,
    "PRODUCT_DESC"           TEXT          NOT NULL,
    "PRODUCT_SUBCATEGORY_ID" INT           NOT NULL,         --FK
    "START_DT"               TIMESTAMP     NOT NULL,
    "END_DT"                 TIMESTAMP     NOT NULL,
    "IS_ACTIVE"              VARCHAR(1)    NOT NULL,
    "SOURCE_SYSTEM"          VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"          VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"           TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"           TIMESTAMP     NOT NULL,
    CONSTRAINT fk_products FOREIGN KEY ("PRODUCT_SUBCATEGORY_ID") REFERENCES "BL_3NF"."CE_PRODUCT_SUBCATEGORIES"("PRODUCT_SUBCATEGORY_ID")
);

CREATE INDEX IF NOT EXISTS btree_ce_prod_doublet_id_idx
    ON "BL_3NF"."CE_PRODUCTS_SCD" ("PRODUCT_CODE", "START_DT");
   
-- Trigger for CE_PRODUCTS_SCD:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'ce_products_ta_update_dt') THEN 
        CREATE TRIGGER ce_products_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_3NF"."CE_PRODUCTS_SCD"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;