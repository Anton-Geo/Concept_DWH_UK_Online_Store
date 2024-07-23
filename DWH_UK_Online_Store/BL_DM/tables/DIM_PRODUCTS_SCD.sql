--------- PRODUCTS DIMENSION ---------

-- Table - "BL_DM"."DIM_PRODUCTS_SCD":

CREATE TABLE IF NOT EXISTS "BL_DM"."DIM_PRODUCTS_SCD"(
    "PRODUCT_SURR_ID"          BIGINT        PRIMARY KEY,
    "PRODUCT_SRC_ID"           BIGINT        NOT NULL,
    "PRODUCT_CODE"             VARCHAR(255)  NOT NULL,
    "PRODUCT_NAME"             VARCHAR(255)  NOT NULL,
    "RETAIL_COST"              NUMERIC(10,2) NOT NULL,
    "PRODUCT_DESC"             TEXT          NOT NULL,
    "PRODUCT_SUBCATEGORY_ID"   INT           NOT NULL,
    "PRODUCT_SUBCATEGORY_NAME" VARCHAR(255)  NOT NULL,
    "PRODUCT_CATEGORY_ID"      INT           NOT NULL,
    "PRODUCT_CATEGORY_NAME"    VARCHAR(255)  NOT NULL,
    "START_DT"                 TIMESTAMP     NOT NULL,
    "END_DT"                   TIMESTAMP     NOT NULL,
    "IS_ACTIVE"                VARCHAR(1)    NOT NULL,
    "SOURCE_SYSTEM"            VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"            VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"             TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"             TIMESTAMP     NOT NULL
);
   
-- Trigger for DM_PRODUCTS_SCD:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'dm_products_ta_update_dt') THEN 
        CREATE TRIGGER dm_products_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_DM"."DIM_PRODUCTS_SCD"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;