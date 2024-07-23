CREATE TABLE IF NOT EXISTS "BL_CL"."WRK_PRODUCTS_SCD"(
    "PRODUCT_SURR_ID"          BIGINT        PRIMARY KEY,
    "PRODUCT_SRC_ID"           VARCHAR(255)  NOT NULL,
    "PRODUCT_CODE"             VARCHAR(255)  NOT NULL,
    "PRODUCT_NAME"             VARCHAR(255)  NOT NULL,
    "PRODUCT_SUBCATEGORY_NAME" VARCHAR(255)  NOT NULL,
    "PRODUCT_DESC"             TEXT          NOT NULL,
    "RETAIL_COST"              NUMERIC(10,2) NOT NULL,
    "START_DT"                 TIMESTAMP     NOT NULL,
    "END_DT"                   TIMESTAMP     NOT NULL,
    "IS_ACTIVE"                VARCHAR(1)    NOT NULL,
    "SOURCE_SYSTEM"            VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"            VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"             TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"             TIMESTAMP     NOT NULL
);

CREATE INDEX IF NOT EXISTS btree_wrk_products_scd_src_id_idx
    ON "BL_CL"."WRK_PRODUCTS_SCD" ("PRODUCT_SRC_ID", "START_DT");
   
-- Trigger for WRK_PRODUCTS:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'wrk_products_ta_update_dt') THEN 
        CREATE TRIGGER wrk_products_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_CL"."WRK_PRODUCTS_SCD"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;