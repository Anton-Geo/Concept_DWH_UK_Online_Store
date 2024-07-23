CREATE TABLE IF NOT EXISTS "BL_CL"."WRK_PRODUCT_CATEGORIES"(
    "PRODUCT_CATEGORY_SURR_ID" INT           PRIMARY KEY,
    "PRODUCT_CATEGORY_SRC_ID"  VARCHAR(255)  NOT NULL,
    "PRODUCT_CATEGORY_NAME"    VARCHAR(255)  NOT NULL,
    "SOURCE_SYSTEM"            VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"            VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"             TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"             TIMESTAMP     NOT NULL
);

CREATE INDEX IF NOT EXISTS btree_wrk_prod_cut_src_id_idx
    ON "BL_CL"."WRK_PRODUCT_CATEGORIES" ("PRODUCT_CATEGORY_SRC_ID");
   
-- Trigger for WRK_PRODUCT_CATEGORIES:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'wrk_prod_cat_ta_update_dt') THEN 
        CREATE TRIGGER wrk_prod_cat_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_CL"."WRK_PRODUCT_CATEGORIES"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;