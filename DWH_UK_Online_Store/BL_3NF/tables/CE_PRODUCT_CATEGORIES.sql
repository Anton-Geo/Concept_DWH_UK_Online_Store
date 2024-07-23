--------- PRODUCT CATEGORY ---------

-- Parent table - "BL_3NF"."CE_PRODUCT_CATEGORIES":

CREATE TABLE IF NOT EXISTS "BL_3NF"."CE_PRODUCT_CATEGORIES"(
    "PRODUCT_CATEGORY_ID"     INT           PRIMARY KEY,      -- SURR KEY
    "PRODUCT_CATEGORY_SRC_ID" INT           NOT NULL,
    "PRODUCT_CATEGORY_NAME"   VARCHAR(255)  NOT NULL,
    "SOURCE_SYSTEM"           VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"           VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"            TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"            TIMESTAMP     NOT NULL
);

CREATE INDEX IF NOT EXISTS btree_ce_product_category_src_id_idx
    ON "BL_3NF"."CE_PRODUCT_CATEGORIES" ("PRODUCT_CATEGORY_SRC_ID");
   
-- Trigger for CE_PRODUCT_CATEGORIES:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'ce_product_category_ta_update_dt') THEN 
        CREATE TRIGGER ce_product_category_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_3NF"."CE_PRODUCT_CATEGORIES"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;