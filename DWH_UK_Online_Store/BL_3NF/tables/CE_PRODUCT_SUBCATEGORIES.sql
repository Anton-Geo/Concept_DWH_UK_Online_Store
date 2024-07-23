--------- PRODUCT SUBCATEGORIES ---------

-- Table - "BL_3NF"."CE_PRODUCT_SUBCATEGORIES":

CREATE TABLE IF NOT EXISTS "BL_3NF"."CE_PRODUCT_SUBCATEGORIES"(
    "PRODUCT_SUBCATEGORY_ID"     INT           PRIMARY KEY,      -- SURR KEY
    "PRODUCT_SUBCATEGORY_SRC_ID" INT           NOT NULL,
    "PRODUCT_SUBCATEGORY_NAME"   VARCHAR(255)  NOT NULL,
    "PRODUCT_CATEGORY_ID"        INT           NOT NULL,         --FK
    "SOURCE_SYSTEM"              VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"              VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"               TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"               TIMESTAMP     NOT NULL,
    CONSTRAINT fk_subcategory FOREIGN KEY ("PRODUCT_CATEGORY_ID") REFERENCES "BL_3NF"."CE_PRODUCT_CATEGORIES" ("PRODUCT_CATEGORY_ID")
);

CREATE INDEX IF NOT EXISTS btree_ce_product_subcategory_src_id_idx
    ON "BL_3NF"."CE_PRODUCT_SUBCATEGORIES" ("PRODUCT_SUBCATEGORY_SRC_ID");
   
-- Trigger for CE_PRODUCT_SUBCATEGORIES:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'ce_product_subcategory_ta_update_dt') THEN 
        CREATE TRIGGER ce_product_subcategory_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_3NF"."CE_PRODUCT_SUBCATEGORIES"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;