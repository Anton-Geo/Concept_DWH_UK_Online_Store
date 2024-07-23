--------- SALES ---------

-- Table - "BL_3NF"."CE_SALES":

CREATE TABLE IF NOT EXISTS "BL_3NF"."CE_SALES"(
    "SALE_ID"          BIGINT        NOT NULL,         -- SURR KEY
    "SALE_DT"          DATE          NOT NULL,         --FK
    "SALE_SRC_ID"      BIGINT        NOT NULL,
    "BUS_SALE_ID"      VARCHAR(255)  NOT NULL,
    "CUSTOMER_ID"      BIGINT        NOT NULL,         --FK
    "PRODUCT_ID"       BIGINT        NOT NULL,         --FK
    "PAYMENT_TYPE_ID"  INT           NOT NULL,         --FK
    "PLATFORM_ID"      INT           NOT NULL,         --FK
    "SALES_PRICE"      NUMERIC(10,2) NOT NULL,
    "QUANTITY"         INT           NOT NULL,
    "SOURCE_SYSTEM"    VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"    VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"     TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"     TIMESTAMP     NOT NULL,
    PRIMARY KEY ("SALE_ID", "SALE_DT"),
    CONSTRAINT fk_sale_customer      FOREIGN KEY ("CUSTOMER_ID")     REFERENCES "BL_3NF"."CE_CUSTOMERS"("CUSTOMER_ID"),
    CONSTRAINT fk_sale_product       FOREIGN KEY ("PRODUCT_ID")      REFERENCES "BL_3NF"."CE_PRODUCTS_SCD"("PRODUCT_ID"),
    CONSTRAINT fk_sale_payment_type  FOREIGN KEY ("PAYMENT_TYPE_ID") REFERENCES "BL_3NF"."CE_PAYMENT_TYPES"("PAYMENT_TYPE_ID"),
    CONSTRAINT fk_sale_platform      FOREIGN KEY ("PLATFORM_ID")     REFERENCES "BL_3NF"."CE_PLATFORMS"("PLATFORM_ID")
) PARTITION BY RANGE ("SALE_DT");

SELECT "BL_CL".fnc_create_partitions_for_ce_sales();

CREATE INDEX IF NOT EXISTS btree_ce_sales_src_id_idx
    ON "BL_3NF"."CE_SALES" ("SALE_SRC_ID");
   
-- Trigger for CE_SALES:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'ce_sales_ta_update_dt') THEN 
        CREATE TRIGGER ce_sales_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_3NF"."CE_SALES"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;