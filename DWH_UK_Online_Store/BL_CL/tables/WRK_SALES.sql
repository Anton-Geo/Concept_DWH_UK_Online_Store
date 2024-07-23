CREATE TABLE IF NOT EXISTS "BL_CL"."WRK_SALES"(
    "SALE_SURR_ID"        BIGINT        NOT NULL,
    "SALE_DT"             DATE          NOT NULL,
    "SALE_SRC_ID"         VARCHAR(255)  NOT NULL,
    "BUS_SALE_ID"         VARCHAR(255)  NOT NULL,
    "BUS_CUSTOMER_ID"     VARCHAR(255)  NOT NULL,
    "PRODUCT_CODE"        VARCHAR(255)  NOT NULL,
    "START_DT"            TIMESTAMP     NOT NULL,
    "PAYMENT_TYPE_NAME"   VARCHAR(255)  NOT NULL,
    "PLATFORM_NAME"       VARCHAR(255)  NOT NULL,
    "SALES_PRICE"         NUMERIC(10,2) NOT NULL,
    "QUANTITY"            INT           NOT NULL,
    "SOURCE_SYSTEM"       VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"       VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"        TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"        TIMESTAMP     NOT NULL,
    PRIMARY KEY ("SALE_SURR_ID", "SALE_DT")
) PARTITION BY RANGE ("SALE_DT");

SELECT "BL_CL".fnc_create_partitions_for_wrk_sales();

CREATE INDEX IF NOT EXISTS btree_wrk_sales_src_id_idx
    ON "BL_CL"."WRK_SALES" ("SALE_SRC_ID");
   
-- Trigger for WRK_SALES:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'wrk_sales_ta_update_dt') THEN 
        CREATE TRIGGER wrk_sales_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_CL"."WRK_SALES"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;