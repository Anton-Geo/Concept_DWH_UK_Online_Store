--------- FACTS ABOUT SALES ---------

-- Fact table - "BL_DM"."FCT_SALES_DD":

CREATE TABLE IF NOT EXISTS "BL_DM"."FCT_SALES_DD"(
    "SALE_SURR_ID"         BIGINT        NOT NULL,
    "SALE_SRC_ID"          BIGINT        NOT NULL,
    "BUS_SALE_ID"          VARCHAR(255)  NOT NULL,
    "SALE_DT"              DATE          NOT NULL,         --FK
    "CUSTOMER_SURR_ID"     BIGINT        NOT NULL,         --FK
    "PRODUCT_SURR_ID"      BIGINT        NOT NULL,         --FK
    "PAYMENT_TYPE_SURR_ID" INT           NOT NULL,         --FK
    "PLATFORM_SURR_ID"     INT           NOT NULL,         --FK
    "FCT_RETAIL_COST_GBP"  NUMERIC(10,2) NOT NULL,
    "FCT_SALE_PRICE_GBP"   NUMERIC(10,2) NOT NULL,
    "FCT_QUANTITY"         INT           NOT NULL,
    "FCT_REVENUE_GBP"      NUMERIC(10,2) NOT NULL,
    "FCT_PROFIT_GBP"       NUMERIC(10,2) NOT NULL,
    "SOURCE_SYSTEM"        VARCHAR(255)  NOT NULL,
    "SOURCE_ENTITY"        VARCHAR(255)  NOT NULL,
    "TA_INSERT_DT"         TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"         TIMESTAMP     NOT NULL
   ,PRIMARY KEY ("SALE_SURR_ID", "SALE_DT"),
    CONSTRAINT fk_dm_sale_date         FOREIGN KEY ("SALE_DT")              REFERENCES "BL_DM"."DIM_TIME_DAY"("DATE_KEY"),
    CONSTRAINT fk_dm_sale_customer     FOREIGN KEY ("CUSTOMER_SURR_ID")     REFERENCES "BL_DM"."DIM_CUSTOMERS"("CUSTOMER_SURR_ID"),
    CONSTRAINT fk_dm_sale_product      FOREIGN KEY ("PRODUCT_SURR_ID")      REFERENCES "BL_DM"."DIM_PRODUCTS_SCD"("PRODUCT_SURR_ID"),
    CONSTRAINT fk_dm_sale_payment_type FOREIGN KEY ("PAYMENT_TYPE_SURR_ID") REFERENCES "BL_DM"."DIM_PAYMENT_TYPES"("PAYMENT_TYPE_SURR_ID"),
    CONSTRAINT fk_dm_sale_platform     FOREIGN KEY ("PLATFORM_SURR_ID")     REFERENCES "BL_DM"."DIM_PLATFORMS"("PLATFORM_SURR_ID")
) PARTITION BY RANGE ("SALE_DT");

SELECT "BL_CL".fnc_create_partitions_for_fct_sales_dd();

CREATE INDEX IF NOT EXISTS btree_fct_sales_cust_idx
    ON "BL_DM"."FCT_SALES_DD" ("CUSTOMER_SURR_ID");
   
CREATE INDEX IF NOT EXISTS btree_fct_sales_prod_idx
    ON "BL_DM"."FCT_SALES_DD" ("PRODUCT_SURR_ID");   

-- Trigger for CE_SALES:
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 
                   FROM pg_trigger 
                   WHERE tgname = 'fct_sales_ta_update_dt') THEN 
        CREATE TRIGGER fct_sales_ta_update_dt
        BEFORE INSERT OR UPDATE ON "BL_DM"."FCT_SALES_DD"
        FOR EACH ROW
        EXECUTE FUNCTION "BL_CL".trigger_ta_update_dt();
    END IF;
END $$;