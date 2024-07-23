-- DDL for Physical table - SRC_EUROPE_SALES:
CREATE UNLOGGED TABLE IF NOT EXISTS "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES"(
    InvoiceNo           VARCHAR(4000),
    InvoiceDate         VARCHAR(4000),
    CustomerID          VARCHAR(4000),
    CustomerName        VARCHAR(4000),
    CustomerSurname     VARCHAR(4000),
    PhoneNumber         VARCHAR(4000),
    Country             VARCHAR(4000),
    StockCode           VARCHAR(4000),
    ProductName         VARCHAR(4000),
    Description         VARCHAR(4000),
    ProductCategory     VARCHAR(4000),
    MainProductCategory VARCHAR(4000),
    UnitCost            VARCHAR(4000),
    UnitPrice           VARCHAR(4000),
    Quantity            VARCHAR(4000),
    SalesPlatform       VARCHAR(4000),
    PaymentMethod       VARCHAR(4000),
    InsertDateProduct   VARCHAR(4000),
    EndDateProduct      VARCHAR(4000)
);
   
ALTER TABLE "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES"
  SET (autovacuum_enabled = TRUE);
 
CREATE INDEX btree_stockcode_src_eu_idx
    ON "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES" (StockCode, InsertDateProduct);