-- DDL for Physical table - SRC_EUROPE_SALES_ARCH:
CREATE TABLE IF NOT EXISTS "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES_ARCH"(
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
    EndDateProduct      VARCHAR(4000),
    "TA_INSERT_DT"      TIMESTAMP     NOT NULL,
    "TA_UPDATE_DT"      TIMESTAMP     NOT NULL
);