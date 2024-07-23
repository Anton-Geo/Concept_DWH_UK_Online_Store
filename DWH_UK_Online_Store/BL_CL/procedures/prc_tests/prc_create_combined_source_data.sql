CREATE OR REPLACE PROCEDURE "BL_CL".prc_create_combined_source_data()
LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE '
    CREATE UNLOGGED TABLE IF NOT EXISTS "BL_CL".combined_source_data AS
    SELECT InvoiceNo,
           InvoiceDate,
           CustomerID,
           CustomerName,
           CustomerSurname,
           PhoneNumber,
           Country,
           NULL AS CountrySubRegion,
           NULL AS CountryRegion,
           StockCode,
           ProductName,
           Description,
           ProductCategory,
           MainProductCategory,
           UnitCost,
           UnitPrice,
           Quantity,
           SalesPlatform,
           PaymentMethod,
           InsertDateProduct,
           EndDateProduct
      FROM "SA_EUROPE_SERVER_SALES"."SRC_EUROPE_SALES_ARCH"
     UNION ALL
    SELECT InvoiceNo,
           InvoiceDate,
           CustomerID,
           CustomerName,
           CustomerSurname,
           PhoneNumber,
           Country,
           CountrySubRegion,
           CountryRegion,
           StockCode,
           ProductName,
           Description,
           ProductCategory,
           NULL AS MainProductCategory,
           Cost AS unitcost,
           UnitPrice,
           Quantity,
           SalesPlatform,
           PaymentMethod,
           InsertDateProduct,
           EndDateProduct
      FROM "SA_GLOBAL_SERVER_SALES"."SRC_GLOBAL_SALES_ARCH";
	';
END;
$$;
