-- Create an external table via function with existence check:
CREATE OR REPLACE FUNCTION "BL_CL".fnc_create_ext_global_sales()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
	-- Create a server for sales from a Global server with the opportunity rerunning:
	PERFORM "BL_CL".fnc_create_server('global_sales');
	
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'EXT_GLOBAL_SALES') THEN
        EXECUTE 'CREATE FOREIGN TABLE "SA_GLOBAL_SERVER_SALES"."EXT_GLOBAL_SALES" (
		    InvoiceNo           VARCHAR(4000),
		    InvoiceDate         VARCHAR(4000),
		    CustomerID          VARCHAR(4000),
		    CustomerName        VARCHAR(4000),
		    CustomerSurname     VARCHAR(4000),
		    PhoneNumber         VARCHAR(4000),
		    Country             VARCHAR(4000),
		    CountrySubRegion    VARCHAR(4000),
		    CountryRegion       VARCHAR(4000),
		    StockCode           VARCHAR(4000),
		    ProductName         VARCHAR(4000),
		    Description         VARCHAR(4000),
		    ProductCategory     VARCHAR(4000),
		    Cost                VARCHAR(4000),
		    UnitPrice           VARCHAR(4000),
		    Quantity            VARCHAR(4000),
		    SalesPlatform       VARCHAR(4000),
		    PaymentMethod       VARCHAR(4000),
            InsertDateProduct   VARCHAR(4000),
            EndDateProduct      VARCHAR(4000)
        )
		SERVER global_sales
		OPTIONS (filename ''../sources/GLOBAL_SERVER_SALES.csv'', 
                 FORMAT ''csv'', HEADER ''true'')';
    ELSE
        RAISE NOTICE 'Foreign table EXT_GLOBAL_SALES already exists.';
    END IF;
END;
$$;
