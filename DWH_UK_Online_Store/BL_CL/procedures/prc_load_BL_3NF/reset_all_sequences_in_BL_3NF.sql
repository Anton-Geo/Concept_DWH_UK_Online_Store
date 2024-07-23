CREATE OR REPLACE PROCEDURE "BL_CL".reset_all_sequences_in_BL_3NF()
LANGUAGE plpgsql
AS
$$
BEGIN
	
	ALTER SEQUENCE "BL_3NF".ce_products_seq            RESTART WITH 1;
	ALTER SEQUENCE "BL_3NF".ce_product_subcategory_seq RESTART WITH 1;
	ALTER SEQUENCE "BL_3NF".ce_product_category_seq    RESTART WITH 1;
	ALTER SEQUENCE "BL_3NF".ce_payment_types_seq       RESTART WITH 1;
	ALTER SEQUENCE "BL_3NF".ce_platforms_seq           RESTART WITH 1;
	ALTER SEQUENCE "BL_3NF".ce_customers_seq           RESTART WITH 1;
	ALTER SEQUENCE "BL_3NF".ce_countries_seq           RESTART WITH 1;
	ALTER SEQUENCE "BL_3NF".ce_subregions_seq          RESTART WITH 1;
	ALTER SEQUENCE "BL_3NF".ce_regions_seq             RESTART WITH 1;
	ALTER SEQUENCE "BL_3NF".ce_sales_seq               RESTART WITH 1;
END;
$$;