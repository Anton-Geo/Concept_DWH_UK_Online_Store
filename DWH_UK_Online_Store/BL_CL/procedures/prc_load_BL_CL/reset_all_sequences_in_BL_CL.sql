CREATE OR REPLACE PROCEDURE "BL_CL".reset_all_sequences_in_BL_CL()
LANGUAGE plpgsql
AS
$$
BEGIN
	
	ALTER SEQUENCE "BL_CL".wrk_region_seq              RESTART WITH 1;
	ALTER SEQUENCE "BL_CL".wrk_subregion_seq           RESTART WITH 1;
	ALTER SEQUENCE "BL_CL".wrk_countries_seq           RESTART WITH 1;
	ALTER SEQUENCE "BL_CL".wrk_customer_seq            RESTART WITH 1;
	ALTER SEQUENCE "BL_CL".wrk_platforms_seq           RESTART WITH 1;
	ALTER SEQUENCE "BL_CL".wrk_payment_types_seq       RESTART WITH 1;
	ALTER SEQUENCE "BL_CL".wrk_product_category_seq    RESTART WITH 1;
	ALTER SEQUENCE "BL_CL".wrk_product_subcategory_seq RESTART WITH 1;
	ALTER SEQUENCE "BL_CL".wrk_products_seq            RESTART WITH 1;
	ALTER SEQUENCE "BL_CL".wrk_sales_seq               RESTART WITH 1;
END;
$$;