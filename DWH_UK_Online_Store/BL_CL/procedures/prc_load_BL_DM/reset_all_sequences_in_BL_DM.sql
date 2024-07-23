CREATE OR REPLACE PROCEDURE "BL_CL".reset_all_sequences_in_BL_DM()
LANGUAGE plpgsql
AS
$$
BEGIN
	
	ALTER SEQUENCE "BL_DM".dm_payment_types_seq RESTART WITH 1;
	ALTER SEQUENCE "BL_DM".dm_platforms_seq     RESTART WITH 1;
	ALTER SEQUENCE "BL_DM".dm_customers_seq     RESTART WITH 1;
	ALTER SEQUENCE "BL_DM".dm_products_seq      RESTART WITH 1;
	ALTER SEQUENCE "BL_DM".fct_sales_seq        RESTART WITH 1;
END;
$$;