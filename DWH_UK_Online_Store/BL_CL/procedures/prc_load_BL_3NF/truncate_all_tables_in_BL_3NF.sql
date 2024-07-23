CREATE OR REPLACE PROCEDURE "BL_CL".truncate_all_tables_in_BL_3NF()
LANGUAGE plpgsql
AS
$$
BEGIN
	
    TRUNCATE TABLE "BL_3NF"."CE_PRODUCTS_SCD"          CASCADE;
	TRUNCATE TABLE "BL_3NF"."CE_PRODUCT_SUBCATEGORIES" CASCADE;
	TRUNCATE TABLE "BL_3NF"."CE_PRODUCT_CATEGORIES"    CASCADE;
	TRUNCATE TABLE "BL_3NF"."CE_PAYMENT_TYPES"         CASCADE;
	TRUNCATE TABLE "BL_3NF"."CE_PLATFORMS"             CASCADE;
	TRUNCATE TABLE "BL_3NF"."CE_CUSTOMERS"             CASCADE;
	TRUNCATE TABLE "BL_3NF"."CE_COUNTRIES"             CASCADE;
	TRUNCATE TABLE "BL_3NF"."CE_SUBREGIONS"            CASCADE;
	TRUNCATE TABLE "BL_3NF"."CE_REGIONS"               CASCADE;
	TRUNCATE TABLE "BL_3NF"."CE_SALES"                 CASCADE;
END;
$$;