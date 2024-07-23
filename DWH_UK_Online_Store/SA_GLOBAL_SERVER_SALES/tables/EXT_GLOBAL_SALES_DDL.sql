-- Create an external table:
SELECT "BL_CL".fnc_create_ext_global_sales();

/* Check result:

SELECT *
  FROM "SA_GLOBAL_SERVER_SALES"."EXT_GLOBAL_SALES";

 * The result is correct.
 */