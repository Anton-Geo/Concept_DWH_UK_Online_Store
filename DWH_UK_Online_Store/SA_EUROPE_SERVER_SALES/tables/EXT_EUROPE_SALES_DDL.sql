-- Create an external table:
SELECT "BL_CL".fnc_create_ext_europe_sales();

/* Check result:

SELECT *
  FROM "SA_EUROPE_SERVER_SALES"."EXT_EUROPE_SALES";

 * The result is correct.
 */