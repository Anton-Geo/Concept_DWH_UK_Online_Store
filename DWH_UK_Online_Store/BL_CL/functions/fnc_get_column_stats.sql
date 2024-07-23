-- DROP FUNCTION public.get_column_stats(text, text);

CREATE OR REPLACE FUNCTION "BL_CL".fnc_get_column_stats(
    IN p_schema_name TEXT, 
    IN p_table_name  TEXT)
RETURNS TABLE (
    column_title     TEXT,
    unique_count     BIGINT,
    null_count       BIGINT,
    total_count      BIGINT,
    column_data_type TEXT
) 
LANGUAGE plpgsql
AS $$
DECLARE
    query TEXT;
BEGIN
    FOR column_title, column_data_type IN
        SELECT column_name, data_type
          FROM information_schema.columns
         WHERE table_schema = p_schema_name 
           AND table_name   = p_table_name
    LOOP
        query := format('SELECT ''%1$s'' AS column_title, 
                                (SELECT COUNT(DISTINCT "%1$s") FROM %3$I.%4$I) AS unique_count, 
                                (SELECT COUNT(*) FROM %3$I.%4$I WHERE "%1$s" IS NULL) AS null_count, 
                                (SELECT COUNT(*) FROM %3$I.%4$I) AS total_count,
                                ''%2$s'' AS data_type', 
                         column_title, column_data_type, p_schema_name, p_table_name);
        RETURN QUERY EXECUTE query;
    END LOOP;
END;
$$;