-- PostgreSQL extension for working with external data
CREATE EXTENSION IF NOT EXISTS file_fdw;

-- Function to create server with existence check:
CREATE OR REPLACE FUNCTION "BL_CL".fnc_create_server(
    IN p_server_name TEXT) 
RETURNS VOID 
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 
                     FROM pg_foreign_server 
                    WHERE srvname = p_server_name) THEN
        EXECUTE 'CREATE SERVER "' || p_server_name || '" FOREIGN DATA WRAPPER file_fdw';
    ELSE
        RAISE NOTICE 'Server % already exists.', p_server_name;
    END IF;
END;
$$;