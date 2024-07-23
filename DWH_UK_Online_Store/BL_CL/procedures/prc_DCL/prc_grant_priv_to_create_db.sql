CREATE OR REPLACE PROCEDURE "BL_CL".prc_grant_priv_to_create_db(
    IN group_name VARCHAR(255)
)
LANGUAGE plpgsql
AS $$
DECLARE
    schema_name VARCHAR(255);
    table_name  VARCHAR(255);
    role_exists BOOLEAN;
    row         RECORD;
BEGIN

    SELECT EXISTS (SELECT 1 
                     FROM pg_roles
                    WHERE rolname = group_name) INTO role_exists;
                   
    IF NOT role_exists THEN
        RAISE NOTICE 'Role % does not exist', group_name;
        RETURN;
    END IF;
   
    FOR schema_name IN 
        SELECT unnest(ARRAY['public', 'SA_EUROPE_SERVER_SALES', 'SA_GLOBAL_SERVER_SALES', 'BL_CL', 'BL_3NF', 'BL_DM'])
    LOOP
        EXECUTE FORMAT('GRANT CREATE, USAGE                                          ON SCHEMA %I TO %I', schema_name, group_name);
        EXECUTE FORMAT('GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA %I TO %I', schema_name, group_name);
        EXECUTE FORMAT('GRANT ALL PRIVILEGES ON ALL SEQUENCES                        IN SCHEMA %I TO %I', schema_name, group_name);
    END LOOP;

    FOR schema_name IN 
        SELECT unnest(ARRAY['public', 'BL_CL'])
    LOOP
        EXECUTE FORMAT('GRANT EXECUTE ON ALL FUNCTIONS  IN SCHEMA %I TO %I', schema_name, group_name);
        EXECUTE FORMAT('GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA %I TO %I', schema_name, group_name);
    END LOOP;

    FOR row IN 
        SELECT jsonb_array_elements('[{"schema": "SA_EUROPE_SERVER_SALES", "table": "SRC_EUROPE_SALES"}, 
                                      {"schema": "SA_GLOBAL_SERVER_SALES", "table": "SRC_GLOBAL_SALES"}, 
                                      {"schema": "BL_CL", "table": "WRK_CUSTOMERS"}, 
                                      {"schema": "BL_CL", "table": "WRK_PRODUCTS_SCD"}, 
                                      {"schema": "BL_3NF", "table": "CE_CUSTOMERS"}, 
                                      {"schema": "BL_3NF", "table": "CE_PRODUCTS_SCD"}, 
                                      {"schema": "BL_DM", "table": "FCT_SALES_DD"}, 
                                      {"schema": "BL_DM", "table": "DIM_TIME_DAY"}, 
                                      {"schema": "BL_DM", "table": "DIM_CUSTOMERS"}, 
                                      {"schema": "BL_DM", "table": "DIM_PRODUCTS_SCD"}, 
                                      {"schema": "BL_DM", "table": "DIM_PAYMENT_TYPES"}, 
                                      {"schema": "BL_DM", "table": "DIM_PLATFORMS"}]') AS elem
    LOOP
        schema_name := row.elem->>'schema';
        table_name := row.elem->>'table';
        EXECUTE FORMAT('ALTER TABLE %I.%I OWNER TO %I', schema_name, table_name, group_name);
    END LOOP;

    -- In PostgreSQL 16 there is role pg_analyze_all_tables
    SELECT EXISTS (SELECT 1 
                     FROM pg_roles 
                    WHERE rolname = 'pg_analyze_all_tables') INTO role_exists;
    IF role_exists THEN
        EXECUTE FORMAT('GRANT pg_analyze_all_tables TO %I', group_name);
    END IF;

    RAISE NOTICE 'Privileges to create the database are granted to %', group_name;
END;
$$;

/*
CALL "BL_CL".prc_grant_priv_to_create_db('db_creators')
 */