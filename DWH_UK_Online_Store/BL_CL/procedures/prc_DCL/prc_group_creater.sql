CREATE OR REPLACE PROCEDURE "BL_CL".prc_group_creater(
    IN group_name VARCHAR(255)
)
LANGUAGE plpgsql
AS $$
DECLARE
    group_exists BOOLEAN;
BEGIN
    SELECT EXISTS(SELECT 1 
                    FROM pg_roles
                   WHERE rolname = group_name)
           INTO group_exists;

    IF group_exists THEN
        RAISE NOTICE 'Group % already exists.', group_name;
    ELSE
        EXECUTE FORMAT('CREATE GROUP %I', group_name);
        RAISE NOTICE 'Group % added successfully.', group_name;
    END IF;
END;
$$;