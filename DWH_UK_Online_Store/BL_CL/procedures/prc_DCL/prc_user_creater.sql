CREATE OR REPLACE PROCEDURE "BL_CL".prc_user_creater(
    IN username     VARCHAR(255), 
    IN userpassword VARCHAR(255) DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    user_exists BOOLEAN;
BEGIN
    SELECT EXISTS(SELECT 1 
                    FROM pg_roles
                   WHERE rolname = username)
           INTO user_exists;

    IF user_exists THEN
        RAISE NOTICE 'User % already exists.', username;
    ELSE
        IF userpassword IS NULL THEN
            EXECUTE FORMAT('CREATE USER %I ', 
                            username);
        ELSE
            EXECUTE FORMAT('CREATE USER %I WITH PASSWORD %L', 
                           username, userpassword);
        END IF;
        RAISE NOTICE 'User % created successfully.', username;
    END IF;
END;
$$;