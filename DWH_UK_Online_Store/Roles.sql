/*
SELECT CURRENT_USER;
SET ROLE postgres;
 */

CALL "BL_CL".prc_group_creater('db_creators');

CALL "BL_CL".prc_grant_priv_to_create_db('db_creators')

CALL "BL_CL".prc_user_creater('db_creator', 'complex_password');

GRANT db_creators TO db_creator;

SET ROLE db_creator;

/*
DROP ROLE IF EXISTS db_creator;
 */