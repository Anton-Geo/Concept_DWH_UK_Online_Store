CREATE OR REPLACE FUNCTION "BL_CL".trigger_ta_update_dt()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        NEW."TA_UPDATE_DT" := NOW() AT TIME ZONE 'UTC';
    END IF;
    RETURN NEW;
END;
$$;