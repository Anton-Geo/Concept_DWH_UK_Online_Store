-- Automatic partitioning of table CE_SALES:
CREATE OR REPLACE FUNCTION "BL_CL".fnc_create_partitions_for_ce_sales(
    p_start_year INT DEFAULT 2022,
    p_end_year   INT DEFAULT 2024
) 
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    start_date DATE := DATE(p_start_year || '-01-01');
    end_date DATE;
    year_i INT;
    quarter_j INT;
BEGIN
    FOR year_i IN p_start_year..p_end_year
    LOOP
        FOR quarter_j IN 1..4
        LOOP
            end_date := start_date + INTERVAL '3 months';
            EXECUTE FORMAT('CREATE TABLE IF NOT EXISTS "BL_3NF"."CE_SALES_%s_Q%s"
                            PARTITION OF "BL_3NF"."CE_SALES"
                            FOR VALUES FROM (%L::DATE) TO (%L::DATE)',
                            year_i, quarter_j, start_date, end_date);
--            EXECUTE FORMAT('ALTER TABLE "BL_DM"."FCT_SALES_DD_%s_Q%s"
--                            ADD PRIMARY KEY ("SALE_SURR_ID")',
--                            year_i, quarter_j);
            start_date := end_date;
        END LOOP;
    END LOOP;
END;
$$;