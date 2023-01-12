
-- Create public_tables_metadata view
-- Created 2023-01-12


CREATE OR REPLACE VIEW public_tables_metadata AS 
SELECT
    cols.table_name, cols.column_name, cols.ordinal_position, cols.data_type, cols.udt_name, 
    cols.character_maximum_length, cols.numeric_precision, cols.numeric_scale, 
    cols.datetime_precision, cols.is_nullable,
    (
        SELECT
            pg_catalog.col_description(c.oid, cols.ordinal_position::int)
        FROM
            pg_catalog.pg_class c
        WHERE
            c.oid = (SELECT ('"' || cols.table_name || '"')::regclass::oid)
            AND c.relname = cols.table_name
    ) AS column_comment
FROM
    information_schema.columns cols
WHERE
    cols.table_catalog    = 'ps_shellfish'
--    AND cols.table_name   = 'survey'
    AND cols.table_schema = 'public';
    
-- Set permissions
-- ALTER TABLE public_tables_metadata OWNER TO stromas;
-- GRANT SELECT, UPDATE, INSERT, DELETE, REFERENCES, TRIGGER ON TABLE public_tables_metadata TO stromas;
-- GRANT SELECT ON TABLE public_tables_metadata TO public