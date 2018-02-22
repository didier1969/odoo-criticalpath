DROP FUNCTION getPaths(Integer, Integer);

CREATE OR REPLACE FUNCTION getPaths(_product_tmpl_id Integer, _product_id Integer DEFAULT null)
    RETURNS TABLE (parent_id Integer, child_id Integer, parent_code VARCHAR, child_code VARCHAR, parent_name VARCHAR, child_name VARCHAR, path_delay REAL, path_depth INTEGER, path_string VARCHAR) AS $$ 
DECLARE
BEGIN
RETURN QUERY
WITH RECURSIVE children(parent_id, child_id, parent_code, child_code, parent_name, child_name, path_delay, path_depth, path_string) AS
(
    SELECT
        0 AS parent_id,
        mb.product_tmpl_id AS child_id,
        'root'::varchar AS parent_code,
        ptc.default_code AS child_code,
        'root'::varchar AS parent_name,
        ptc.name AS child_name,
        ptc.produce_delay::real AS path_delay,
        1 AS path_depth,
        CAST ('(root) <-[' || ptc.produce_delay || ']-- (' || ptc.default_code || ')' AS VARCHAR) AS path_string
    FROM mrp_bom mb
    INNER JOIN product_template ptc ON ptc.id = mb.product_tmpl_id
    WHERE mb.product_tmpl_id = _product_tmpl_id AND CASE WHEN _product_id IS NULL THEN mb.product_id IS NULL ELSE mb.product_id = _product_id END
        UNION
    SELECT
        mb.product_tmpl_id AS parent_id,
        mbl.product_id AS child_id,
        ptp.default_code AS parent_code,
        ptc.default_code as child_code,
        ptp.name AS parent_name,
        ptc.name AS child_name,
        c.path_delay + ptc.produce_delay::real AS path_delay,
        c.path_depth + 1 AS path_depth,
        c.path_string || ' <-[' || ptc.produce_delay || ']-- (' || ptc.default_code || ')' AS path_string
    FROM mrp_bom mb
    INNER JOIN mrp_bom_line mbl ON mbl.bom_id = mb.id
    INNER JOIN product_template ptp ON ptp.id = mb.product_tmpl_id
    INNER JOIN product_template ptc ON ptc.id = mbl.product_id
    INNER JOIN children c ON c.child_id = mb.product_tmpl_id AND mb.product_id is null
) SELECT c.parent_id, c.child_id, c.parent_code, c.child_code, c.parent_name, c.child_name, c.path_delay, c.path_depth, c.path_string FROM children c;

END;     
$$ LANGUAGE plpgsql;

SELECT * FROM getPaths(5, 540)
