-- Regression test for MSan use-of-uninitialized-value in typeid_cast called from
-- SetVariants::chooseMethod via IntersectOrExceptTransform::accumulate.
-- The issue manifests when INTERSECT/EXCEPT processes chunks with Nullable columns
-- (e.g. from RIGHT JOIN) combined with constant columns that get converted
-- by convertToFullColumnIfConst.

SELECT *, isNullable(12), *, name < 'send_timeout', 12, 12, 723, 778
FROM system.merge_tree_settings INNER JOIN system.settings AS s ON equals(s.name, type)
INTERSECT DISTINCT
SELECT *, 12, *, name < 'send_timeout', 12, 12, 723, 778
FROM system.merge_tree_settings RIGHT JOIN system.settings AS s ON equals(s.name, type)
FORMAT Null;
