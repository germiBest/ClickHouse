-- Regression test for MSan use-of-uninitialized-value in SetVariants::chooseMethod
-- called from IntersectOrExceptTransform::accumulate via typeid_cast/checkAndGetColumn.
-- The issue manifests when EXCEPT/INTERSECT processes columns including Nullable types.

-- Basic EXCEPT with Nullable columns
(SELECT DISTINCT *, name <=> 'send_timeout', toNullable(toUInt256(12)) FROM system.settings WHERE 'send_timeout' != name LIMIT 9223372036854775807, 1048576) EXCEPT (SELECT DISTINCT *, name <=> 'send_timeout', toNullable(toUInt256(12)) FROM system.settings WHERE 'send_timeout' != name LIMIT 9223372036854775807, 1048576) FORMAT Null;

-- Simpler variant without the huge LIMIT
(SELECT *, toNullable(toUInt256(12)) FROM system.one) EXCEPT (SELECT *, toNullable(toUInt256(12)) FROM system.one) FORMAT Null;

-- INTERSECT variant
(SELECT *, toNullable(toUInt256(12)) FROM system.one) INTERSECT (SELECT *, toNullable(toUInt256(12)) FROM system.one) FORMAT Null;

-- Variant with more Nullable types
(SELECT toNullable(1), toNullable('abc'), toNullable(toUInt256(42))) EXCEPT (SELECT toNullable(2), toNullable('def'), toNullable(toUInt256(43))) FORMAT Null;

-- EXCEPT with Nullable and non-Nullable mix
(SELECT number, toNullable(number) FROM numbers(100)) EXCEPT (SELECT number, toNullable(number) FROM numbers(50)) FORMAT Null;
