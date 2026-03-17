-- Test LIKE with ESCAPE clause

-- Basic: literal % matching with custom escape character
SELECT '10%' LIKE '10|%' ESCAPE '|';
SELECT '10x' LIKE '10|%' ESCAPE '|';

-- Basic: literal _ matching with custom escape character
SELECT 'a_b' LIKE 'a|_b' ESCAPE '|';
SELECT 'axb' LIKE 'a|_b' ESCAPE '|';

-- Custom escape character: #
SELECT '50%off' LIKE '50#%off' ESCAPE '#';
SELECT '50xoff' LIKE '50#%off' ESCAPE '#';

-- Backslash as escape character (standard behavior)
SELECT '10%' LIKE '10\%' ESCAPE '\\';
SELECT '10x' LIKE '10\%' ESCAPE '\\';

-- Double escape character (literal escape char in pattern)
SELECT 'a|b' LIKE 'a||b' ESCAPE '|';
SELECT 'axb' LIKE 'a||b' ESCAPE '|';

-- Wildcards still work when not escaped
SELECT 'hello' LIKE 'h%o' ESCAPE '|';
SELECT 'hello' LIKE 'h_llo' ESCAPE '|';

-- NOT LIKE with ESCAPE
SELECT '10%' NOT LIKE '10|%' ESCAPE '|';
SELECT '10x' NOT LIKE '10|%' ESCAPE '|';

-- ILIKE with ESCAPE
SELECT '10%' ILIKE '10|%' ESCAPE '|';
SELECT 'ABC' ILIKE 'a|%' ESCAPE '|';

-- NOT ILIKE with ESCAPE
SELECT '10%' NOT ILIKE '10|%' ESCAPE '|';

-- Backslash is literal when custom escape is used
SELECT 'a\b' LIKE 'a\b' ESCAPE '|';
SELECT 'a\b' LIKE 'a|%b' ESCAPE '|';

-- Pattern with mixed escaped and unescaped wildcards
SELECT 'test%value' LIKE 'test|%%' ESCAPE '|';
SELECT 'test%' LIKE 'test|%' ESCAPE '|';

-- Error: escape sequence at end of pattern
SELECT '10%' LIKE '10|' ESCAPE '|'; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }

-- Error: invalid escape sequence
SELECT '10%' LIKE '10|x' ESCAPE '|'; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }

-- Verify round-trip formatting
SELECT formatQuery('SELECT x LIKE ''abc'' ESCAPE ''|''');
