#!/usr/bin/env python3
"""
Rewrite SQLStorm PostgreSQL-dialect queries to ClickHouse-compatible SQL.
"""

import os
import re
import sys


def find_balanced_parens(s, start):
    """Find the matching closing paren for the opening paren at position start.
    Returns the index of the closing paren, or -1 if not found."""
    if start >= len(s) or s[start] != '(':
        return -1
    depth = 0
    i = start
    while i < len(s):
        if s[i] == '(':
            depth += 1
        elif s[i] == ')':
            depth -= 1
            if depth == 0:
                return i
        elif s[i] == "'" :
            # Skip string literals
            i += 1
            while i < len(s) and s[i] != "'":
                if s[i] == '\\':
                    i += 1
                i += 1
        i += 1
    return -1


def rewrite_function_call(sql, func_name, rewriter):
    """Find and rewrite all calls to func_name(args) using the rewriter function.
    rewriter(args_string) -> replacement_string"""
    result = []
    i = 0
    pat = re.compile(re.escape(func_name) + r'\s*\(', re.IGNORECASE)
    while i < len(sql):
        m = pat.search(sql, i)
        if not m:
            result.append(sql[i:])
            break
        # Check it's not part of a larger identifier
        if m.start() > 0 and (sql[m.start()-1].isalnum() or sql[m.start()-1] == '_'):
            result.append(sql[i:m.end()])
            i = m.end()
            continue
        result.append(sql[i:m.start()])
        paren_start = m.end() - 1  # position of '('
        paren_end = find_balanced_parens(sql, paren_start)
        if paren_end == -1:
            # Unbalanced, leave as-is
            result.append(sql[m.start():])
            break
        args = sql[paren_start+1:paren_end]
        replacement = rewriter(args)
        if replacement is not None:
            result.append(replacement)
        else:
            # rewriter declined, keep original
            result.append(sql[m.start():paren_end+1])
        i = paren_end + 1
    return ''.join(result)


def split_top_level_args(args):
    """Split arguments at top-level commas (respecting parens and strings)."""
    parts = []
    depth = 0
    current = []
    i = 0
    while i < len(args):
        c = args[i]
        if c == '(' :
            depth += 1
            current.append(c)
        elif c == ')':
            depth -= 1
            current.append(c)
        elif c == ',' and depth == 0:
            parts.append(''.join(current).strip())
            current = []
        elif c == "'":
            current.append(c)
            i += 1
            while i < len(args) and args[i] != "'":
                current.append(args[i])
                i += 1
            if i < len(args):
                current.append(args[i])
        else:
            current.append(c)
        i += 1
    parts.append(''.join(current).strip())
    return parts


def rewrite_string_agg(args):
    """STRING_AGG(expr, sep) -> arrayStringConcat(groupArray(assumeNotNull(expr)), sep)
       STRING_AGG(DISTINCT expr, sep) -> arrayStringConcat(arrayDistinct(groupArray(assumeNotNull(expr))), sep)"""
    parts = split_top_level_args(args)
    if len(parts) != 2:
        return None
    expr = parts[0].strip()
    sep = parts[1].strip()
    distinct = False
    if expr.upper().startswith('DISTINCT '):
        distinct = True
        expr = expr[9:].strip()
    if distinct:
        return f"arrayStringConcat(arrayDistinct(groupArray(assumeNotNull({expr}))), {sep})"
    else:
        return f"arrayStringConcat(groupArray(assumeNotNull({expr})), {sep})"


def rewrite_array_agg(args):
    """ARRAY_AGG(expr) -> groupArray(assumeNotNull(expr))
       ARRAY_AGG(DISTINCT expr) -> arrayDistinct(groupArray(assumeNotNull(expr)))"""
    expr = args.strip()
    if expr.upper().startswith('DISTINCT '):
        expr = expr[9:].strip()
        return f"arrayDistinct(groupArray(assumeNotNull({expr})))"
    return f"groupArray(assumeNotNull({expr}))"


def rewrite_string_to_array(args):
    """string_to_array(str, sep) -> splitByString(sep, str)  (args swapped)"""
    parts = split_top_level_args(args)
    if len(parts) != 2:
        return None
    return f"splitByString({parts[1]}, {parts[0]})"


def rewrite_date_part(args):
    """DATE_PART('unit', expr) -> datePart('unit', expr) -- ClickHouse has this"""
    return f"datePart({args})"


def rewrite_regexp_split_to_array(args):
    """regexp_split_to_array(str, pattern) -> splitByRegexp(pattern, str)"""
    parts = split_top_level_args(args)
    if len(parts) != 2:
        return None
    return f"splitByRegexp({parts[1]}, {parts[0]})"


def rewrite_stddev(args):
    """STDDEV(expr) -> stddevPop(expr)"""
    return f"stddevPop({args})"


def rewrite_random(args):
    """RANDOM() -> rand()"""
    return f"rand({args})"


def rewrite_age(args):
    """age(timestamp) -> dateDiff('year', timestamp, now())
       age(t1, t2) -> dateDiff('second', t2, t1)"""
    parts = split_top_level_args(args)
    if len(parts) == 1:
        return f"dateDiff('year', {parts[0]}, now())"
    elif len(parts) == 2:
        return f"dateDiff('second', {parts[1]}, {parts[0]})"
    return None


def rewrite_functions(sql):
    """Rewrite PostgreSQL function calls to ClickHouse equivalents."""
    # STRING_AGG and STRING_AGGDistinct
    sql = rewrite_function_call(sql, 'STRING_AGG', rewrite_string_agg)
    sql = rewrite_function_call(sql, 'string_agg', rewrite_string_agg)
    # STRING_AGGDistinct is a weird artifact — it's STRING_AGG(DISTINCT ...)
    # that got mangled. Rewrite as groupArray variant.
    def rewrite_string_agg_distinct(args):
        parts = split_top_level_args(args)
        if len(parts) != 2:
            return None
        return f"arrayStringConcat(arrayDistinct(groupArray(assumeNotNull({parts[0]}))), {parts[1]})"
    sql = rewrite_function_call(sql, 'STRING_AGGDistinct', rewrite_string_agg_distinct)
    sql = rewrite_function_call(sql, 'string_aggDistinct', rewrite_string_agg_distinct)
    sql = rewrite_function_call(sql, 'STRING_AGGIf', lambda args: rewrite_string_agg(args))

    # ARRAY_AGG
    sql = rewrite_function_call(sql, 'ARRAY_AGG', rewrite_array_agg)
    sql = rewrite_function_call(sql, 'array_agg', rewrite_array_agg)

    # string_to_array / STRING_TO_ARRAY
    sql = rewrite_function_call(sql, 'string_to_array', rewrite_string_to_array)
    sql = rewrite_function_call(sql, 'STRING_TO_ARRAY', rewrite_string_to_array)

    # DATE_PART
    sql = rewrite_function_call(sql, 'DATE_PART', rewrite_date_part)

    # regexp_split_to_array / REGEXP_SPLIT_TO_ARRAY
    sql = rewrite_function_call(sql, 'regexp_split_to_array', rewrite_regexp_split_to_array)
    sql = rewrite_function_call(sql, 'REGEXP_SPLIT_TO_ARRAY', rewrite_regexp_split_to_array)
    sql = rewrite_function_call(sql, 'REGEXP_SPLIT_TO_TABLE', rewrite_regexp_split_to_array)

    # STDDEV -> stddevPop
    sql = rewrite_function_call(sql, 'STDDEV', rewrite_stddev)

    # RANDOM -> rand
    sql = rewrite_function_call(sql, 'RANDOM', rewrite_random)

    # TO_TIMESTAMP -> toDateTime64
    sql = rewrite_function_call(sql, 'TO_TIMESTAMP', lambda args: f"toDateTime64({args}, 6)")

    # ARRAY_LENGTH -> length
    sql = rewrite_function_call(sql, 'ARRAY_LENGTH', lambda args: f"length({args})")

    # REGEXP_SUBSTR -> regexpExtract
    sql = rewrite_function_call(sql, 'REGEXP_SUBSTR', lambda args: f"regexpExtract({args})")

    # TRANSLATE -> replaceAll (best-effort, only works for single-char replacements)
    sql = rewrite_function_call(sql, 'TRANSLATE', lambda args: f"replaceAll({args})")

    return sql


def rewrite_extract_epoch(sql):
    """EXTRACT(EPOCH FROM expr) -> toUnixTimestamp(expr)"""
    pat = re.compile(r'\bEXTRACT\s*\(\s*EPOCH\s+FROM\s+', re.IGNORECASE)
    while True:
        m = pat.search(sql)
        if not m:
            break
        # Find the matching closing paren for this EXTRACT(
        paren_start = sql.index('(', m.start())
        paren_end = find_balanced_parens(sql, paren_start)
        if paren_end == -1:
            break
        # The expr is between "FROM " and the closing paren
        from_pos = m.end()
        expr = sql[from_pos:paren_end]
        sql = sql[:m.start()] + f'toUnixTimestamp({expr})' + sql[paren_end+1:]
    return sql


def rewrite_extract_unit(sql):
    """EXTRACT(YEAR FROM expr) -> toYear(expr), etc."""
    unit_map = {
        'YEAR': 'toYear',
        'MONTH': 'toMonth',
        'DAY': 'toDayOfMonth',
        'HOUR': 'toHour',
        'MINUTE': 'toMinute',
        'SECOND': 'toSecond',
        'DOW': 'toDayOfWeek',
        'DOY': 'toDayOfYear',
        'WEEK': 'toWeek',
        'QUARTER': 'toQuarter',
    }
    for unit, func in unit_map.items():
        pat = re.compile(
            r'\bEXTRACT\s*\(\s*' + unit + r'\s+FROM\s+',
            re.IGNORECASE
        )
        while True:
            m = pat.search(sql)
            if not m:
                break
            paren_start = sql.index('(', m.start())
            paren_end = find_balanced_parens(sql, paren_start)
            if paren_end == -1:
                break
            from_pos = m.end()
            expr = sql[from_pos:paren_end]
            sql = sql[:m.start()] + f'{func}({expr})' + sql[paren_end+1:]
    return sql


def rewrite_fetch_offset(sql):
    """
    OFFSET N ROWS FETCH NEXT M ROWS ONLY -> LIMIT M OFFSET N
    FETCH FIRST N ROWS ONLY -> LIMIT N
    FETCH NEXT N ROWS ONLY -> LIMIT N
    OFFSET N ROWS -> OFFSET N  (when no FETCH follows)
    """
    # OFFSET N ROWS FETCH NEXT M ROWS ONLY
    sql = re.sub(
        r'\bOFFSET\s+(\S+)\s+ROWS?\s+FETCH\s+(?:NEXT|FIRST)\s+(\S+)\s+ROWS?\s+ONLY\b',
        r'LIMIT \2 OFFSET \1',
        sql,
        flags=re.IGNORECASE,
    )
    # FETCH FIRST/NEXT N ROWS ONLY (without preceding OFFSET)
    sql = re.sub(
        r'\bFETCH\s+(?:FIRST|NEXT)\s+(\S+)\s+ROWS?\s+ONLY\b',
        r'LIMIT \1',
        sql,
        flags=re.IGNORECASE,
    )
    # Standalone OFFSET N ROWS (remove the ROWS keyword)
    sql = re.sub(
        r'\bOFFSET\s+(\d+)\s+ROWS?\b',
        r'OFFSET \1',
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def rewrite_interval(sql):
    """INTERVAL 'N unit' -> INTERVAL N unit
    e.g. INTERVAL '30 days' -> INTERVAL 30 DAY
         INTERVAL '1 year' -> INTERVAL 1 YEAR"""
    def replace_interval(m):
        val = m.group(1)
        # Parse "N unit" from the string
        parts = val.strip().split()
        if len(parts) == 2:
            num, unit = parts
            # Singularize the unit
            unit = unit.upper().rstrip('S')
            if unit in ('YEAR', 'MONTH', 'WEEK', 'DAY', 'HOUR', 'MINUTE', 'SECOND'):
                return f'INTERVAL {num} {unit}'
        return m.group(0)  # no change

    sql = re.sub(
        r"\bINTERVAL\s+'([^']+)'",
        replace_interval,
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def rewrite_cast_timestamp(sql):
    """CAST('...' AS TIMESTAMP) -> toDateTime64('...', 6)
    TIMESTAMP '...' -> toDateTime64('...', 6)
    Also: CAST(expr AS TIMESTAMP) -> toDateTime64(expr, 6)"""
    # CAST(expr AS TIMESTAMP)
    sql = re.sub(
        r"\bCAST\s*\(([^)]+?)\s+AS\s+TIMESTAMP\s*\)",
        r"toDateTime64(\1, 6)",
        sql,
        flags=re.IGNORECASE,
    )
    # Standalone TIMESTAMP 'literal' (not preceded by AS or CAST)
    sql = re.sub(
        r"(?<!\bAS\s)(?<!\bCAST\s)\bTIMESTAMP\s+'([^']+)'",
        r"toDateTime64('\1', 6)",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def rewrite_current_timestamp(sql):
    """CURRENT_TIMESTAMP -> now64(6)"""
    sql = re.sub(r'\bCURRENT_TIMESTAMP\b', 'now64(6)', sql, flags=re.IGNORECASE)
    return sql


def rewrite_unnest_lateral(sql):
    """
    Rewrite UNNEST/LATERAL JOIN patterns to ClickHouse ARRAY JOIN.
    Also replaces standalone unnest(expr) calls with arrayJoin(expr).
    """
    # Remove LATERAL keyword (not supported in ClickHouse)
    sql = re.sub(r'\bLATERAL\s+', '', sql, flags=re.IGNORECASE)

    # Pattern: CROSS JOIN UNNEST(expr) AS alias(col) ON TRUE
    # -> ARRAY JOIN expr AS col
    sql = re.sub(
        r'\bCROSS\s+JOIN\s+[Uu][Nn][Nn][Ee][Ss][Tt]\(([^)]+)\)\s+AS\s+\w+\((\w+)\)\s*(?:ON\s+TRUE)?',
        r'ARRAY JOIN \1 AS \2',
        sql,
        flags=re.IGNORECASE,
    )

    # Pattern: LEFT JOIN UNNEST(expr) AS alias(col) ON TRUE
    # -> LEFT ARRAY JOIN expr AS col
    sql = re.sub(
        r'\bLEFT\s+JOIN\s+[Uu][Nn][Nn][Ee][Ss][Tt]\(([^)]+)\)\s+AS\s+\w+\((\w+)\)\s*(?:ON\s+TRUE)?',
        r'LEFT ARRAY JOIN \1 AS \2',
        sql,
        flags=re.IGNORECASE,
    )

    # Pattern: CROSS JOIN UNNEST(expr) AS col ON TRUE (no parens around col)
    sql = re.sub(
        r'\bCROSS\s+JOIN\s+[Uu][Nn][Nn][Ee][Ss][Tt]\(([^)]+)\)\s+AS\s+(\w+)\s*(?:ON\s+TRUE)?',
        r'ARRAY JOIN \1 AS \2',
        sql,
        flags=re.IGNORECASE,
    )

    # Pattern: LEFT JOIN UNNEST(expr) AS col ON TRUE
    sql = re.sub(
        r'\bLEFT\s+JOIN\s+[Uu][Nn][Nn][Ee][Ss][Tt]\(([^)]+)\)\s+AS\s+(\w+)\s*(?:ON\s+TRUE)?',
        r'LEFT ARRAY JOIN \1 AS \2',
        sql,
        flags=re.IGNORECASE,
    )

    # Pattern: , UNNEST(expr) AS col (in FROM clause, comma-joined)
    sql = re.sub(
        r',\s*[Uu][Nn][Nn][Ee][Ss][Tt]\(([^)]+)\)\s+AS\s+(\w+)\s*(?:ON\s+TRUE)?',
        r' ARRAY JOIN \1 AS \2',
        sql,
        flags=re.IGNORECASE,
    )

    # Pattern: JOIN (SELECT unnest(expr) AS col) alias ON TRUE
    # -> ARRAY JOIN expr AS col
    sql = re.sub(
        r'\b(?:CROSS\s+)?JOIN\s+\(\s*SELECT\s+[Uu][Nn][Nn][Ee][Ss][Tt]\(([^)]+)\)\s+AS\s+(\w+)\s*\)\s*\w*\s*ON\s+TRUE',
        r'ARRAY JOIN \1 AS \2',
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r'\bLEFT\s+JOIN\s+\(\s*SELECT\s+[Uu][Nn][Nn][Ee][Ss][Tt]\(([^)]+)\)\s+AS\s+(\w+)\s*\)\s*\w*\s*ON\s+TRUE',
        r'LEFT ARRAY JOIN \1 AS \2',
        sql,
        flags=re.IGNORECASE,
    )

    # Remaining standalone unnest(expr) calls -> arrayJoin(expr)
    sql = re.sub(r'\bunnest\s*\(', 'arrayJoin(', sql)
    sql = re.sub(r'\bUNNEST\s*\(', 'arrayJoin(', sql)

    return sql


def rewrite_pg_cast(sql):
    """Replace PostgreSQL :: cast operator.
    expr::type -> CAST(expr AS type)
    Common cases: ::int, ::text, ::varchar, ::float, ::numeric, ::bigint, ::date, ::timestamp
    Handles dotted identifiers like ph.Comment::int -> CAST(ph.Comment AS int)
    """
    # Match dotted identifiers (a.b), simple identifiers, string literals, or )
    pat = re.compile(r"(\w+\.\w+|\w+|'[^']*'|\))\s*::\s*(\w+)")
    while pat.search(sql):
        sql = pat.sub(r'CAST(\1 AS \2)', sql, count=1)
    return sql


def rewrite_bool_literals(sql):
    """PostgreSQL TRUE/FALSE are fine in ClickHouse, no change needed."""
    return sql


def rewrite_ilike(sql):
    """ILIKE is supported in ClickHouse, no change needed."""
    return sql


def rewrite_no_supertype(sql):
    """Fix signed/unsigned conflicts in COALESCE/CASE with COUNT.
    COALESCE(COUNT(...), -1) -> use 0 instead of -1 won't work.
    Actually: wrap in toInt64. This is rare (21 cases), skip for now."""
    return sql


def rewrite_query(sql):
    """Apply all rewrites to a SQL query."""
    # Order matters: do function rewrites before simpler regex replacements

    # 1. Function rewrites (handle balanced parens)
    sql = rewrite_functions(sql)

    # 2. EXTRACT rewrites
    sql = rewrite_extract_epoch(sql)
    sql = rewrite_extract_unit(sql)

    # 3. Syntax pattern rewrites
    sql = rewrite_fetch_offset(sql)
    sql = rewrite_interval(sql)
    sql = rewrite_cast_timestamp(sql)
    sql = rewrite_current_timestamp(sql)
    sql = rewrite_unnest_lateral(sql)
    sql = rewrite_pg_cast(sql)

    # 4. DATE keyword standalone: DATE '2024-01-01' -> toDate('2024-01-01')
    sql = re.sub(
        r"\bDATE\s+'([^']+)'",
        r"toDate('\1')",
        sql,
        flags=re.IGNORECASE,
    )

    # 5. Fix broken alias.CAST( pattern from previous :: rewrite
    # e.g. ph.CAST(Comment AS int) -> CAST(ph.Comment AS int)
    sql = re.sub(
        r'\b(\w+)\.CAST\((\w+)',
        r'CAST(\1.\2',
        sql,
    )

    # 6. Fix broken numberCAST patterns like 0CAST, 2CAST
    # e.g. 0CAST(... -> 0, CAST(... -- but this is usually FALSECAST or 0CAST
    # These are from badly placed :: casts. Best effort: just fix FALSECAST
    sql = re.sub(r'\bFALSECAST\b', 'FALSE, CAST', sql)
    sql = re.sub(r'\b(\d+)CAST\(', r'\1, CAST(', sql)

    return sql


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <query_dir> [--dry-run] [--stats]")
        sys.exit(1)

    query_dir = sys.argv[1]
    dry_run = '--dry-run' in sys.argv
    show_stats = '--stats' in sys.argv

    files = sorted(f for f in os.listdir(query_dir) if f.endswith('.sql'))

    changed = 0
    unchanged = 0
    errors = 0

    for fname in files:
        path = os.path.join(query_dir, fname)
        try:
            with open(path, 'r') as f:
                original = f.read()

            rewritten = rewrite_query(original)

            if rewritten != original:
                changed += 1
                if not dry_run:
                    with open(path, 'w') as f:
                        f.write(rewritten)
                if show_stats and changed <= 5:
                    print(f"--- {fname} ---")
                    # Show a compact diff
                    orig_lines = original.splitlines()
                    new_lines = rewritten.splitlines()
                    for i, (o, n) in enumerate(zip(orig_lines, new_lines)):
                        if o != n:
                            print(f"  L{i+1} - {o.strip()[:100]}")
                            print(f"  L{i+1} + {n.strip()[:100]}")
            else:
                unchanged += 1
        except Exception as e:
            errors += 1
            print(f"ERROR processing {fname}: {e}", file=sys.stderr)

    print(f"\nResults: {changed} changed, {unchanged} unchanged, {errors} errors")
    print(f"Total: {changed + unchanged + errors} files")


if __name__ == '__main__':
    main()
