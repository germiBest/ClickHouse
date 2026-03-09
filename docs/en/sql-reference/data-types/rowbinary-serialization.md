---
description: 'Describes how ClickHouse data types are serialized in RowBinary format, with annotated examples for Dynamic, JSON, and other complex types'
sidebar_label: 'RowBinary serialization'
sidebar_position: 57
slug: /sql-reference/data-types/rowbinary-serialization
title: 'RowBinary serialization'
keywords: ['rowbinary', 'binary', 'serialization', 'wire format', 'dynamic', 'json']
doc_type: 'reference'
---

# RowBinary serialization

This page describes how ClickHouse data types are serialized in [RowBinary](/interfaces/formats/RowBinary) format, focusing on the `Dynamic` and `JSON` types.

For binary type identifiers (the single-byte index used by `Dynamic`), see the [Data types binary encoding specification](/sql-reference/data-types/data-types-binary-encoding).

## Foundational concepts {#foundational-concepts}

### Unsigned LEB128 (VarUInt) {#unsigned-leb128}

An unsigned variable-width integer encoding used as a length prefix for `String`, `Array`, `Map`, and other variable-length types. See [LEB128](https://en.wikipedia.org/wiki/LEB128#Decode_unsigned_integer) for a reference implementation.

### Byte order {#byte-order}

All fixed-width numeric types (`(U)Int8` through `(U)Int256`, `Float32`, `Float64`, `Decimal*`) are encoded as **little-endian**.

## Simple types {#simple-types}

Summary of RowBinary encodings for simple types:

| Type | Wire format |
|---|---|
| `(U)Int8` .. `(U)Int256` | Little-endian, fixed width (1–32 bytes) |
| `Bool` | Single byte: `0x00` = false, `0x01` = true |
| `Float32`, `Float64` | Little-endian IEEE 754 (4 or 8 bytes) |
| `String` | VarUInt length + raw bytes |
| `FixedString(N)` | Exactly N bytes, zero-padded if shorter |
| `Date` | `UInt16` — days since `1970-01-01` |
| `Date32` | `Int32` — days before or after `1970-01-01` |
| `DateTime` | `UInt32` — seconds since `1970-01-01 00:00:00 UTC` |
| `DateTime64(P)` | `Int64` — ticks since epoch (scale = `10^P`) |
| `Enum8` / `Enum16` | `UInt8` / `UInt16` — the enum index value |
| `UUID` | 16 bytes (see [UUID encoding](#uuid-encoding)) |
| `IPv4` | 4 bytes as `UInt32` |
| `IPv6` | 16 bytes (not equivalent to `UInt128`) |
| `Decimal32` / `64` / `128` / `256` | Little-endian `Int32` / `64` / `128` / `256` (scaled integer) |

### UUID encoding {#uuid-encoding}

UUIDs are stored as 16 bytes. The byte order does not match the standard string representation. For example, `61f0c404-5cb3-11e7-907b-a6006ad3dba0` is encoded as:

```text
e7 11 b3 5c 04 c4 f0 61
a0 db d3 6a 00 a6 7b 90
```

The default UUID `00000000-0000-0000-0000-000000000000` is 16 zero bytes.

## Nullable {#nullable}

Encoded as:

1. A single byte: `0x00` (not NULL) or `0x01` (NULL).
2. If not NULL, the value of `T` in its standard encoding.

```text
00                      -- not NULL — value follows
2a 00 00 00             -- UInt32: 42

01                      -- NULL — nothing follows
```

## Array {#array}

1. VarUInt element count.
2. Each element in its standard encoding.

```text
03                      -- 3 elements
01 00 00 00             -- UInt32: 1
02 00 00 00             -- UInt32: 2
03 00 00 00             -- UInt32: 3
```

`Array(Nullable(T))` is valid, but `Nullable(Array(T))` is not.

## Tuple {#tuple}

Elements encoded consecutively with no length prefix or delimiters. The schema defines element count and types.

```text
2a 00 00 00             -- UInt32: 42
03                      -- VarUInt: string length 3
66 6f 6f                -- "foo"
02                      -- VarUInt: array has 2 elements
63                      -- UInt8: 99
90                      -- UInt8: 144
```

Encodes `Tuple(UInt32, String, Array(UInt8))` with value `(42, 'foo', [99, 144])`.

## Map {#map}

Encoded identically to `Array(Tuple(K, V))`:

1. VarUInt element count.
2. Each key-value pair encoded consecutively.

```text
02                      -- 2 entries
03 66 6f 6f             -- String key: "foo"
01 00 00 00             -- UInt32 value: 1
03 62 61 72             -- String key: "bar"
02 00 00 00             -- UInt32 value: 2
```

## LowCardinality {#lowcardinality}

`LowCardinality` does not affect the RowBinary wire format. `LowCardinality(String)` is encoded identically to `String`.

:::note
`LowCardinality(Nullable(T))` is valid, but `Nullable(LowCardinality(T))` will cause a server error.
:::

## Variant {#variant}

Each value is encoded as:

1. A single byte **discriminant** — the index of the active type.
2. The value in that type's standard encoding.

:::warning
Types in a `Variant` definition are always **sorted alphabetically** in the wire format, regardless of the order in `CREATE TABLE`. The discriminant corresponds to this sorted order.
:::

```text
01                      -- discriminant 1 → Bool (alphabetically sorted)
01                      -- true

0b                      -- discriminant 11 → String
06 66 6f 6f 62 61 72    -- "foobar"
```

## Dynamic {#dynamic}

[`Dynamic`](/sql-reference/data-types/dynamic) values are self-describing. Each is encoded as:

1. A **binary type descriptor** — a `BinaryTypeIndex` byte identifying the type, plus any type-specific parameters (e.g., precision and timezone for `DateTime64`). See the [binary encoding specification](/sql-reference/data-types/data-types-binary-encoding) for the full type index table.
2. The **value** in its standard RowBinary encoding.

```text
[BinaryTypeIndex][type-specific parameters...][value]
```

### Examples {#dynamic-examples}

Integer:

```sql
SELECT 42::Dynamic
```

```text
0a                        -- BinaryTypeIndex: Int64 (0x0A)
2a 00 00 00 00 00 00 00   -- Int64 value: 42
```

`DateTime64` with timezone:

```sql
SELECT toDateTime64('2024-01-15 10:30:00', 3, 'America/New_York')::Dynamic
```

```text
14                        -- BinaryTypeIndex: DateTime64WithTimezone (0x14)
03                        -- UInt8: precision (3 = milliseconds)
10                        -- VarUInt: timezone name length (16 bytes)
41 6d 65 72 69 63 61 2f   -- "America/"
4e 65 77 5f 59 6f 72 6b   -- "New_York"
c0 6c be 0d 8d 01 00 00   -- Int64: timestamp value
```

The parser uses the `BinaryTypeIndex` to select the correct deserializer, then reuses standard RowBinary parsing for the identified type.

## JSON {#json}

Each [`JSON`](/sql-reference/data-types/newjson) row is serialized as a flat list of paths and values. Nested objects are flattened into dot-separated paths.

### Wire format {#json-wire-format}

```text
[VarUInt: number_of_paths]
[String: path_1][value_1]
[String: path_2][value_2]
...
```

Paths are always serialized in **alphabetical order**.

### Typed paths vs dynamic paths {#json-typed-vs-dynamic-paths}

Encoding rules differ for **typed paths** (declared in the schema) and **dynamic paths** (discovered at runtime):

| Path category | Included in serialization | Value encoding | Nullable allowed |
|---|---|---|---|
| **Typed paths** | Always (even if `NULL`) | Type-specific binary format | Yes |
| **Dynamic paths** | Only if non-null | [Dynamic](#dynamic) encoding (type descriptor + value) | No |

### Typed paths only {#json-typed-paths-only}

Values are encoded directly using their declared type.

Schema: `JSON(user_id UInt32, active Bool)`

Row: `{"user_id": 42, "active": true}`

```text
02                              -- VarUInt: 2 paths

-- Path "active" (typed: Bool)
06 61 63 74 69 76 65            -- String: "active" (length 6 + bytes)
01                              -- Bool value: true

-- Path "user_id" (typed: UInt32)
07 75 73 65 72 5f 69 64         -- String: "user_id" (length 7 + bytes)
2a 00 00 00                     -- UInt32 value: 42 (little-endian)
```

### Typed and dynamic paths {#json-typed-and-dynamic-paths}

Paths not declared in the schema are encoded as [`Dynamic`](#dynamic) values (type descriptor + value).

Schema: `JSON(user_id UInt32, active Bool)`

Row: `{"user_id": 42, "active": true, "name": "Alice"}`

```text
03                              -- VarUInt: 3 paths

-- Path "active" (typed: Bool)
06 61 63 74 69 76 65            -- String: "active" (length 6 + bytes)
01                              -- Bool value: true

-- Path "name" (dynamic)
04 6e 61 6d 65                  -- String: "name" (length 4 + bytes)
15                              -- BinaryTypeIndex: String (0x15)
05 41 6c 69 63 65               -- String value: "Alice" (length 5 + bytes)

-- Path "user_id" (typed: UInt32)
07 75 73 65 72 5f 69 64         -- String: "user_id" (length 7 + bytes)
2a 00 00 00                     -- UInt32 value: 42 (little-endian)
```

### NULL handling {#json-null-handling}

**Typed nullable paths** use the standard `Nullable` encoding (`0x01` flag byte):

Schema: `JSON(score Nullable(Int32))`

Row: `{"score": null}`

```text
01                              -- VarUInt: 1 path

-- Path "score" (typed: Nullable(Int32))
05 73 63 6f 72 65               -- String: "score" (length 5 + bytes)
01                              -- Nullable flag: 1 (NULL — no value follows)
```

**Typed non-nullable paths** use the type's default value (e.g., empty string for `String`):

Schema: `JSON(name String)`

Row: `{"name": null}`

```text
01                              -- VarUInt: 1 path

-- Path "name" (typed: String, non-nullable)
04 6e 61 6d 65                  -- String: "name" (length 4 + bytes)
00                              -- String length 0 (empty string — default value)
```

**Dynamic NULL paths are omitted entirely:**

Schema: `JSON(id UInt64)`

Row: `{"id": 100, "metadata": null}`

```text
01                              -- VarUInt: 1 path (dynamic NULL paths are skipped)

-- Path "id" (typed: UInt64)
02 69 64                        -- String: "id" (length 2 + bytes)
64 00 00 00 00 00 00 00         -- UInt64 value: 100 (little-endian)
```

### Nested objects {#json-nested-objects}

Nested objects are flattened into dot-separated paths. Each leaf value is encoded individually.

Schema: `JSON()`

Row: `{"user": {"name": "Bob", "age": 30}}`

```text
02                              -- VarUInt: 2 paths (nested objects are flattened)

-- Path "user.age" (dynamic)
08 75 73 65 72 2e 61 67 65      -- String: "user.age" (length 8 + bytes)
0a                              -- BinaryTypeIndex: Int64 (0x0A)
1e 00 00 00 00 00 00 00         -- Int64 value: 30 (little-endian)

-- Path "user.name" (dynamic)
09 75 73 65 72 2e 6e 61 6d 65   -- String: "user.name" (length 9 + bytes)
15                              -- BinaryTypeIndex: String (0x15)
03 42 6f 62                     -- String value: "Bob" (length 3 + bytes)
```

### JSON as String mode {#json-as-string}

JSON columns can alternatively be serialized as plain JSON text strings instead of the structured binary format:

- `output_format_binary_write_json_as_string=1` — writes JSON columns as a JSON string on output.
- `input_format_binary_read_json_as_string=1` — reads JSON columns from a JSON string on input.

Useful when the client should handle JSON parsing instead of the server.

## Geo types {#geo-types}

Geo types are aliases for `Tuple` and `Array` combinations:

| Geo type | Equivalent type |
|---|---|
| `Point` | `Tuple(Float64, Float64)` |
| `Ring` | `Array(Point)` |
| `LineString` | `Array(Point)` |
| `Polygon` | `Array(Ring)` |
| `MultiPolygon` | `Array(Polygon)` |
| `MultiLineString` | `Array(LineString)` |

Wire format is identical to the equivalent Tuple/Array encoding.

## Nested {#nested}

Serialized as a sequence of arrays, one per nested column. `Nested(a String, b Int32)` is encoded as `Array(String)` followed by `Array(Int32)`.

## See also {#see-also}

- [Data types binary encoding specification](/sql-reference/data-types/data-types-binary-encoding) — the `BinaryTypeIndex` lookup table used by `Dynamic` and `JSON`.
- [RowBinary format](/interfaces/formats/RowBinary) — format overview and settings.
- [RowBinaryWithNamesAndTypes format](/interfaces/formats/RowBinaryWithNamesAndTypes) — RowBinary with a header containing column names and types.
