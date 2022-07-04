# SQL Type System

## Overview
The type system defines how objects of different types interact with each other in the Hazelcast Mustang engine.
The type system is defined by the list of supported types, type mapping, and type conversion rules.

A type is defined by name, precedence, precision, and scale.
- **Name** is a textual representation of type name
- **Precision** is the total count of significant digits in the whole number, applicable to **numeric types**
- **Precedence** is a comparable value that is used for type inference in expressions. A type with a
higher value has precedence over a type with a lower value. If two types have the same precedence value, then
the type with higher precision has precedence.

**Type family** is a collection of types with the same name, but different precisions. All types
within a family have the same name and precedence. For example, `INTEGER(11)` and `INTEGER(12)` are two types
from the same `INTEGER` family.

The scale is not used in Hazelcast Mustang. It is applicable only for `DECIMAL`, `REAL`, and `DOUBLE` types.
Instead of defining it as a separate value, we just treat these types as types with infinite scale.

## 1 Supported Types
Types supported by the Hazelcast Mustang are listed in Table 1. Precision is the smallest precision in the type family.

`OBJECT` is a Hazelcast-specific type representing an object which doesn't match any other type.

`NULL` is a special type representing a type of a `NULL` literal to which a more
specific type couldn't be assigned. Consider `SELECT NULL FROM t` query, the `NULL`
literal would have `NULL` type assigned. 

*Table 1: Hazelcast Mustang Data Types*

| SQL Type | Precedence | Precision |
|---|---|---|
| `NULL` | 0 |  |
| `VARCHAR` | 100 |  |
| `BOOLEAN` | 200 | 1 |
| `TINYINT` | 300 | 4 |
| `SMALLINT` | 400 | 7 |
| `INTEGER` | 500 | 11 |
| `BIGINT` | 600 | 20 |
| `DECIMAL` | 700 | Unlimited |
| `REAL` | 800 | Unlimited |
| `DOUBLE` | 900 | Unlimited |
| `TIME` | 1000 |  |
| `DATE` | 1100 |  |
| `TIMESTAMP` | 1200 |  |
| `TIMESTAMP WITH TIME ZONE` | 1300 |  |
| `OBJECT` | 1400 |  |

The type `TIME WITH TIME ZONE` is not supported because of its confusing behavior: daylight-saving rules make it hard to reason
about time with offset without date part. For this reason, this type is of little use for real applications. The support for this
type might be added in future releases if we find useful use cases for it.

Structured and UDF data types are not supported at the moment. The support for these types will be added in future releases.

## 2 Type Mapping
Hazelcast is implemented in Java. It is necessary to map SQL types to Java types. We define two mapping tables:
1. SQL-to-Java mapping: defines the Java class of the value returned by a query depending on the SQL type
1. Java-to-SQL mapping: defines how user values stored in Hazelcast data structures are mapped to relevant SQL types

Every input value is mapped to an SQL type first. If there is no appropriate Java-to-SQL mapping, then the value is
interpreted as the `OBJECT` type. Then the value is converted to a Java type mapped to the SQL type. Internally the
engine operates only on Java types defined in SQL-to-Java mapping, which simplifies the implementation significantly.

For example, an input value of the type `java.math.BigInteger` is mapped to `DECIMAL` SQL type. But since `DECIMAL`
type is mapped to `java.math.BigDecimal`, the input value is converted from `java.math.BigInteger` to `java.math.BigDecimal`
on first access.

### 2.1 SQL to Java Mapping
Table 2 establishes a strict one-to-one mapping between SQL and Java types.

*Table 2: SQL-to-Java mapping*

| SQL Type | Java Type |
|---|---|
| `NULL` | `java.lang.Void` |
| `VARCHAR` | `java.lang.String` |
| `BOOLEAN` | `java.lang.Boolean` |
| `TINYINT` | `java.lang.Byte` |
| `SMALLINT` | `java.lang.Short` |
| `INTEGER` | `java.lang.Integer` |
| `BIGINT` | `java.lang.Long` |
| `DECIMAL` | `java.math.BigDecimal` |
| `REAL` | `java.lang.Float` |
| `DOUBLE` | `java.lang.Double` |
| `DATE` | `java.time.LocalDate` |
| `TIME` | `java.time.LocalTime` |
| `TIMESTAMP` | `java.time.LocalDateTime` |
| `TIMESTAMP W/ TZ` | `java.time.OffsetDateTime` |
| `OBJECT` | `java.lang.Object` |

`TIMESTAMP WITH TIME ZONE` is mapped to the `java.time.OffsetDateTime` class because ANSI SQL requires only zone
displacement, so full zone information from the `java.time.ZonedDateTime` class is not needed.

### 2.2 Java to SQL Mapping
Table 3 establishes a many-to-one mapping between Java and SQL types.

*Table 3: Java-to-SQL mapping*

| Java Type | SQL Type |
|---|---|
| `java.lang.Void` | `NULL` |
| `java.lang.String` | `VARCHAR` |
| `java.lang.Character` | `VARCHAR` |
| `java.lang.Boolean` | `BOOLEAN` |
| `java.lang.Byte` | `TINYINT` |
| `java.lang.Short` | `SMALLINT` |
| `java.lang.Integer` | `INTEGER` |
| `java.lang.Long` | `BIGINT` |
| `java.math.BigInteger` | `DECIMAL`  |
| `java.math.BigDecimal` | `DECIMAL`  |
| `java.lang.Float` | `REAL` |
| `java.lang.Double` | `DOUBLE` |
| `java.time.LocalDate` | `DATE` |
| `java.time.LocalTime` | `TIME` |
| `java.time.LocalDateTime` | `TIMESTAMP` |
| `java.util.Calendar` | `TIMESTAMP W/ TZ` |
| `java.util.Date` | `TIMESTAMP W/ TZ` |
| `java.time.Instant` | `TIMESTAMP W/ TZ` |
| `java.time.OffsetDateTime` | `TIMESTAMP W/ TZ` |
| `java.time.ZonedDateTime` | `TIMESTAMP W/ TZ` |
| Any other type | `OBJECT` |

The following SQL types are mapped to several Java types:
- `VARCHAR` is mapped to `java.lang.String` and `java.lang.Character`
- `DECIMAL` is mapped to `java.math.BigInteger` and `java.math.BigDecimal`
- `TIMESTAMP W/ TZ` is mapped to multiple date/time classes which represent a time instant.

## 3. Type Conversions
Different types might be converted to each other. The table provides the list of type conversions.

*Table 4: Type conversions (I - implicit, E - explicit)*

| From/To | NULL | VARCHAR | BOOLEAN | TINYINT | SMALLINT | INTEGER | BIGINT | DECIMAL | REAL | DOUBLE | DATE | TIME | TIMESTAMP | TIMESTAMP W/ TZ | OBJECT |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **NULL** | `-` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` |
| **VARCHAR** |  | `-` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` |
| **BOOLEAN** |  | `Y` | `-` |  |  |  |  |  |  |  |  |  |  |  | `Y` |
| **TINYINT** |  | `Y` |  | `-` | `Y` | `Y` | `Y` | `Y` | `Y` |`Y`  |  |  |  |  | `Y` |
| **SMALLINT** |  | `Y` |  | `Y` | `-` | `Y` | `Y` | `Y` | `Y` | `Y` |  |  |  |  | `Y` |
| **INTEGER** |  | `Y` |  | `Y` | `Y` | `-` | `Y` | `Y` | `Y` | `Y` |  |  |  |  | `Y` |
| **BIGINT** |  | `Y` |  | `Y` | `Y` | `Y` | `-` | `Y` | `Y` | `Y` |  |  |  |  | `Y` |
| **DECIMAL** |  | `Y` |  | `Y` | `Y` | `Y` | `Y` | `-` | `Y` | `Y` |  |  |  |  | `Y` |
| **REAL** |  | `Y` |  | `Y` | `Y` | `Y` | `Y` | `Y` | `-` | `Y` |  |  |  |  | `Y` |
| **DOUBLE** |  | `Y` |  | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `-` |  |  |  |  | `Y` |
| **DATE** |  | `Y` |  |  |  |  |  |  |  |  | `-` |  | `Y` | `Y` | `Y` |
| **TIME** |  | `Y` |  |  |  |  |  |  |  |  |  | `-` | `Y` | `Y` | `Y` |
| **TIMESTAMP** |  | `Y` |  |  |  |  |  |  |  |  | `Y` | `Y` | `-` | `Y` | `Y` |
| **TIMESTAMP W/ TZ** |  | `Y` |  |  |  |  |  |  |  |  | `Y` | `Y` | `Y`  | `-` | `Y` |
| **OBJECT** |  | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `Y` | `-` |

Conversions between VARCHAR and temporal types are performed using patterns defined in `java.time.format.DateTimeFormatter`
class [[1]].

*Table 5: Temporal type conversion patterns*

| Type | Pattern |
|---|---|
| `TIME` | `ISO_LOCAL_TIME` |
| `DATE` | `ISO_LOCAL_DATE` |
| `TIMESTAMP` | `ISO_LOCAL_DATE_TIME` |
| `TIMESTAMP W/ TZ` | `ISO_OFFSET_DATE_TIME` |

[1]: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/format/DateTimeFormatter.html "java.time.format.DateTimeFormatter JavaDoc"
