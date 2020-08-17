/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.validate.types;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;

/**
 * Represents integer-valued types for Calcite.
 * <p>
 * Unlike the standard Calcite implementation for TINYINT, SMALLINT, INT and
 * BIGINT, this implementation tracks the actual bit width required to represent
 * integer values. That bit width information is not directly related to the
 * underlying machine representation. The closest concept is SQL precision
 * tracking done for DECIMAL type, but the bit width information tracks the
 * precision in terms of binary (base-2) digits instead of decimal (base-10)
 * digits. For instance, -1 and 1 require a single binary digit, 14 requires 4
 * binary digits.
 * <p>
 * There are 4 edge case values: {@link Byte#MIN_VALUE}, {@link Short#MIN_VALUE},
 * {@link Integer#MIN_VALUE} and {@link Long#MIN_VALUE} which, due to their
 * hardware representation, require one less bit comparing to their positive
 * counterparts. For instance, -128 ({@link Byte#MIN_VALUE}) requires 7 bits
 * while 128 requires 8.
 * <p>
 * In general, for an N-bit integer type the valid range of bit widths is from 0
 * to N: zero bit width corresponds to 0 integer value, bit widths from 1 to
 * N - 1 correspond to regular integer values, bit width of N bits has a special
 * meaning and indicates a possibility of an overflow.
 * <p>
 * For instance, for BIGINT type represented as Java {@code long} type:
 * the valid bit width range is from 0 to 64, 0L has BIGINT(0) type, -14L has
 * BIGINT(4) type, a BIGINT SQL column has BIGINT(63) type, BIGINT(64) indicates
 * a potential overflow.
 * <p>
 * Each arithmetic operation acting on integer types infers its return type
 * based on the bit width information: INT(4) + INT(10) -> INT(11),
 * INT(4) + INT(31) -> BIGINT(32), INT(10) + BIGINT(63) -> BIGINT(64). In the
 * first example the bit width was expanded; in the second example the bit width
 * was expanded and the type was promoted to the next wider integer type; in the
 * last example the bit width was expanded, but there was no wider integer type
 * to promote to and avoid a possible overflow.
 * <p>
 * The benefits of that bit width approach are: the smallest possible type is
 * always selected to represent a result of a certain operation and it's always
 * possible to tell from the selected type alone whether overflow checking is
 * necessary while executing the operation.
 */
public final class HazelcastIntegerType extends BasicSqlType {

    private static final Map<SqlTypeName, HazelcastIntegerType[]> TYPES = new HashMap<>();
    private static final Map<SqlTypeName, HazelcastIntegerType[]> NULLABLE_TYPES = new HashMap<>();

    static {
        // Preallocate all possible types of all possible bit widths.

        TYPES.put(TINYINT, new HazelcastIntegerType[Byte.SIZE + 1]);
        TYPES.put(SMALLINT, new HazelcastIntegerType[Short.SIZE + 1]);
        TYPES.put(INTEGER, new HazelcastIntegerType[Integer.SIZE + 1]);
        TYPES.put(BIGINT, new HazelcastIntegerType[Long.SIZE + 1]);

        for (Map.Entry<SqlTypeName, HazelcastIntegerType[]> entry : TYPES.entrySet()) {
            SqlTypeName typeName = entry.getKey();
            HazelcastIntegerType[] types = entry.getValue();

            HazelcastIntegerType[] nullableTypes = new HazelcastIntegerType[types.length];
            NULLABLE_TYPES.put(typeName, nullableTypes);

            for (int i = 0; i < types.length; ++i) {
                types[i] = new HazelcastIntegerType(typeName, false, i);
                nullableTypes[i] = new HazelcastIntegerType(typeName, true, i);
            }
        }
    }

    private static final HazelcastIntegerType[] TYPES_BY_BIT_WIDTH = new HazelcastIntegerType[Long.SIZE + 1];
    private static final HazelcastIntegerType[] NULLABLE_TYPES_BY_BIT_WIDTH = new HazelcastIntegerType[Long.SIZE + 1];

    static {
        // Build reverse mapping structures to map from a bit width to a
        // preferred integer type.

        for (int i = 0; i <= Long.SIZE; ++i) {
            HazelcastIntegerType type;
            HazelcastIntegerType nullableType;
            if (i < Byte.SIZE) {
                type = TYPES.get(TINYINT)[i];
                nullableType = NULLABLE_TYPES.get(TINYINT)[i];
            } else if (i < Short.SIZE) {
                type = TYPES.get(SMALLINT)[i];
                nullableType = NULLABLE_TYPES.get(SMALLINT)[i];
            } else if (i < Integer.SIZE) {
                type = TYPES.get(INTEGER)[i];
                nullableType = NULLABLE_TYPES.get(INTEGER)[i];
            } else {
                type = TYPES.get(BIGINT)[i];
                nullableType = NULLABLE_TYPES.get(BIGINT)[i];
            }

            TYPES_BY_BIT_WIDTH[i] = type;
            NULLABLE_TYPES_BY_BIT_WIDTH[i] = nullableType;
        }
    }

    private final int bitWidth;

    private HazelcastIntegerType(SqlTypeName typeName, boolean nullable, int bitWidth) {
        super(HazelcastTypeSystem.INSTANCE, typeName);
        this.isNullable = nullable;
        assert bitWidth >= 0 && bitWidth <= overflowBitWidthOf(typeName);
        this.bitWidth = bitWidth;

        // recompute the digest to reflect the nullability of the type
        computeDigest();
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        super.generateTypeString(sb, withDetail);
        if (withDetail) {
            sb.append('(').append(bitWidth).append(')');
        }
    }

    /**
     * @return the non-nullable integer type able to represent any integer value
     * of given integer type name.
     */
    public static HazelcastIntegerType of(SqlTypeName typeName) {
        assert supports(typeName);
        return TYPES.get(typeName)[bitWidthOf(typeName)];
    }

    /**
     * @return the integer type able to represent any integer value of given
     * integer type name with the given nullability.
     */
    public static HazelcastIntegerType of(SqlTypeName typeName, boolean nullable) {
        assert supports(typeName);
        if (nullable) {
            return NULLABLE_TYPES.get(typeName)[bitWidthOf(typeName)];
        } else {
            return TYPES.get(typeName)[bitWidthOf(typeName)];
        }
    }

    /**
     * @return the integer type equivalent to the given integer type with
     * the nullability adjusted according to the given nullability.
     */
    public static RelDataType of(RelDataType type, boolean nullable) {
        SqlTypeName typeName = type.getSqlTypeName();
        assert supports(typeName);

        if (type.isNullable() == nullable) {
            return type;
        }

        return HazelcastIntegerType.of(typeName, nullable, bitWidthOf(type));
    }

    /**
     * @return the narrowest integer type able to represent integer values of
     * given bit width with the given nullability.
     */
    public static HazelcastIntegerType of(int bitWidth, boolean nullable) {
        assert bitWidth >= 0;
        if (nullable) {
            return bitWidth > Long.SIZE ? NULLABLE_TYPES_BY_BIT_WIDTH[Long.SIZE] : NULLABLE_TYPES_BY_BIT_WIDTH[bitWidth];
        } else {
            return bitWidth > Long.SIZE ? TYPES_BY_BIT_WIDTH[Long.SIZE] : TYPES_BY_BIT_WIDTH[bitWidth];
        }
    }

    /**
     * @return {@code true} if the given type name is supported by {@link
     * HazelcastIntegerType}, {@code false} otherwise; supported types are
     * TINYINT, SMALLINT, INTEGER and BIGINT.
     */
    public static boolean supports(SqlTypeName typeName) {
        switch (typeName) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return true;
            default:
                return false;
        }
    }

    /**
     * @return the bit width required to represent the given value.
     */
    public static int bitWidthOf(long value) {
        // handle edge cases
        if (value == Long.MIN_VALUE) {
            return Long.SIZE - 1;
        } else if (value == Integer.MIN_VALUE) {
            return Integer.SIZE - 1;
        } else if (value == Short.MIN_VALUE) {
            return Short.SIZE - 1;
        } else if (value == Byte.MIN_VALUE) {
            return Byte.SIZE - 1;
        }

        value = Math.abs(value);
        return Long.SIZE - Long.numberOfLeadingZeros(value);
    }

    /**
     * @return the bit width specified by the given integer type stripping the
     * overflow indicator; consider 1 + CAST(1111 AS TINYINT), the cast has
     * overflown TINYINT(8) type, but if the cast was performed successfully
     * we know for sure its result is not overflown, so the addition operator
     * should not take the overflow indicator into account and use TINYINT(7)
     * instead.
     */
    public static int noOverflowBitWidthOf(RelDataType type) {
        assert supports(type.getSqlTypeName());

        HazelcastIntegerType integerType = (HazelcastIntegerType) type;
        return Math.min(integerType.bitWidth, bitWidthOf(type.getSqlTypeName()));
    }

    /**
     * Derives the integer type of the given numeric and supposedly
     * integer-valued SQL literal.
     * <p>
     * If the given literal is numeric, but can't be represented as Java {@code
     * long}, its type is assumed to be BIGINT. Callers should validate the
     * literal value to make sure it's compatible with the derived type.
     *
     * @param literal the literal to derive the type of.
     * @return the derived literal type.
     * @throws Error if the given literal is not numeric.
     */
    public static RelDataType deriveLiteralType(SqlLiteral literal) {
        long value;
        try {
            value = literal.bigDecimalValue().longValueExact();
        } catch (ArithmeticException e) {
            // It's ok to fallback to BIGINT here, we will fail later with a
            // proper error message while validating the literal.
            return HazelcastTypeFactory.INSTANCE.createSqlType(BIGINT);
        }

        return HazelcastIntegerType.of(bitWidthOf(value), false);
    }

    /**
     * Derives a type of the cast from one integer type to another respecting
     * the bit width information provided by the types.
     *
     * @param fromType the type to cast from.
     * @param toType   the type to cast to.
     * @return the resulting cast type.
     */
    public static RelDataType deriveCastType(RelDataType fromType, RelDataType toType) {
        SqlTypeName fromTypeName = fromType.getSqlTypeName();
        assert supports(fromTypeName);
        SqlTypeName toTypeName = toType.getSqlTypeName();
        assert supports(toTypeName);

        int fromBitWidth = bitWidthOf(fromType);
        int toBitWidth = bitWidthOf(toType);

        if (fromBitWidth < toBitWidth) {
            return HazelcastIntegerType.of(toTypeName, toType.isNullable(), fromBitWidth);
        } else if (fromBitWidth > toBitWidth) {
            int expandedBitWidth;
            if (fromBitWidth == overflowBitWidthOf(fromTypeName)) {
                // If a wider from type is overflown, a narrower to type is
                // overflown too.
                expandedBitWidth = overflowBitWidthOf(toTypeName);
            } else {
                // Casting from INT(31) to INT(1), for instance, is possible
                // without an overflow: INT(1) is still a 32-bit INT under the
                // hood.
                expandedBitWidth = Math.min(fromBitWidth, overflowBitWidthOf(toTypeName));
            }
            return HazelcastIntegerType.of(toTypeName, toType.isNullable(), expandedBitWidth);
        } else {
            return toType;
        }
    }

    /**
     * Derives a type of the cast of the given {@code long} value to the given
     * type.
     *
     * @param value  the value to cast.
     * @param toType the type to cast to.
     * @return the resulting cast type.
     */
    public static RelDataType deriveCastType(long value, RelDataType toType) {
        SqlTypeName toTypeName = toType.getSqlTypeName();
        assert supports(toTypeName);

        int valueBitWidth = bitWidthOf(value);
        int typeBitWidth = bitWidthOf(toTypeName);

        if (valueBitWidth > typeBitWidth) {
            return HazelcastIntegerType.of(toTypeName, toType.isNullable(), overflowBitWidthOf(toTypeName));
        } else {
            return HazelcastIntegerType.of(toTypeName, toType.isNullable(), valueBitWidth);
        }
    }

    /**
     * Finds the widest bit width integer type belonging to the same type name
     * (family) as the given target integer type from the given list of types.
     *
     * @param targetType the target type to find the widest instance of.
     * @param types      the list of types to inspect.
     * @return the found widest integer type.
     */
    public static RelDataType leastRestrictive(RelDataType targetType, List<RelDataType> types) {
        SqlTypeName typeName = targetType.getSqlTypeName();
        assert supports(typeName);

        int maxBitWidth = -1;
        RelDataType maxBitWidthType = null;

        for (RelDataType type : types) {
            if (type.getSqlTypeName() != typeName) {
                continue;
            }

            int bitWidth = bitWidthOf(type);

            if (bitWidth > maxBitWidth) {
                maxBitWidth = bitWidth;
                maxBitWidthType = type;
            }
        }
        assert maxBitWidthType != null;
        assert maxBitWidthType.getSqlTypeName() == typeName;

        return HazelcastIntegerType.of(maxBitWidthType, targetType.isNullable());
    }

    /**
     * @return the bit width enough to represent any integer value of the given
     * type name.
     */
    public static int bitWidthOf(SqlTypeName typeName) {
        switch (typeName) {
            case TINYINT:
                return Byte.SIZE - 1;
            case SMALLINT:
                return Short.SIZE - 1;
            case INTEGER:
                return Integer.SIZE - 1;
            case BIGINT:
                return Long.SIZE - 1;
            default:
                throw new IllegalArgumentException("unexpected type: " + typeName);
        }
    }

    /**
     * @return the bit width of the given integer type.
     */
    public static int bitWidthOf(RelDataType type) {
        assert supports(type.getSqlTypeName());
        return ((HazelcastIntegerType) type).bitWidth;
    }

    /**
     * @return {@code true} if the given type indicates a potential overflow,
     * {@code false} otherwise.
     */
    public static boolean canOverflow(RelDataType type) {
        assert type instanceof HazelcastIntegerType;
        HazelcastIntegerType integerType = (HazelcastIntegerType) type;

        int overflowBitWidth = overflowBitWidthOf(integerType.getSqlTypeName());
        assert integerType.bitWidth >= 0 && integerType.bitWidth <= overflowBitWidth;

        return integerType.bitWidth == overflowBitWidth;
    }

    private static HazelcastIntegerType of(SqlTypeName typeName, boolean nullable, int bitWidth) {
        assert bitWidth >= 0 && bitWidth <= overflowBitWidthOf(typeName);

        if (nullable) {
            return NULLABLE_TYPES.get(typeName)[bitWidth];
        } else {
            return TYPES.get(typeName)[bitWidth];
        }
    }

    private static int overflowBitWidthOf(SqlTypeName typeName) {
        switch (typeName) {
            case TINYINT:
                return Byte.SIZE;
            case SMALLINT:
                return Short.SIZE;
            case INTEGER:
                return Integer.SIZE;
            case BIGINT:
                return Long.SIZE;
            default:
                throw new IllegalArgumentException("unexpected type: " + typeName);
        }
    }

}
