/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.validate.types;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
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
@SuppressWarnings("checkstyle:ExecutableStatementCount")
public final class HazelcastIntegerType extends BasicSqlType {

    private static final Map<SqlTypeName, HazelcastIntegerType[]> TYPES = new HashMap<>();
    private static final Map<SqlTypeName, HazelcastIntegerType[]> NULLABLE_TYPES = new HashMap<>();

    private static final HazelcastIntegerType[] TYPES_BY_BIT_WIDTH = new HazelcastIntegerType[Long.SIZE + 1];
    private static final HazelcastIntegerType[] NULLABLE_TYPES_BY_BIT_WIDTH = new HazelcastIntegerType[Long.SIZE + 1];

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
        this.bitWidth = bitWidth;

        // recompute the digest to reflect the nullability of the type
        computeDigest();
    }

    public int getBitWidth() {
        return bitWidth;
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        super.generateTypeString(sb, withDetail);

        if (withDetail) {
            sb.append('(').append(bitWidth).append(')');
        }
    }

    public static RelDataType create(HazelcastIntegerType type, boolean nullable) {
        if (type.isNullable() == nullable) {
            return type;
        }

        return create0(type.getSqlTypeName(), nullable, type.getBitWidth());
    }

    public static HazelcastIntegerType create(SqlTypeName typeName, boolean nullable) {
        return create0(typeName, nullable, bitWidthOf(typeName));
    }

    private static HazelcastIntegerType create0(SqlTypeName typeName, boolean nullable, int bitWidth) {
        if (nullable) {
            return NULLABLE_TYPES.get(typeName)[bitWidth];
        } else {
            return TYPES.get(typeName)[bitWidth];
        }
    }

    /**
     * @return the narrowest integer type able to represent integer values of
     * given bit width with the given nullability.
     */
    public static HazelcastIntegerType create(int bitWidth, boolean nullable) {
        assert bitWidth >= 0;

        if (nullable) {
            return NULLABLE_TYPES_BY_BIT_WIDTH[Math.min(bitWidth, Long.SIZE)];
        } else {
            return TYPES_BY_BIT_WIDTH[Math.min(bitWidth, Long.SIZE)];
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
     * @return the bit width enough to represent any integer value of the given
     * type name.
     */
    static int bitWidthOf(SqlTypeName typeName) {
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
}
