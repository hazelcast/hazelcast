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
import static org.apache.calcite.sql.type.SqlTypeName.NULL;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;

public final class HazelcastIntegerType extends BasicSqlType {

    private static final Map<SqlTypeName, HazelcastIntegerType[]> TYPES = new HashMap<>();
    private static final Map<SqlTypeName, HazelcastIntegerType[]> NULLABLE_TYPES = new HashMap<>();

    static {
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
        assert bitWidth >= 0 && (bitWidth <= overflowBitWidthOf(typeName));
        this.bitWidth = bitWidth;

        computeDigest();
    }

    public static boolean canOverflow(RelDataType type) {
        assert type instanceof HazelcastIntegerType;
        HazelcastIntegerType integerType = (HazelcastIntegerType) type;

        int overflowBitWidth = overflowBitWidthOf(integerType.getSqlTypeName());
        assert integerType.bitWidth >= 0 && integerType.bitWidth <= overflowBitWidth;

        return integerType.bitWidth == overflowBitWidth;
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        super.generateTypeString(sb, withDetail);
        if (withDetail) {
            sb.append('(');
            sb.append(bitWidth);
            sb.append(')');
        }
    }

    public static HazelcastIntegerType of(SqlTypeName typeName) {
        return TYPES.get(typeName)[bitWidthOf(typeName)];
    }

    public static HazelcastIntegerType of(SqlTypeName typeName, boolean nullable) {
        if (nullable) {
            return NULLABLE_TYPES.get(typeName)[bitWidthOf(typeName)];
        } else {
            return TYPES.get(typeName)[bitWidthOf(typeName)];
        }
    }

    public static RelDataType of(RelDataType type, boolean nullable) {
        SqlTypeName typeName = type.getSqlTypeName();
        assert supports(typeName);

        if (type instanceof HazelcastIntegerType) {
            if (type.isNullable() == nullable) {
                return type;
            }

            HazelcastIntegerType hazelcastType = (HazelcastIntegerType) type;
            return HazelcastIntegerType.of(typeName, nullable, hazelcastType.bitWidth);
        }

        return HazelcastIntegerType.of(typeName, nullable, bitWidthOf(typeName));
    }

    public static HazelcastIntegerType of(int bitWidth, boolean nullable) {
        assert bitWidth >= 0;
        if (nullable) {
            return bitWidth > Long.SIZE ? NULLABLE_TYPES_BY_BIT_WIDTH[Long.SIZE] : NULLABLE_TYPES_BY_BIT_WIDTH[bitWidth];
        } else {
            return bitWidth > Long.SIZE ? TYPES_BY_BIT_WIDTH[Long.SIZE] : TYPES_BY_BIT_WIDTH[bitWidth];
        }
    }

    public static boolean supports(SqlTypeName typeName) {
        if (typeName == null) {
            return false;
        }

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

    public static int bitWidthOf(long value) {
        if (value == Long.MIN_VALUE) {
            return Long.SIZE - 1;
        }

        value = Math.abs(value);
        return Long.SIZE - Long.numberOfLeadingZeros(value);
    }

    public static int noOverflowBitWidthOf(RelDataType type) {
        assert supports(type.getSqlTypeName());

        HazelcastIntegerType integerType = (HazelcastIntegerType) type;
        return Math.min(integerType.bitWidth, bitWidthOf(type.getSqlTypeName()));
    }

    public static RelDataType deriveLiteralType(SqlLiteral literal) {
        if (literal.getTypeName() == NULL) {
            return null;
        }

        long value;
        try {
            value = literal.bigDecimalValue().longValueExact();
        } catch (ArithmeticException e) {
            return HazelcastTypeFactory.INSTANCE.createSqlType(BIGINT);
        }

        int bitWidth = bitWidthOf(value);

        SqlTypeName typeName;
        if (bitWidth <= Byte.SIZE - 1) {
            typeName = TINYINT;
        } else if (bitWidth <= Short.SIZE - 1) {
            typeName = SMALLINT;
        } else if (bitWidth <= Integer.SIZE - 1) {
            typeName = INTEGER;
        } else {
            typeName = BIGINT;
        }

        return HazelcastIntegerType.of(typeName, false, bitWidth);
    }

    public static RelDataType cast(RelDataType fromType, RelDataType toType) {
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
                // without overflow: INT(1) is still a 32-bit INT.
                expandedBitWidth = Math.min(fromBitWidth, overflowBitWidthOf(toTypeName));
            }
            return HazelcastIntegerType.of(toTypeName, toType.isNullable(), expandedBitWidth);
        } else {
            return toType;
        }
    }

    public static RelDataType cast(long value, RelDataType toType) {
        SqlTypeName toTypeName = toType.getSqlTypeName();
        assert supports(toTypeName);

        int valueBitWidth = bitWidthOf(value);
        int typeBitWidth = bitWidthOf(toTypeName);
        if (valueBitWidth > typeBitWidth) {
            return HazelcastIntegerType.of(toTypeName, toType.isNullable(), overflowBitWidthOf(toTypeName));
        }

        return HazelcastIntegerType.of(toTypeName, toType.isNullable(), valueBitWidth);
    }

    public static RelDataType leastRestrictive(SqlTypeName typeName, boolean nullable, List<RelDataType> types) {
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

        return HazelcastIntegerType.of(maxBitWidthType, nullable);
    }

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

    public static int bitWidthOf(RelDataType type) {
        assert supports(type.getSqlTypeName());
        return ((HazelcastIntegerType) type).bitWidth;
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
