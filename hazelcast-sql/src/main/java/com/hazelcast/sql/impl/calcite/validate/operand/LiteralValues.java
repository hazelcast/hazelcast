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

package com.hazelcast.sql.impl.calcite.validate.operand;

import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import com.hazelcast.sql.impl.type.converter.StringConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.Locale;

public final class LiteralValues {

    private static final String DOUBLE_POSITIVE_INFINITY = "infinity";
    private static final String DOUBLE_NEGATIVE_INFINITY = "-infinity";
    private static final String DOUBLE_NAN = "nan";
    private static final String DOUBLE_EXPONENT = "e";

    private final LiteralValue stringValue;
    private final LiteralValue value;

    public LiteralValues(LiteralValue stringValue, LiteralValue value) {
        this.stringValue = stringValue;
        this.value = value;
    }

    public LiteralValue value() {
        return stringValue != null ? stringValue : value;
    }

    public boolean hasStringAlias() {
        return stringValue != null && value != null;
    }

    public LiteralValue stringAlias() {
        return hasStringAlias() ? stringValue : null;
    }

    public static LiteralValues parse(SqlLiteral literal, RelDataTypeFactory typeFactory) {
        RelDataType literalType = literal.createSqlType(typeFactory);

        if (literal.getValue() == null) {
            return new LiteralValues(null, new LiteralValue(null, literalType));
        }

        switch (literal.getTypeName()) {
            case CHAR:
            case VARCHAR:
                return parseString(literal, literalType, typeFactory);

            case BOOLEAN:
                return parseBoolean(literal, literalType);

            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return parseBigint(literal);

            case DECIMAL:
                return parseDecimal(literal, literalType);

            case FLOAT:
            case REAL:
            case DOUBLE:
                return parseDouble(literal, literalType);

            default:
                return null;
        }
    }

    private static LiteralValues parseString(SqlLiteral literal, RelDataType literalType, RelDataTypeFactory typeFactory) {
        String value = literal.getValueAs(String.class);
        String lowerValue = value.toLowerCase(Locale.ROOT);

        LiteralValue derived = null;

        // Boolean?
        if (lowerValue.equals(Boolean.TRUE.toString())) {
            derived = new LiteralValue(typeFactory.createSqlType(SqlTypeName.BOOLEAN), true);
        } else if (lowerValue.equals(Boolean.FALSE.toString())) {
            derived = new LiteralValue(typeFactory.createSqlType(SqlTypeName.BOOLEAN), false);
        }

        // Integer?
        if (derived == null) {
            try {
                derived = optimizeBigint(Long.parseLong(lowerValue));
            } catch (NumberFormatException ignore) {
                // No-op
            }
        }

        // Fractional, inexact?
        if (derived == null) {
            Double doubleValue = null;

            if (DOUBLE_POSITIVE_INFINITY.equals(lowerValue)) {
                doubleValue = Double.POSITIVE_INFINITY;
            } else if (DOUBLE_NEGATIVE_INFINITY.equals(lowerValue)) {
                doubleValue = Double.NEGATIVE_INFINITY;
            } else if (DOUBLE_NAN.equals(lowerValue)) {
                doubleValue = Double.NaN;
            } else if (lowerValue.contains(DOUBLE_EXPONENT)) {
                try {
                    doubleValue = StringConverter.INSTANCE.asDouble(value);
                } catch (Exception ignore) {
                    // No-op.
                }
            }

            if (doubleValue != null) {
                derived = new LiteralValue(typeFactory.createSqlType(SqlTypeName.DOUBLE), doubleValue);
            }
        }

        // Fractional, exact?
        if (derived != null) {
            try {
                derived = new LiteralValue(typeFactory.createSqlType(SqlTypeName.DECIMAL), new BigDecimal(lowerValue));
            } catch (Exception ignore) {
                // No-op.
            }
        }

        return new LiteralValues(new LiteralValue(literalType, value), derived);
    }

    private static LiteralValues parseBoolean(SqlLiteral literal, RelDataType literalType) {
        // BOOLEAN literal represents only self
        return new LiteralValues(null, new LiteralValue(literalType, literal.booleanValue()));
    }

    private static LiteralValues parseBigint(SqlLiteral literal) {
        return new LiteralValues(null, optimizeBigint(literal.getValueAs(Long.class)));
    }

    private static LiteralValues parseDecimal(SqlLiteral literal, RelDataType literalType) {
        // DECIMAL literal represents only self
        return new LiteralValues(null, new LiteralValue(literalType, literal.getValueAs(BigDecimal.class)));
    }

    private static LiteralValues parseDouble(SqlLiteral literal, RelDataType literalType) {
        // DOUBLE literal represents only self
        return new LiteralValues(null, new LiteralValue(literalType, literal.getValueAs(Double.class)));
    }

    private static LiteralValue optimizeBigint(long value) {
        int bitWidth = HazelcastIntegerType.bitWidthOf(value);

        RelDataType type = HazelcastIntegerType.of(bitWidth, false);

        switch (SqlToQueryType.map(type.getSqlTypeName()).getTypeFamily()) {
            case TINYINT:
                return new LiteralValue(type, (byte) value);

            case SMALLINT:
                return new LiteralValue(type, (short) value);

            case INTEGER:
                return new LiteralValue(type, (int) value);

            default:
                return new LiteralValue(type, value);
        }
    }
}
