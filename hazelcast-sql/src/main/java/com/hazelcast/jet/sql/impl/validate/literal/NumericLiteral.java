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

package com.hazelcast.jet.sql.impl.validate.literal;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastIntegerType;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.type.converter.BigDecimalConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import java.math.BigDecimal;

public final class NumericLiteral extends Literal {

    private final Mode mode;
    private final int bitWidth;

    private NumericLiteral(
            Object value,
            SqlTypeName typeName,
            Mode mode,
            int bitWidth
    ) {
        super(value, typeName);

        this.mode = mode;
        this.bitWidth = bitWidth;
    }

    public static Literal create(SqlTypeName typeName, Object value) {
        // Calcite always uses DECIMAL or DOUBLE for literals.
        // If the DECIMAL is an integer that fits into a smaller integer type, replace the type.
        if (value instanceof BigDecimal && !hasDecimalPlaces((BigDecimal) value)
                && !HazelcastTypeUtils.isNumericInexactType(typeName)) {
            Literal res = tryReduceInteger((BigDecimal) value);
            if (res != null) {
                return res;
            }
        }

        if (value instanceof BigDecimal) {
            if (typeName == SqlTypeName.DOUBLE) {
                value = ((BigDecimal) value).doubleValue();
            } else if (typeName == SqlTypeName.REAL) {
                value = ((BigDecimal) value).floatValue();
            }
        }

        return new NumericLiteral(
                value,
                typeName,
                typeName == SqlTypeName.DOUBLE ? Mode.FRACTIONAL_INEXACT : Mode.FRACTIONAL_EXACT,
                0
        );
    }

    private static Literal tryReduceInteger(BigDecimal value) {
        // Dealing with integer type family
        long longValue;
        try {
            longValue = BigDecimalConverter.INSTANCE.asBigint(value);
        } catch (QueryException e) {
            assert e.getMessage().contains("Numeric overflow");
            return null;
        }

        int bitWidth = HazelcastIntegerType.bitWidthOf(longValue);
        RelDataType type = HazelcastIntegerType.create(bitWidth, false);
        Object adjustedValue;

        switch (type.getSqlTypeName()) {
            case TINYINT:
                adjustedValue = (byte) longValue;
                break;

            case SMALLINT:
                adjustedValue = (short) longValue;
                break;

            case INTEGER:
                adjustedValue = (int) longValue;
                break;

            default:
                assert type.getSqlTypeName() == SqlTypeName.BIGINT;
                adjustedValue = longValue;
        }

        return new NumericLiteral(
                adjustedValue,
                type.getSqlTypeName(),
                Mode.INTEGER,
                bitWidth
        );
    }

    @Override
    public RelDataType getType(HazelcastTypeFactory typeFactory) {
        switch (mode) {
            case INTEGER:
                return HazelcastIntegerType.create(bitWidth, false);

            case FRACTIONAL_EXACT:
                return typeFactory.createSqlType(SqlTypeName.DECIMAL);

            default:
                assert mode == Mode.FRACTIONAL_INEXACT;

                return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        }
    }

    @Override
    public String getStringValue() {
        if (mode != Mode.FRACTIONAL_INEXACT) {
            return value.toString();
        }
        return Util.toScientificNotation(BigDecimal.valueOf((double) value).stripTrailingZeros());
    }

    private static boolean hasDecimalPlaces(BigDecimal value) {
        return value.scale() > 0;
    }

    private enum Mode {
        INTEGER,
        FRACTIONAL_EXACT,
        FRACTIONAL_INEXACT
    }
}
