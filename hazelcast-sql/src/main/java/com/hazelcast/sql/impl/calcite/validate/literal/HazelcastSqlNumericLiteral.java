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

package com.hazelcast.sql.impl.calcite.validate.literal;

import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.type.converter.BigDecimalConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;

public final class HazelcastSqlNumericLiteral extends HazelcastSqlLiteral {

    private final Mode mode;
    private final int bitWidth;

    private HazelcastSqlNumericLiteral(
        SqlLiteral original,
        Object value,
        SqlTypeName typeName,
        Mode mode,
        int bitWidth
    ) {
        super(original, value, typeName);

        this.mode = mode;
        this.bitWidth = bitWidth;
    }

    public static HazelcastSqlNumericLiteral create(SqlNumericLiteral original) {
        BigDecimal valueDecimal = (BigDecimal) original.getValue();

        if (original.isExact()) {
            if (original.getScale() == 0) {
                // Dealing with integer type family
                try {
                    long value = BigDecimalConverter.INSTANCE.asBigint(valueDecimal);

                    int bitWidth = HazelcastIntegerType.bitWidthOf(value);

                    RelDataType type = HazelcastIntegerType.of(bitWidth, false);

                    long adjustedValue;

                    switch (type.getSqlTypeName()) {
                        case TINYINT:
                            adjustedValue = (byte) value;
                            break;

                        case SMALLINT:
                            adjustedValue = (short) value;
                            break;

                        case INTEGER:
                            adjustedValue = (int) value;
                            break;

                        default:
                            assert type.getSqlTypeName() == SqlTypeName.BIGINT;
                            adjustedValue = value;
                    }

                    return new HazelcastSqlNumericLiteral(
                        original,
                        adjustedValue,
                        type.getSqlTypeName(),
                        Mode.INTEGER,
                        bitWidth
                    );
                } catch (Exception ignore) {
                    // Fallback to DECIMAL
                }
            }

            // Dealing with DECIMAL
            return new HazelcastSqlNumericLiteral(
                original,
                valueDecimal,
                SqlTypeName.DECIMAL,
                Mode.FRACTIONAL_EXACT,
                0
            );

        } else {
            // Dealing with DOUBLE
            return new HazelcastSqlNumericLiteral(
                original,
                valueDecimal,
                SqlTypeName.DOUBLE,
                Mode.FRACTIONAL_INEXACT,
                0
            );
        }
    }

    @Override
    public HazelcastSqlNumericLiteral clone(SqlParserPos pos) {
        return new HazelcastSqlNumericLiteral(original.clone(pos), value, typeName, mode, bitWidth);
    }

    @Override
    public RelDataType getType(HazelcastTypeFactory typeFactory) {
        switch (mode) {
            case INTEGER:
                return HazelcastIntegerType.of(bitWidth, false);

            case FRACTIONAL_EXACT:
                return typeFactory.createSqlType(SqlTypeName.DECIMAL);

            default:
                assert mode == Mode.FRACTIONAL_INEXACT;

                return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        }
    }

    private enum Mode {
        INTEGER,
        FRACTIONAL_EXACT,
        FRACTIONAL_INEXACT
    }
}
