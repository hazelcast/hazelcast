/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

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
        Converter converter = Converters.getConverter(value.getClass());
        if (HazelcastTypeUtils.isNumericIntegerType(typeName) || typeName == SqlTypeName.DECIMAL) {
            // Dealing with integer type family
            long longValue;
            try {
                longValue = converter.asBigint(value);
            } catch (Exception ignore) {
                // Numeric overflow for BIGINT - dealing with DECIMAL
                return new NumericLiteral(
                        converter.asDecimal(value),
                        SqlTypeName.DECIMAL,
                        Mode.FRACTIONAL_EXACT,
                        0
                );
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

        // Dealing with DOUBLE
        return new NumericLiteral(
            converter.asDouble(value),
            SqlTypeName.DOUBLE,
            Mode.FRACTIONAL_INEXACT,
            0
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

    private enum Mode {
        INTEGER,
        FRACTIONAL_EXACT,
        FRACTIONAL_INEXACT
    }
}
