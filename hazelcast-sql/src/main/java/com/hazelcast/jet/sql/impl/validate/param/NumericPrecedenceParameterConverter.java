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

package com.hazelcast.jet.sql.impl.validate.param;

import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.converter.Converter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class NumericPrecedenceParameterConverter extends AbstractParameterConverter {
    public NumericPrecedenceParameterConverter(int ordinal, SqlParserPos parserPos, QueryDataType targetType) {
        super(ordinal, parserPos, targetType);
    }

    @Override
    protected boolean isValid(Object value, Converter valueConverter) {
        QueryDataTypeFamily valueTypeFamily = valueConverter.getTypeFamily();

        if (!valueTypeFamily.isNumeric()) {
            // conversion from non-numeric value where a numeric parameter is expected isn't allowed
            return false;
        }

        if (valueTypeFamily.getPrecedence() <= targetType.getTypeFamily().getPrecedence()) {
            // Conversion from value with lower precedence to target with higher precedence is allowed.
            // For examplce, conversion from INTEGER to BIGINT is allowed or from BIGINT to REAL
            return true;
        }

        if (targetType.getTypeFamily().isNumericInteger() && valueConverter.getTypeFamily().isNumericInteger()) {
            // we allow conversion among any integer types. If the value is out of range, it will fail later.
            return true;
        }

        // we don't allow conversion from numeric types with higher precedence to lower precedence types.
        // For example conversion from DOUBLE to DECIMAL isn't allowed, or from DECIMAL to integer types.
        // The reason is that precision loss is possible.
        return false;
    }
}
