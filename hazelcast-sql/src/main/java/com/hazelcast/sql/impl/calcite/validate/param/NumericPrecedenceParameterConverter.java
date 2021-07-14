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

package com.hazelcast.sql.impl.calcite.validate.param;

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

        return valueTypeFamily.isNumeric() && valueTypeFamily.getPrecedence() <= targetType.getTypeFamily().getPrecedence();
    }
}
