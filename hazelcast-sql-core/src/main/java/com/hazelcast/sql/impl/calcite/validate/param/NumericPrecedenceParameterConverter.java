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

package com.hazelcast.sql.impl.calcite.validate.param;

import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.converter.Converter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class NumericPrecedenceParameterConverter extends AbstractParameterConverter {
    public NumericPrecedenceParameterConverter(int ordinal, SqlParserPos parserPos, QueryDataType type) {
        super(ordinal, parserPos, type);
    }

    @Override
    protected boolean isValid(Object value, Converter valueConverter) {
        QueryDataTypeFamily valueTypeFamily = valueConverter.getTypeFamily();

        return valueTypeFamily.isNumeric() && valueTypeFamily.getPrecedence() <= targetType.getTypeFamily().getPrecedence();
    }
}
