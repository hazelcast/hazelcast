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

package com.hazelcast.jet.sql.impl.validate.param;

import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.converter.Converter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class JsonParameterConverter extends AbstractParameterConverter {

    public JsonParameterConverter(int ordinal, SqlParserPos parserPos, QueryDataType targetType) {
        super(ordinal, parserPos, targetType);
    }

    @Override
    protected boolean isValid(Object value, Converter valueConverter) {
        QueryDataTypeFamily srcFamily = valueConverter.getTypeFamily();
        return srcFamily == QueryDataTypeFamily.VARCHAR || srcFamily == QueryDataTypeFamily.JSON;
    }
}
