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

import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;
import org.apache.calcite.sql.parser.SqlParserPos;

public abstract class AbstractParameterConverter implements ParameterConverter {

    protected final int ordinal;
    protected final SqlParserPos parserPos;
    protected final QueryDataType targetType;

    protected AbstractParameterConverter(int ordinal, SqlParserPos parserPos, QueryDataType targetType) {
        this.ordinal = ordinal;
        this.parserPos = parserPos;
        this.targetType = targetType;
    }

    @Override
    public QueryDataType getTargetType() {
        return targetType;
    }

    @Override
    public final Object convert(Object value) {
        // NULL value is always OK
        if (value == null) {
            return null;
        }

        // Validate the value
        Converter valueConverter = Converters.getConverter(value.getClass());

        if (!isValid(value, valueConverter)) {
            String actualTypeName = valueConverter.getTypeFamily().getPublicType().name();
            String targetTypeName = targetType.getTypeFamily().getPublicType().name();

            String error = String.format(
                    "Parameter at position %d must be of %s type, but %s was found (consider adding an explicit CAST)",
                    ordinal,
                    targetTypeName,
                    actualTypeName
            );

            throw QueryException.error(SqlErrorCode.DATA_EXCEPTION, withContext(error));
        }

        // Convert the value
        try {
            return targetType.getConverter().convertToSelf(valueConverter, value);
        } catch (Exception e) {
            String error = String.format(
                    "Failed to convert parameter at position %d from %s to %s: %s",
                    ordinal,
                    valueConverter.getTypeFamily().getPublicType(),
                    targetType.getConverter().getTypeFamily().getPublicType(),
                    e.getMessage()
            );

            throw QueryException.error(SqlErrorCode.DATA_EXCEPTION, withContext(error), e);
        }
    }

    protected abstract boolean isValid(Object value, Converter valueConverter);

    private String withContext(String message) {
        int line = parserPos.getLineNum();
        int col = parserPos.getColumnNum();
        int endLine = parserPos.getEndLineNum();
        int endCol = parserPos.getEndColumnNum();

        String context;

        if (line == endLine && col == endCol) {
            context = String.format("At line %d, column %d", line, col);
        } else {
            context = String.format("From line %d, column %d to line %d, column %d", line, col, endLine, endCol);
        }

        return context + ": " + message;
    }
}
