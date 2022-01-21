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

import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
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
            String error = String.format(
                    "Parameter at position %d must be of %s type, but %s was found (consider adding an explicit CAST)",
                    ordinal,
                    targetType.getTypeFamily().getPublicType(),
                    valueConverter.getTypeFamily().getPublicType()
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

    public static ParameterConverter from(QueryDataType targetType, int index, SqlParserPos parserPosition) {
        QueryDataTypeFamily targetTypeFamily = targetType.getTypeFamily();
        if (targetTypeFamily.isNumeric()) {
            return new NumericPrecedenceParameterConverter(index, parserPosition, targetType);
        } else if (targetTypeFamily.isTemporal()) {
            return new TemporalPrecedenceParameterConverter(index, parserPosition, targetType);
        } else if (targetTypeFamily.isObject()) {
            return NoOpParameterConverter.INSTANCE;
        } else if (targetTypeFamily == QueryDataTypeFamily.JSON) {
            return new JsonParameterConverter(index, parserPosition, targetType);
        } else {
            return new StrictParameterConverter(index, parserPosition, targetType);
        }
    }
}
