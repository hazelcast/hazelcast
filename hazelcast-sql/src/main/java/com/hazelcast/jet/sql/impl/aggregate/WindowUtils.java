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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.sql.impl.validate.ValidatorResource;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.EmptyRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.jet.sql.impl.validate.ValidationUtil.unwrapFunctionOperand;
import static com.hazelcast.sql.impl.expression.datetime.DateTimeUtils.asTimestampWithTimezone;
import static com.hazelcast.sql.impl.type.converter.AbstractTemporalConverter.DEFAULT_ZONE;
import static org.apache.calcite.util.Static.RESOURCE;

public final class WindowUtils {

    private WindowUtils() {
    }

    /**
     * Returns a row with two added fields: the window_start and window_end.
     */
    public static Object[] addWindowBounds(Object[] row, int index, SlidingWindowPolicy windowPolicy) {
        Object value = row[index];
        long millis = extractMillis(value);
        long windowStartMillis = windowPolicy.floorFrameTs(millis);
        long windowEndMillis = windowPolicy.higherFrameTs(millis);
        Object[] result = Arrays.copyOf(row, row.length + 2);
        if (value instanceof Byte) {
            result[result.length - 2] = (byte) windowStartMillis;
            result[result.length - 1] = (byte) windowEndMillis;
            return result;
        } else if (value instanceof Short) {
            result[result.length - 2] = (short) windowStartMillis;
            result[result.length - 1] = (short) windowEndMillis;
            return result;
        } else if (value instanceof Integer) {
            result[result.length - 2] = (int) windowStartMillis;
            result[result.length - 1] = (int) windowEndMillis;
            return result;
        } else if (value instanceof Long) {
            result[result.length - 2] = windowStartMillis;
            result[result.length - 1] = windowEndMillis;
            return result;
        } else if (value instanceof LocalTime) {
            result[result.length - 2] = asTimestampWithTimezone(windowStartMillis, DEFAULT_ZONE).toLocalTime();
            result[result.length - 1] = asTimestampWithTimezone(windowEndMillis, DEFAULT_ZONE).toLocalTime();
            return result;
        } else if (value instanceof LocalDate) {
            result[result.length - 2] = asTimestampWithTimezone(windowStartMillis, DEFAULT_ZONE).toLocalDate();
            result[result.length - 1] = asTimestampWithTimezone(windowEndMillis, DEFAULT_ZONE).toLocalDate();
            return result;
        } else if (value instanceof LocalDateTime) {
            result[result.length - 2] = asTimestampWithTimezone(windowStartMillis, DEFAULT_ZONE).toLocalDateTime();
            result[result.length - 1] = asTimestampWithTimezone(windowEndMillis, DEFAULT_ZONE).toLocalDateTime();
            return result;
        } else {
            result[result.length - 2] = asTimestampWithTimezone(windowStartMillis, DEFAULT_ZONE);
            result[result.length - 1] = asTimestampWithTimezone(windowEndMillis, DEFAULT_ZONE);
            return result;
        }
    }

    public static long extractMillis(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof LocalTime) {
            return QueryDataType.TIME.getConverter().asTimestampWithTimezone(value).toInstant().toEpochMilli();
        } else if (value instanceof LocalDate) {
            return QueryDataType.DATE.getConverter().asTimestampWithTimezone(value).toInstant().toEpochMilli();
        } else if (value instanceof LocalDateTime) {
            return QueryDataType.TIMESTAMP.getConverter().asTimestampWithTimezone(value).toInstant().toEpochMilli();
        } else {
            return ((OffsetDateTime) value).toInstant().toEpochMilli();
        }
    }

    public static long extractMillis(Expression<?> expression, ExpressionEvalContext evalContext) {
        Object lag = expression.eval(EmptyRow.INSTANCE, evalContext);
        return lag instanceof Number ? ((Number) lag).longValue() : ((SqlDaySecondInterval) lag).getMillis();
    }

    /**
     * Return the datatype of the target column referenced by the
     * DESCRIPTOR argument.
     */
    public static SqlTypeName getOrderingColumnType(SqlCallBinding binding, int orderingColumnParameterIndex) {
        SqlNode input = binding.operand(0);

        SqlCall descriptor = (SqlCall) unwrapFunctionOperand(binding.operand(orderingColumnParameterIndex));
        List<SqlNode> columnIdentifiers = descriptor.getOperandList();
        if (columnIdentifiers.size() != 1) {
            throw SqlUtil.newContextException(descriptor.getParserPosition(),
                    ValidatorResource.RESOURCE.mustUseSingleOrderingColumn());
        }

        // paramValue is the DESCRIPTOR call, it's operand is an SqlIdentifier having the column name
        SqlIdentifier orderingColumnIdentifier = (SqlIdentifier) descriptor.getOperandList().get(0);
        String orderingColumnName = orderingColumnIdentifier.getSimple();

        SqlValidator validator = binding.getValidator();
        RelDataTypeField columnField = validator
                .getValidatedNodeType(input)
                .getField(orderingColumnName, validator.getCatalogReader().nameMatcher().isCaseSensitive(), false);
        if (columnField == null) {
            throw SqlUtil.newContextException(descriptor.getParserPosition(),
                    RESOURCE.unknownIdentifier(orderingColumnName));
        }
        return columnField.getType().getSqlTypeName();
    }
}
