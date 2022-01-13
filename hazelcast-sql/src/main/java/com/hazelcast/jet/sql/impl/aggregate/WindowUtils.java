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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.sql.impl.validate.ValidatorResource;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.EmptyRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.jet.sql.impl.validate.ValidationUtil.unwrapFunctionOperand;
import static com.hazelcast.sql.impl.expression.datetime.DateTimeUtils.asTimestampWithTimezone;
import static com.hazelcast.sql.impl.type.converter.AbstractTemporalConverter.DEFAULT_ZONE;
import static org.apache.calcite.util.Static.RESOURCE;

public final class WindowUtils {

    private WindowUtils() {
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public static Object[] insertWindowBound(
            Object[] row,
            long windowStart,
            long windowEnd,
            int[] mapping
    ) {
        Object[] result = new Object[mapping.length];
        for (int i = 0; i < mapping.length; i++) {
            if (mapping[i] == -1) {
                // TODO: [viliam] could we use Instant here (QueryDataType.TIMESTAMP_WITH_TZ_INSTANT).
                // That conversion is much cheaper.
                result[i] = OffsetDateTime.ofInstant(Instant.ofEpochMilli(windowStart), ZoneId.systemDefault());
            } else if (mapping[i] == -2) {
                result[i] = OffsetDateTime.ofInstant(Instant.ofEpochMilli(windowEnd), ZoneId.systemDefault());
            } else {
                result[i] = row[mapping[i]];
            }
        }
        return result;
    }

    /**
     * Returns a traverser of rows with two added fields: the
     * window_start and window_end. For tumbling windows, the returned
     * traverser always contains a single row. For hopping windows, it
     * contains multiple rows.
     */
    public static Traverser<Object[]> addWindowBounds(Object[] row, int timeStampIndex, SlidingWindowPolicy windowPolicy) {
        Object value = row[timeStampIndex];
        if (value == null) {
            return Traversers.singleton(Arrays.copyOf(row, row.length + 2));
        }
        long millis = extractMillis(value);

        long slideStep = windowPolicy.frameSize();
        long firstWindowStart = windowPolicy.floorFrameTs(millis - windowPolicy.windowSize() + slideStep);

        return new Traverser<Object[]>() {
            long currentStart = firstWindowStart;

            @Override
            public Object[] next() {
                if (currentStart >= firstWindowStart + windowPolicy.windowSize()) {
                    return null;
                }
                try {
                    return addWindowBoundsSingleRow(row, value, currentStart, currentStart + windowPolicy.windowSize());
                } finally {
                    currentStart += slideStep;
                }
            }
        };
    }

    private static Object[] addWindowBoundsSingleRow(Object[] row, Object timeStamp, long windowStart, long windowEnd) {
        Object[] result = Arrays.copyOf(row, row.length + 2);
        if (timeStamp instanceof Byte) {
            result[result.length - 2] = (byte) windowStart;
            result[result.length - 1] = (byte) windowEnd;
            return result;
        } else if (timeStamp instanceof Short) {
            result[result.length - 2] = (short) windowStart;
            result[result.length - 1] = (short) windowEnd;
            return result;
        } else if (timeStamp instanceof Integer) {
            result[result.length - 2] = (int) windowStart;
            result[result.length - 1] = (int) windowEnd;
            return result;
        } else if (timeStamp instanceof Long) {
            result[result.length - 2] = windowStart;
            result[result.length - 1] = windowEnd;
            return result;
        } else if (timeStamp instanceof LocalTime) {
            result[result.length - 2] = asTimestampWithTimezone(windowStart, DEFAULT_ZONE).toLocalTime();
            result[result.length - 1] = asTimestampWithTimezone(windowEnd, DEFAULT_ZONE).toLocalTime();
            return result;
        } else if (timeStamp instanceof LocalDate) {
            result[result.length - 2] = asTimestampWithTimezone(windowStart, DEFAULT_ZONE).toLocalDate();
            result[result.length - 1] = asTimestampWithTimezone(windowEnd, DEFAULT_ZONE).toLocalDate();
            return result;
        } else if (timeStamp instanceof LocalDateTime) {
            result[result.length - 2] = asTimestampWithTimezone(windowStart, DEFAULT_ZONE).toLocalDateTime();
            result[result.length - 1] = asTimestampWithTimezone(windowEnd, DEFAULT_ZONE).toLocalDateTime();
            return result;
        } else {
            result[result.length - 2] = asTimestampWithTimezone(windowStart, DEFAULT_ZONE);
            result[result.length - 1] = asTimestampWithTimezone(windowEnd, DEFAULT_ZONE);
            return result;
        }
    }

    public static Object convert(long millis, SqlTypeName typeName) {
        switch (typeName) {
            case TINYINT:
                return (byte) millis;
            case SMALLINT:
                return (short) millis;
            case INTEGER:
                return (int) millis;
            case BIGINT:
                return millis;
            case TIME:
                return asTimestampWithTimezone(millis, DEFAULT_ZONE).toLocalTime();
            case DATE:
                return asTimestampWithTimezone(millis, DEFAULT_ZONE).toLocalDate();
            case TIMESTAMP:
                return asTimestampWithTimezone(millis, DEFAULT_ZONE).toLocalDateTime();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return asTimestampWithTimezone(millis, DEFAULT_ZONE);
            default:
                throw new IllegalArgumentException();
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
     * Return the datatype of the target column referenced by the DESCRIPTOR argument.
     */
    public static RelDataType getOrderingColumnType(SqlCallBinding binding, int orderingColumnParameterIndex) {
        SqlNode input = binding.operand(0);

        SqlCall descriptor = (SqlCall) unwrapFunctionOperand(binding.operand(orderingColumnParameterIndex));
        List<SqlNode> columnIdentifiers = descriptor.getOperandList();
        if (columnIdentifiers.size() != 1) {
            throw SqlUtil.newContextException(descriptor.getParserPosition(),
                    ValidatorResource.RESOURCE.mustUseSingleOrderingColumn());
        }

        // `descriptor` is the DESCRIPTOR call, its operand is an SqlIdentifier having the column name
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
        return columnField.getType();
    }
}
