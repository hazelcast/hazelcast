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
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
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
     * Inject window bounds into the row, according to the mapping.
     * <p>
     * The result row will always have `mapping.length` fields. Where the
     * mapping has value of -1, windowStart will be used. Where it has a value
     * of -2, windowEnd will be used. Otherwise, the field from input row
     * referenced by the (non-negative) mapping value will be used.
     *
     * @return row with inserted bounds
     */
    @SuppressWarnings("checkstyle:MagicNumber")
    public static JetSqlRow insertWindowBound(
            JetSqlRow row,
            long windowStart,
            long windowEnd,
            QueryDataType descriptorType,
            int[] mapping
    ) {
        Object[] result = new Object[mapping.length];
        for (int i = 0; i < mapping.length; i++) {
            if (mapping[i] == -1) {
                result[i] = convertWindowBound(windowStart, descriptorType);
            } else if (mapping[i] == -2) {
                result[i] = convertWindowBound(windowEnd, descriptorType);
            } else {
                result[i] = row.get(mapping[i]);
            }
        }
        return new JetSqlRow(row.getSerializationService(), result);
    }

    /**
     * Returns a traverser of rows with two added fields: the
     * window_start and window_end. For tumbling windows, the returned
     * traverser always contains a single row. For hopping windows, it
     * contains multiple rows.
     */
    public static Traverser<JetSqlRow> addWindowBounds(JetSqlRow row, int timeStampIndex, SlidingWindowPolicy windowPolicy) {
        Object value = row.get(timeStampIndex);
        if (value == null) {
            return Traversers.singleton(row.extendedRow(2));
        }
        long millis = extractMillis(value);

        long slideStep = windowPolicy.frameSize();
        long firstWindowStart = windowPolicy.floorFrameTs(millis - windowPolicy.windowSize() + slideStep);

        return new Traverser<JetSqlRow>() {
            long currentStart = firstWindowStart;

            @Override
            public JetSqlRow next() {
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

    private static Object convertWindowBound(long boundary, QueryDataType descriptorType) {
        if (descriptorType.getTypeFamily().isTemporal()) {
            return descriptorType.convert(asTimestampWithTimezone(boundary, DEFAULT_ZONE));
        } else {
            return descriptorType.convert(boundary);
        }
    }

    private static JetSqlRow addWindowBoundsSingleRow(JetSqlRow row, Object timeStamp, long windowStart, long windowEnd) {
        Object[] result = Arrays.copyOf(row.getValues(), row.getFieldCount() + 2);
        if (timeStamp instanceof Byte) {
            result[result.length - 2] = (byte) windowStart;
            result[result.length - 1] = (byte) windowEnd;
        } else if (timeStamp instanceof Short) {
            result[result.length - 2] = (short) windowStart;
            result[result.length - 1] = (short) windowEnd;
        } else if (timeStamp instanceof Integer) {
            result[result.length - 2] = (int) windowStart;
            result[result.length - 1] = (int) windowEnd;
        } else if (timeStamp instanceof Long) {
            result[result.length - 2] = windowStart;
            result[result.length - 1] = windowEnd;
        } else if (timeStamp instanceof LocalTime) {
            result[result.length - 2] = asTimestampWithTimezone(windowStart, DEFAULT_ZONE).toLocalTime();
            result[result.length - 1] = asTimestampWithTimezone(windowEnd, DEFAULT_ZONE).toLocalTime();
        } else if (timeStamp instanceof LocalDate) {
            result[result.length - 2] = asTimestampWithTimezone(windowStart, DEFAULT_ZONE).toLocalDate();
            result[result.length - 1] = asTimestampWithTimezone(windowEnd, DEFAULT_ZONE).toLocalDate();
        } else if (timeStamp instanceof LocalDateTime) {
            result[result.length - 2] = asTimestampWithTimezone(windowStart, DEFAULT_ZONE).toLocalDateTime();
            result[result.length - 1] = asTimestampWithTimezone(windowEnd, DEFAULT_ZONE).toLocalDateTime();
        } else {
            result[result.length - 2] = asTimestampWithTimezone(windowStart, DEFAULT_ZONE);
            result[result.length - 1] = asTimestampWithTimezone(windowEnd, DEFAULT_ZONE);
        }
        return new JetSqlRow(row.getSerializationService(), result);
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
        } else if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value).toInstant().toEpochMilli();
        } else {
            return value.hashCode();
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
