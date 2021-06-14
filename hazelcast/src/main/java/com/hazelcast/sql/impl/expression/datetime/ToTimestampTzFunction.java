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

package com.hazelcast.sql.impl.expression.datetime;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

public final class ToTimestampTzFunction extends UniExpressionWithType<OffsetDateTime> implements IdentifiedDataSerializable {
    private static final long YEAR_MILLISECONDS = 31536000000L;
    private static final long YEAR_MICROSECONDS = YEAR_MILLISECONDS * 1000L;
    private static final long YEAR_NANOSECONDS = YEAR_MICROSECONDS * 1000L;

    public ToTimestampTzFunction() { }

    private ToTimestampTzFunction(Expression<?> operand, QueryDataType resultType) {
        super(operand, resultType);
    }

    public static ToTimestampTzFunction create(Expression<?> operand, QueryDataType resultType) {
        return new ToTimestampTzFunction(operand, resultType);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_TO_TIMESTAMP_TZ;
    }

    @Override
    public OffsetDateTime eval(final Row row, final ExpressionEvalContext context) {
        final Number value = (Number) this.operand.eval(row, context);
        if (value == null) {
            return null;
        }

        final long unixTimestamp = value.longValue();
        final TemporalUnit unit = getChronoUnit(unixTimestamp);
        final Instant instant = Instant.EPOCH.plus(unixTimestamp, unit);

        return OffsetDateTime.from(instant.atZone(ZoneOffset.systemDefault()));
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;
    }

    private TemporalUnit getChronoUnit(final long unixTimestamp) {
        if (unixTimestamp < YEAR_MILLISECONDS) {
            return ChronoUnit.SECONDS;
        } else if (unixTimestamp < YEAR_MICROSECONDS) {
            return ChronoUnit.MILLIS;
        } else if (unixTimestamp < YEAR_NANOSECONDS) {
            return ChronoUnit.MICROS;
        } else {
            return ChronoUnit.NANOS;
        }
    }
}
