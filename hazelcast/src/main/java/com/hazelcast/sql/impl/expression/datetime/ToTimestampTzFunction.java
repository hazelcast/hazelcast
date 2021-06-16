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
import com.hazelcast.sql.impl.expression.UniExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

public final class ToTimestampTzFunction extends UniExpression<OffsetDateTime> implements IdentifiedDataSerializable {
    private static final long MILLISECONDS_IN_YEAR = 31536000000L;
    private static final long MICROSECONDS_IN_YEAR = MILLISECONDS_IN_YEAR * 1000L;
    private static final long NANOSECONDS_IN_YEAR = MICROSECONDS_IN_YEAR * 1000L;

    public ToTimestampTzFunction() { }

    private ToTimestampTzFunction(Expression<?> operand) {
        super(operand);
    }

    public static ToTimestampTzFunction create(Expression<?> operand) {
        return new ToTimestampTzFunction(operand);
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
        final Long unixTimestamp = (Long) this.operand.eval(row, context);
        if (unixTimestamp == null) {
            return null;
        }

        final TemporalUnit unit = getChronoUnit(unixTimestamp);
        final Instant instant = Instant.EPOCH.plus(unixTimestamp, unit);

        return OffsetDateTime.from(instant.atZone(ZoneOffset.systemDefault()));
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;
    }

    /**
     * Returns TemporalUnit of the given unixTimestamp, using the magnitude of the value to determine most likely unit.
     * - unixTimestamp < MILLISECONDS_IN_YEAR (365 * 86400 * 1000) - timestamp is in Seconds
     * - unixTimestamp >= MILLISECONDS_IN_YEAR (365 * 86400 * 1000) - timestamp in in Milliseconds
     * - unixTimestamp >= MICROSECONDS_IN_YEAR (365 * 86400 * 1000_000) - timestamp is in Microseconds
     * - unixTimestamp >= NANOSECONDS_IN_YEAR (365 * 86400 * 1000_000_000) - timestamp is in Nanoseconds
     * - unixTimestamp < 0 - timestamp is in Seconds
     *
     * Note that this also imposes limits on the possible max correctly interpreted timestamp value,
     * that would be otherwise correctly interpreted as in Seconds/Milliseconds/Microseconds.
     * For example if this function is called on a unixTimestamp of 31_536_000_001 which in seconds
     * corresponds to datetime of 2969-05-03 00:00:01, this function would instead interpret it
     * as 1971-01-01 00:00:00.001. This is also true for maximum date expressed in milliseconds and microseconds.
     * Additionally this of course also imposes the min date possible to be interpreted in Milliseconds,
     * Microseconds and Nanoseconds.
     *
     * Summary of Date limits (under which the interpretation of the timestamp's unit is correct):
     * 1. Seconds:      from 1970-01-01 00:00:00Z (0)
     *                  to   2969-05-02 23:59:59Z (MILLISECONDS_IN_YEAR - 1)
     * 2. Milliseconds: from 1971-01-01 00:00:00.0Z (MILLISECONDS_IN_YEAR)
     *                  to   2969-05-02 23:59:59.999Z (MICROSECONDS_IN_YEAR - 1)
     * 3. Microseconds: from 1971-01-01 00:00:00.0Z (MICROSECONDS_IN_YEAR)
     *                  to   2969-05-02 23:59:59.999999Z (NANOSECONDS_IN_YEAR - 1)
     * 4. Nanoseconds:  from 1971-01-01 00:00:00.0Z (NANOSECONDS_IN_YEAR)
     *                  to   2262-04-11 23:47:16.854775807Z (Long.MAX_VALUE of nanoseconds).
     *
     * @param unixTimestamp - input timestamp
     * @return - determined {@link TemporalUnit} of the value, inferred based on the magnitude
     * of the input unixTimestamp
     */
    private TemporalUnit getChronoUnit(final long unixTimestamp) {
        if (unixTimestamp < MILLISECONDS_IN_YEAR) {
            return ChronoUnit.SECONDS;
        } else if (unixTimestamp < MICROSECONDS_IN_YEAR) {
            return ChronoUnit.MILLIS;
        } else if (unixTimestamp < NANOSECONDS_IN_YEAR) {
            return ChronoUnit.MICROS;
        } else {
            return ChronoUnit.NANOS;
        }
    }
}
