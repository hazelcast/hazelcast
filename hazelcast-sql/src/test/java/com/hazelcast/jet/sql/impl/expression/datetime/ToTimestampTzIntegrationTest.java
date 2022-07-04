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

package com.hazelcast.jet.sql.impl.expression.datetime;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

import static com.hazelcast.sql.SqlColumnType.BIGINT;
import static com.hazelcast.sql.SqlColumnType.BOOLEAN;
import static com.hazelcast.sql.SqlColumnType.DATE;
import static com.hazelcast.sql.SqlColumnType.DECIMAL;
import static com.hazelcast.sql.SqlColumnType.DOUBLE;
import static com.hazelcast.sql.SqlColumnType.OBJECT;
import static com.hazelcast.sql.SqlColumnType.REAL;
import static com.hazelcast.sql.SqlColumnType.TIME;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ToTimestampTzIntegrationTest extends ExpressionTestSupport {
    private static final Long SECONDS = 31536001L;
    private static final Long MILLISECONDS = SECONDS * 1000L;
    private static final Long MICROSECONDS = MILLISECONDS * 1000L;
    private static final Long NANOSECONDS = MICROSECONDS * 1000L;
    private static final Long MIN_NEGATIVE_SECONDS = LocalDateTime
            .of(-999999999, 1, 1, 0, 0)
            .atZone(ZoneOffset.systemDefault())
            .toEpochSecond();

    @Test
    public void testColumn() {
        checkColumn((byte) 1, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(1L, ChronoUnit.SECONDS));
        checkColumn((short) 1, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(1L, ChronoUnit.SECONDS));
        checkColumn(1, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(1L, ChronoUnit.SECONDS));
        checkColumn(1L, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(1L, ChronoUnit.SECONDS));
        checkColumn(SECONDS, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(SECONDS, ChronoUnit.SECONDS));
        checkColumn(MILLISECONDS, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(MILLISECONDS, ChronoUnit.MILLIS));
        checkColumn(MICROSECONDS, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(MICROSECONDS, ChronoUnit.MICROS));
        checkColumn(NANOSECONDS, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(NANOSECONDS, ChronoUnit.NANOS));
        checkColumn(Long.MAX_VALUE, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(Long.MAX_VALUE, ChronoUnit.NANOS));
        checkColumn(-MILLISECONDS, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(-MILLISECONDS, ChronoUnit.SECONDS));
        checkColumn(-MICROSECONDS, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(-MICROSECONDS, ChronoUnit.SECONDS));
        checkColumn(-NANOSECONDS, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(-NANOSECONDS, ChronoUnit.SECONDS));
        checkColumn(MIN_NEGATIVE_SECONDS, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(MIN_NEGATIVE_SECONDS, ChronoUnit.SECONDS));

        checkColumnFailure("'1'", SqlErrorCode.PARSING, signatureError(SqlColumnType.VARCHAR));
        checkColumnFailure(BigDecimal.valueOf(1.0), SqlErrorCode.PARSING, signatureError(SqlColumnType.DECIMAL));
        checkColumnFailure(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE), SqlErrorCode.PARSING, signatureError(SqlColumnType.DECIMAL));
        checkColumnFailure(BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE), SqlErrorCode.PARSING, signatureError(SqlColumnType.DECIMAL));
        checkColumnFailure(1.0d, SqlErrorCode.PARSING, signatureError(SqlColumnType.DOUBLE));
        checkColumnFailure(1.0f, SqlErrorCode.PARSING, signatureError(SqlColumnType.REAL));
        checkColumnFailure(true, SqlErrorCode.PARSING, signatureError(SqlColumnType.BOOLEAN));
        checkColumnFailure(LOCAL_DATE_TIME_VAL, SqlErrorCode.PARSING, signatureError(TIMESTAMP));
        checkColumnFailure(OFFSET_DATE_TIME_VAL, SqlErrorCode.PARSING, signatureError(TIMESTAMP_WITH_TIME_ZONE));
        checkColumnFailure(LOCAL_DATE_VAL, SqlErrorCode.PARSING, signatureError(DATE));
        checkColumnFailure(LOCAL_TIME_VAL, SqlErrorCode.PARSING, signatureError(TIME));
        checkColumnFailure(OBJECT_VAL, SqlErrorCode.PARSING, signatureError(OBJECT));
    }

    @Test
    public void testNull() {
        put(0);
        check("null", TIMESTAMP_WITH_TIME_ZONE, null);
    }

    @Test
    public void testLiteral() {
        put(0);
        checkLiteral(1L, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(1L, ChronoUnit.SECONDS));
        checkLiteral(SECONDS, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(SECONDS, ChronoUnit.SECONDS));
        checkLiteral(MILLISECONDS, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(MILLISECONDS, ChronoUnit.MILLIS));
        checkLiteral(MICROSECONDS, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(MICROSECONDS, ChronoUnit.MICROS));
        checkLiteral(NANOSECONDS, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(NANOSECONDS, ChronoUnit.NANOS));
        checkLiteral("-10000", TIMESTAMP_WITH_TIME_ZONE, fromEpoch(-10000L, ChronoUnit.SECONDS));
        checkLiteral("0", TIMESTAMP_WITH_TIME_ZONE, fromEpoch(0L, ChronoUnit.SECONDS));
        checkLiteral("9223372036854775807", TIMESTAMP_WITH_TIME_ZONE, fromEpoch(9223372036854775807L, ChronoUnit.NANOS));
        checkLiteral("null", TIMESTAMP_WITH_TIME_ZONE, null);
        checkLiteral(-MILLISECONDS, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(-MILLISECONDS, ChronoUnit.SECONDS));
        checkLiteral(-MICROSECONDS, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(-MICROSECONDS, ChronoUnit.SECONDS));
        checkLiteral(-NANOSECONDS, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(-NANOSECONDS, ChronoUnit.SECONDS));
        checkLiteral(MIN_NEGATIVE_SECONDS, TIMESTAMP_WITH_TIME_ZONE, fromEpoch(MIN_NEGATIVE_SECONDS, ChronoUnit.SECONDS));

        checkFailure("'1'", SqlErrorCode.PARSING, signatureError(SqlColumnType.VARCHAR));
        checkFailure("1.0", SqlErrorCode.PARSING, signatureError(SqlColumnType.DECIMAL));
        checkFailure("9223372036854775808", SqlErrorCode.PARSING, signatureError(SqlColumnType.DECIMAL));
        checkFailure("-9223372036854775809", SqlErrorCode.PARSING, signatureError(SqlColumnType.DECIMAL));
    }

    @Test
    public void testParameter() {
        put(0);
        checkParameter(1L, fromEpoch(1L, ChronoUnit.SECONDS));
        checkParameter(SECONDS, fromEpoch(SECONDS, ChronoUnit.SECONDS));
        checkParameter(MILLISECONDS, fromEpoch(MILLISECONDS, ChronoUnit.MILLIS));
        checkParameter(MICROSECONDS, fromEpoch(MICROSECONDS, ChronoUnit.MICROS));
        checkParameter(NANOSECONDS, fromEpoch(NANOSECONDS, ChronoUnit.NANOS));
        checkParameter(Long.MAX_VALUE, fromEpoch(Long.MAX_VALUE, ChronoUnit.NANOS));
        checkParameter(null, null);
        checkParameter(-MILLISECONDS, fromEpoch(-MILLISECONDS, ChronoUnit.SECONDS));
        checkParameter(-MICROSECONDS, fromEpoch(-MICROSECONDS, ChronoUnit.SECONDS));
        checkParameter(-NANOSECONDS, fromEpoch(-NANOSECONDS, ChronoUnit.SECONDS));
        checkParameter(MIN_NEGATIVE_SECONDS, fromEpoch(MIN_NEGATIVE_SECONDS, ChronoUnit.SECONDS));

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), "foo");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), true);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BigInteger.ZERO);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BigDecimal.ZERO);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, REAL), 0.0f);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, DOUBLE), 0.0d);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, DATE), LOCAL_DATE_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, TIME), LOCAL_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, OBJECT), OBJECT_VAL);
    }

    private OffsetDateTime fromEpoch(final Long timestamp, final TemporalUnit unit) {
        final Instant instant = Instant.EPOCH.plus(timestamp, unit);
        return OffsetDateTime.from(instant.atZone(ZoneOffset.systemDefault()));
    }

    private void checkColumn(Object value, SqlColumnType expectedType, Object expectedResult) {
        put(value);
        check("this", expectedType, expectedResult);
    }

    private void checkParameter(Object parameterValue, Object expectedValue) {
        check("?", TIMESTAMP_WITH_TIME_ZONE, expectedValue, parameterValue);
    }

    private void checkLiteral(Object literal, SqlColumnType expectedType, Object expectedValue) {
        check(literal.toString(), expectedType, expectedValue);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT TO_TIMESTAMP_TZ(" + operand + ") FROM map";

        checkFailure0(sql, expectedErrorCode, expectedErrorMessage, params);
    }

    private void checkColumnFailure(Object value, int expectedErrorCode, String expectedErrorMessage) {
        put(value);
        checkFailure("this", expectedErrorCode, expectedErrorMessage);
    }

    private void check(Object operand, SqlColumnType expectedType, Object expectedValue, Object... params) {
        final String sql = "SELECT TO_TIMESTAMP_TZ(" + operand + ") FROM map";
        checkValue0(sql, expectedType, expectedValue, params);
    }

    private static String signatureError(SqlColumnType type) {
        return signatureErrorFunction("TO_TIMESTAMP_TZ", type);
    }
}
