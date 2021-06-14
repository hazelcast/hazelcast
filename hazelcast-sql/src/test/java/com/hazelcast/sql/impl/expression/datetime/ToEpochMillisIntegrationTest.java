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

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

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
public class ToEpochMillisIntegrationTest extends ExpressionTestSupport {

    @Test
    public void testColumn() {
        checkColumn(fromEpochMillis(1L), SqlColumnType.BIGINT, 1L);
        checkColumn(fromEpochMillis(0L), SqlColumnType.BIGINT, 0L);
        checkColumn(fromEpochMillis(-1L), SqlColumnType.BIGINT, -1L);
        checkColumn(fromEpochMillis(1_000_000_000_000_000_000L), SqlColumnType.BIGINT, 1_000_000_000_000_000_000L);
        checkColumn(fromEpochMillis(-1_000_000_000_000_000_000L), SqlColumnType.BIGINT, -1_000_000_000_000_000_000L);

        checkColumnFailure("null", SqlErrorCode.PARSING, signatureError(SqlColumnType.VARCHAR));
        checkColumnFailure(1.0f, SqlErrorCode.PARSING, signatureError(SqlColumnType.REAL));
        checkColumnFailure(1.0d, SqlErrorCode.PARSING, signatureError(SqlColumnType.DOUBLE));
        checkColumnFailure(BigDecimal.valueOf(1L), SqlErrorCode.PARSING, signatureError(SqlColumnType.DECIMAL));
        checkColumnFailure(LocalDate.now(), SqlErrorCode.PARSING, signatureError(SqlColumnType.DATE));
        checkColumnFailure(LocalTime.now(), SqlErrorCode.PARSING, signatureError(SqlColumnType.TIME));
        checkColumnFailure(LocalDateTime.now(), SqlErrorCode.PARSING, signatureError(SqlColumnType.TIMESTAMP));
    }

    @Test
    public void testParameter() {
        put(0);
        checkParameter(fromEpochMillis(1L), 1L);
        checkParameter(fromEpochMillis(0L), 0L);
        checkParameter(fromEpochMillis(-1L), -1L);
        checkParameter(fromEpochMillis(1_000_000_000_000_000_000L), 1_000_000_000_000_000_000L);
        checkParameter(fromEpochMillis(-1_000_000_000_000_000_000L), -1_000_000_000_000_000_000L);

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, TIMESTAMP_WITH_TIME_ZONE, VARCHAR), "foo");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, TIMESTAMP_WITH_TIME_ZONE, BOOLEAN), true);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, TIMESTAMP_WITH_TIME_ZONE, DECIMAL), BigInteger.ZERO);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, TIMESTAMP_WITH_TIME_ZONE, DECIMAL), BigDecimal.ZERO);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, TIMESTAMP_WITH_TIME_ZONE, REAL), 0.0f);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, TIMESTAMP_WITH_TIME_ZONE, DOUBLE), 0.0d);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, TIMESTAMP_WITH_TIME_ZONE, DATE), LOCAL_DATE_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, TIMESTAMP_WITH_TIME_ZONE, TIME), LOCAL_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, TIMESTAMP_WITH_TIME_ZONE, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testLiteral() {
        put(0);
        checkLiteral("CAST('1970-01-01T00:00:00Z' AS TIMESTAMP WITH TIME ZONE)", BIGINT, 0L);
        checkLiteral("CAST('1970-01-01T00:00:01Z' AS TIMESTAMP WITH TIME ZONE)", BIGINT, 1000L);
        checkLiteral("CAST('1969-12-31T23:59:59Z' AS TIMESTAMP WITH TIME ZONE)", BIGINT, -1000L);
        checkLiteral("null", BIGINT, null);

        checkFailure("'1'", SqlErrorCode.PARSING, signatureError(VARCHAR));
        checkFailure(1.0f, SqlErrorCode.PARSING, signatureError(DECIMAL));
        checkFailure(1.0d, SqlErrorCode.PARSING, signatureError(DECIMAL));
        checkFailure("1.0", SqlErrorCode.PARSING, signatureError(DECIMAL));
        checkFailure("'1969-12-31T23:59:59Z'", SqlErrorCode.PARSING, signatureError(VARCHAR));
    }

    private OffsetDateTime fromEpochMillis(final Long timestamp) {
        final Instant instant = Instant.EPOCH.plus(timestamp, ChronoUnit.MILLIS);
        return OffsetDateTime.from(instant.atZone(ZoneOffset.systemDefault()));
    }

    private void checkColumn(Object value, SqlColumnType expectedType, Object expectedResult) {
        put(value);
        check("this", expectedType, expectedResult);
    }

    private void checkParameter(Object parameterValue, Object expectedValue) {
        check("?", BIGINT, expectedValue, parameterValue);
    }

    private void checkLiteral(Object literal, SqlColumnType expectedType, Object expectedValue) {
        check(literal.toString(), expectedType, expectedValue);
    }

    private void checkColumnFailure(Object value, int expectedErrorCode, String expectedErrorMessage) {
        put(value);
        checkFailure("this", expectedErrorCode, expectedErrorMessage);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT TO_EPOCH_MILLIS(" + operand + ") FROM map";
        checkFailure0(sql, expectedErrorCode, expectedErrorMessage, params);
    }

    private void check(Object operand, SqlColumnType expectedType, Object expectedValue, Object... params) {
        final String sql = "SELECT TO_EPOCH_MILLIS(" + operand + ") FROM map";
        checkValue0(sql, expectedType, expectedValue, params);
    }

    private static String signatureError(SqlColumnType type) {
        return signatureErrorFunction("TO_EPOCH_MILLIS", type);
    }
}
