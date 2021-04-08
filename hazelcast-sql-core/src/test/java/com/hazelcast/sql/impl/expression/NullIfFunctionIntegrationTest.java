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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlErrorCode;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;

public class NullIfFunctionIntegrationTest extends ExpressionTestSupport {
    @Test
    public void equalParameters() {
        put(1);

        checkValue0("select nullif(this, 1) from map", SqlColumnType.INTEGER, null);
    }

    @Test
    public void notEqualParameters() {
        put(1);

        checkValue0("select nullif(this, 2) from map", SqlColumnType.INTEGER, 1);
    }

    @Test
    public void numbersCoercion() {
        put(1);

        checkValue0("select nullif(this, 1) from map", SqlColumnType.INTEGER, null);
        checkValue0("select nullif(this, CAST(1 as SMALLINT)) from map", SqlColumnType.INTEGER, null);
        checkValue0("select nullif(this, CAST(1 as BIGINT)) from map", SqlColumnType.BIGINT, null);
        checkValue0("select nullif(this, CAST(1 as REAL)) from map", SqlColumnType.REAL, null);
        checkValue0("select nullif(this, CAST(1 as DOUBLE PRECISION)) from map", SqlColumnType.DOUBLE, null);
        checkValue0("select nullif(this, CAST(1 as DECIMAL)) from map", SqlColumnType.DECIMAL, null);
    }

    @Test
    public void dateTimeValuesAndLiterals() {
        LocalDate localDate = LocalDate.of(2021, 1, 1);
        put(localDate);
        checkValue0("select nullif(this, '2021-01-02') from map", SqlColumnType.DATE, localDate);

        LocalTime localTime = LocalTime.of(12, 0);
        put(localTime);
        checkValue0("select nullif(this, '13:00') from map", SqlColumnType.TIME, localTime);

        LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);
        put(localDateTime);
        checkValue0("select nullif(this, '2021-01-02T13:00') from map", SqlColumnType.TIMESTAMP, localDateTime);

        OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime, ZoneOffset.ofHours(2));
        put(offsetDateTime);
        checkValue0("select nullif(this, '2021-01-02T13:00+01:00') from map", SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, offsetDateTime);
    }

    @Test
    public void fail_whenCantInferNullIfParameterTypes() {
        put(1);
        checkFailure0("select nullif(?, ?) from map", SqlErrorCode.PARSING, "Cannot apply 'NULLIF' function to [UNKNOWN, UNKNOWN] (consider adding an explicit CAST)");
    }

    @Test
    public void nonCoercibleTypes() {
        put(1);
        checkFailure0(
                "select nullif(1, 'abc') from map",
                SqlErrorCode.PARSING,
                "Cannot apply 'NULLIF' function to [TINYINT, VARCHAR] (consider adding an explicit CAST)");
        checkFailure0(
                "select nullif('abc', CAST('2021-01-02' as DATE)) from map",
                SqlErrorCode.GENERIC,
                "while converting NULLIF(CAST('abc' AS DATE), CAST('2021-01-02' AS DATE))");
        checkFailure0(
                "select nullif('abc', CAST('13:00:00' as TIME)) from map",
                SqlErrorCode.GENERIC,
                "while converting NULLIF(CAST('abc' AS TIME(0)), CAST('13:00:00' AS TIME))");
        checkFailure0(
                "select nullif('abc', CAST('2021-01-02T13:00' as TIMESTAMP)) from map",
                SqlErrorCode.GENERIC,
                "while converting NULLIF(CAST('abc' AS TIMESTAMP(0)), CAST('2021-01-02T13:00' AS TIMESTAMP))");
        checkFailure0(
                "select nullif(1, CAST('2021-01-02' as DATE)) from map",
                SqlErrorCode.PARSING,
                "Cannot apply 'NULLIF' function to [TINYINT, DATE] (consider adding an explicit CAST)");
        checkFailure0(
                "select nullif(1, CAST('13:00:00' as TIME)) from map",
                SqlErrorCode.PARSING,
                "Cannot apply 'NULLIF' function to [TINYINT, TIME] (consider adding an explicit CAST)");
        checkFailure0(
                "select nullif(1, CAST('2021-01-02T13:00' as TIMESTAMP)) from map",
                SqlErrorCode.PARSING,
                "Cannot apply 'NULLIF' function to [TINYINT, TIMESTAMP] (consider adding an explicit CAST)");
    }

    @Test
    public void testEquality() {
        checkEquals(
                NullIfExpression.create(ConstantExpression.create(1, INT), ConstantExpression.create(1, INT)),
                NullIfExpression.create(ConstantExpression.create(1, INT), ConstantExpression.create(1, INT)),
                true);

        checkEquals(
                NullIfExpression.create(ConstantExpression.create(1, INT), ConstantExpression.create(1, INT)),
                NullIfExpression.create(ConstantExpression.create(1, INT), ConstantExpression.create(10, INT)),
                false);
    }

    @Test
    public void testSerialization() {
        NullIfExpression<?> original = NullIfExpression.create(ConstantExpression.create(1, INT), ConstantExpression.create(1, INT));
        NullIfExpression<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_NULLIF);

        checkEquals(original, restored, true);
    }
}
