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

package com.hazelcast.jet.sql.impl.expression.misc;

import com.hazelcast.sql.impl.expression.CaseExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
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

public class CoalesceFunctionIntegrationTest extends ExpressionTestSupport {
    @Test
    public void coalesce() {
        put(1);

        checkValue0("select coalesce(null) from map", SqlColumnType.NULL, null);
        checkValue0("select coalesce(null, null) from map", SqlColumnType.NULL, null);
        checkValue0("select coalesce(this, 2) from map", SqlColumnType.INTEGER, 1);
        checkValue0("select coalesce(null, this, 2) from map", SqlColumnType.INTEGER, 1);
        checkValue0("select coalesce(null, 2, this) from map", SqlColumnType.INTEGER, 2);
        checkValue0("select coalesce(CAST(null as INT), null) from map", SqlColumnType.INTEGER, null);
        checkValue0("select coalesce(CAST(null as INT)) from map", SqlColumnType.INTEGER, null);
    }

    @Test
    public void fails_whenReturnTypeCantBeInferred() {
        put(1);
        checkFailure0("select coalesce() from map", SqlErrorCode.PARSING, "Invalid number of arguments to function 'COALESCE'. Was expecting 1 arguments");
        checkFailure0("select coalesce(?) from map", SqlErrorCode.PARSING, "Cannot apply 'COALESCE' function to [UNKNOWN] (consider adding an explicit CAST)");
        checkFailure0("select coalesce(?, ?) from map", SqlErrorCode.PARSING, "Cannot apply 'COALESCE' function to [UNKNOWN, UNKNOWN] (consider adding an explicit CAST)");
    }

    @Test
    public void numbersCoercion() {
        put(1);

        checkValue0("select coalesce(this, CAST(null as BIGINT)) from map", SqlColumnType.BIGINT, 1L);
        checkValue0("select coalesce(null, this, 1234567890123) from map", SqlColumnType.BIGINT, 1L);
    }

    @Test
    public void dateTimeValuesAndLiterals() {
        LocalDate localDate = LocalDate.of(2021, 1, 1);
        put(localDate);
        checkValue0("select coalesce(this, '2021-01-02') from map", SqlColumnType.DATE, localDate);

        LocalTime localTime = LocalTime.of(12, 0);
        put(localTime);
        checkValue0("select coalesce(this, '13:00') from map", SqlColumnType.TIME, localTime);

        LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);
        put(localDateTime);
        checkValue0("select coalesce(this, '2021-01-02T13:00') from map", SqlColumnType.TIMESTAMP, localDateTime);

        OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime, ZoneOffset.ofHours(2));
        put(offsetDateTime);
        checkValue0("select coalesce(this, '2021-01-02T13:00+01:00') from map", SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, offsetDateTime);

        checkValue0("select coalesce(this, CAST('2021-01-02' as DATE), CAST('2021-01-02T13:00' as TIMESTAMP), '2021-01-02T13:00+01:00') from map", SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, offsetDateTime);

        checkValue0("select CAST('13:00:00' as TIME) from map", SqlColumnType.TIME, localTime.plusHours(1));
        checkValue0("select coalesce(this, CAST(? as TIMESTAMP), CAST('2021-01-02T13:00' as TIMESTAMP), '2021-01-02T13:00+01:00') from map", SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, offsetDateTime, localTime);
    }

    @Test
    public void nonCoercibleTypes() {
        put(1);
        checkFailure0("select coalesce(1, 'abc') from map", SqlErrorCode.PARSING, "Cannot infer return type for COALESCE among [TINYINT, VARCHAR]");
        checkFailure0("select coalesce('abc', CAST('2021-01-02' as DATE)) from map", SqlErrorCode.PARSING, "CAST function cannot convert literal 'abc' to type DATE: Cannot parse VARCHAR value to DATE");
        checkFailure0("select coalesce('abc', CAST('13:00:00' as TIME)) from map", SqlErrorCode.PARSING, "CAST function cannot convert literal 'abc' to type TIME: Cannot parse VARCHAR value to TIME");
        checkFailure0("select coalesce('abc', CAST('2021-01-02T13:00' as TIMESTAMP)) from map", SqlErrorCode.PARSING, "CAST function cannot convert literal 'abc' to type TIMESTAMP: Cannot parse VARCHAR value to TIMESTAMP");
        checkFailure0("select coalesce(1, CAST('2021-01-02' as DATE)) from map", SqlErrorCode.PARSING, "Cannot infer return type for COALESCE among [TINYINT, DATE]");
        checkFailure0("select coalesce(1, CAST('13:00:00' as TIME)) from map", SqlErrorCode.PARSING, "Cannot infer return type for COALESCE among [TINYINT, TIME]");
        checkFailure0("select coalesce(1, CAST('2021-01-02T13:00' as TIMESTAMP)) from map", SqlErrorCode.PARSING, "Cannot infer return type for COALESCE among [TINYINT, TIMESTAMP]");
    }

    @Test
    public void testEquality() {
        checkEquals(
                CaseExpression.coalesce(ConstantExpression.create(1, INT), ConstantExpression.create(1, INT)),
                CaseExpression.coalesce(ConstantExpression.create(1, INT), ConstantExpression.create(1, INT)),
                true);

        checkEquals(
                CaseExpression.coalesce(ConstantExpression.create(1, INT), ConstantExpression.create(1, INT)),
                CaseExpression.coalesce(ConstantExpression.create(1, INT), ConstantExpression.create(10, INT)),
                false);
    }

    @Test
    public void testSerialization() {
        CaseExpression<?> original = CaseExpression.coalesce(ConstantExpression.create(1, INT), ConstantExpression.create(1, INT));
        CaseExpression<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_CASE);

        checkEquals(original, restored, true);
    }
}
