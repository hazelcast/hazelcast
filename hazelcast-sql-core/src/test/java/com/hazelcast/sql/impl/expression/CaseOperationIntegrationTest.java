/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.support.expressions.ExpressionBiValue;
import com.hazelcast.sql.support.expressions.ExpressionType;
import com.hazelcast.sql.support.expressions.ExpressionTypes;
import org.junit.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;

public class CaseOperationIntegrationTest extends ExpressionTestSupport {
    @Test
    public void caseWithConstants() {
        put(1);

        checkValue0("select case when true then null else null end from map", SqlColumnType.NULL, null);
        checkFailure0("select case when 1 then 1 else 2 end from map", SqlErrorCode.PARSING, "Expected a boolean type");
        checkValue0("select case when 1 = 1 then 1 else null end from map", SqlColumnType.TINYINT, (byte) 1);
        checkValue0("select case when 1 = 1 then null else 1 end from map", SqlColumnType.TINYINT, null);
        checkValue0("select case 1 when 1 then 100 else 2 end from map", SqlColumnType.TINYINT, (byte) 100);
        checkFailure0("select case 'a' when 1 then 100 else 2 end from map", SqlErrorCode.PARSING, "Cannot apply '=' operator to [VARCHAR, TINYINT]");
    }

    @Test
    public void testOnlyElse() {
        put(1);

        String sql = "select case else 2 end from map";

        checkFailure0(sql, SqlErrorCode.PARSING, "Encountered \"case else\"");
    }

    @Test
    public void useMapValue() {
        put(1);

        checkValue0("select case when this = 1 then 10 end from map", SqlColumnType.TINYINT, (byte) 10);
    }

    @Test
    public void multipleConditions() {
        put(1);

        String sql = "select case when this > 1 then 10 when this = 1 then 100 end from map";

        checkValue0(sql, SqlColumnType.TINYINT, (byte) 100);
    }

    @Test
    public void doesntMatchWhen_andNoElseBranch() {
        put(1);

        String sql = "select case when this > 1 then 10 end from map";

        checkValue0(sql, SqlColumnType.TINYINT, null);
    }

    @Test
    public void differentReturnTypes() {
        put(1);

        checkFailure0(
                "select case this when 2 then 100 when 1 then 'a' end from map",
                SqlErrorCode.PARSING,
                "Cannot infer return type for CASE among [TINYINT, VARCHAR, NULL]");
        checkFailure0(
                "select case when 1 = 1 then 1 when 2 = 2 then 1000000000 else 'some string' end from map",
                SqlErrorCode.PARSING,
                "Cannot infer return type for CASE among [TINYINT, INTEGER, VARCHAR]");
    }

    @Test
    public void withParameterAsValue() {
        put(1);

        checkValue0("select ? = this from map", SqlColumnType.BOOLEAN, true, 1L);

        checkFailure0("select case when ? = ? then 1 end from map", SqlErrorCode.PARSING, "Cannot apply '=' operator to [UNKNOWN, UNKNOWN]");
        checkFailure0("select case ? when ? then 100 end from map", SqlErrorCode.PARSING, "Cannot apply '=' operator to [UNKNOWN, UNKNOWN]");
        checkValue0("select case when ? then 1 end from map", SqlColumnType.TINYINT, (byte) 1, true);
        checkValue0("select case when ? IS NOT NULL then 100 end from map", SqlColumnType.TINYINT, (byte) 100, 1);
        checkValue0("select case ? when this then 100 end from map", SqlColumnType.TINYINT, (byte) 100, 1);
        checkValue0("select case when ? = this then 100 end from map", SqlColumnType.TINYINT, (byte) 100, 1);
    }

    @Test
    public void numericWithString() {
        String sql = "select case when 1 = 1 then field1 else field2 end from map";

        putBiValue((byte) 1, "str", ExpressionTypes.BYTE, ExpressionTypes.STRING);
        checkFailure0(sql, SqlErrorCode.PARSING, "Cannot infer return type for CASE among [TINYINT, VARCHAR]");

        putBiValue((short) 1, "str", ExpressionTypes.SHORT, ExpressionTypes.STRING);
        checkFailure0(sql, SqlErrorCode.PARSING, "Cannot infer return type for CASE among [SMALLINT, VARCHAR]");

        putBiValue(1, "str", ExpressionTypes.INTEGER, ExpressionTypes.STRING);
        checkFailure0(sql, SqlErrorCode.PARSING, "Cannot infer return type for CASE among [INTEGER, VARCHAR]");

        putBiValue(1L, "str", ExpressionTypes.LONG, ExpressionTypes.STRING);
        checkFailure0(sql, SqlErrorCode.PARSING, "Cannot infer return type for CASE among [BIGINT, VARCHAR]");

        putBiValue(BigDecimal.ONE, "str", ExpressionTypes.BIG_DECIMAL, ExpressionTypes.STRING);
        checkFailure0(sql, SqlErrorCode.PARSING, "Cannot infer return type for CASE among [DECIMAL(38, 38), VARCHAR]");

        putBiValue(BigInteger.ONE, "str", ExpressionTypes.BIG_INTEGER, ExpressionTypes.STRING);
        checkFailure0(sql, SqlErrorCode.PARSING, "Cannot infer return type for CASE among [DECIMAL(38, 38), VARCHAR]");

        putBiValue(1.0, "str", ExpressionTypes.DOUBLE, ExpressionTypes.STRING);
        checkFailure0(sql, SqlErrorCode.PARSING, "Cannot infer return type for CASE among [DOUBLE, VARCHAR]");

        putBiValue(1.0f, "str", ExpressionTypes.FLOAT, ExpressionTypes.STRING);
        checkFailure0(sql, SqlErrorCode.PARSING, "Cannot infer return type for CASE among [REAL, VARCHAR]");
    }

    @Test
    public void parameterWithString() {
        String sql = "select case when 1 = 1 then ? else this end from map";
        put("str");

        checkFailure0(sql, SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type, but TINYINT was found", (byte) 1);
        checkFailure0(sql, SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type, but SMALLINT was found", (short) 1);
        checkFailure0(sql, SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type, but INTEGER was found", 1);
        checkFailure0(sql, SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type, but BIGINT was found", 1L);
        checkFailure0(sql, SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type, but DECIMAL was found", BigDecimal.ONE);
        checkFailure0(sql, SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type, but DECIMAL was found", BigInteger.ONE);
        checkFailure0(sql, SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type, but DOUBLE was found", 1.0);
        checkFailure0(sql, SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type, but REAL was found", 1.0f);
    }

    @Test
    public void dateTimeLiterals() {
        String sql = "select case\n"
                + "        when true\n"
                + "            then this\n"
                + "            else '%s'\n"
                + "        end\n"
                + "from map";

        LocalDate date = LocalDate.now();
        put(date);
        checkValue0(String.format(sql, date.minusDays(1)), SqlColumnType.DATE, date);

        LocalTime time = LocalTime.now();
        put(time);
        checkValue0(String.format(sql, time.minusHours(1)), SqlColumnType.TIME, time);

        LocalDateTime dateTime = LocalDateTime.now();
        put(dateTime);
        checkValue0(String.format(sql, dateTime.minusHours(1)), SqlColumnType.TIMESTAMP, dateTime);
    }

    @Test
    public void date_time_typeConversion() {
        String sql = "select case when 1 = 1 then field1 else field2 end from map";

        putBiValue(
                LocalDate.of(2020, 12, 30),
                LocalTime.of(14, 2, 0),
                ExpressionTypes.LOCAL_DATE, ExpressionTypes.LOCAL_TIME
        );
        checkFailure0(sql, SqlErrorCode.PARSING, "Cannot infer return type for CASE among [DATE, TIME]");

        putBiValue(
                LocalTime.of(14, 2, 0),
                LocalDate.of(2020, 12, 30),
                ExpressionTypes.LOCAL_TIME, ExpressionTypes.LOCAL_DATE
        );
        checkFailure0(sql, SqlErrorCode.PARSING, "Cannot infer return type for CASE among [TIME, DATE]");

        putBiValue(
                LocalDateTime.of(2020, 12, 30, 14, 2, 0),
                LocalTime.of(14, 2, 0),
                ExpressionTypes.LOCAL_DATE_TIME, ExpressionTypes.LOCAL_TIME
        );
        checkValue0(sql, SqlColumnType.TIMESTAMP, LocalDateTime.of(2020, 12, 30, 14, 2, 0));

        putBiValue(
                LocalTime.of(14, 2, 0),
                LocalDateTime.of(2020, 12, 30, 14, 2, 0),
                ExpressionTypes.LOCAL_TIME, ExpressionTypes.LOCAL_DATE_TIME
        );
        checkValue0(sql, SqlColumnType.TIMESTAMP, LocalDateTime.of(LocalDate.now(), LocalTime.of(14, 2, 0)));

        putBiValue(
                LocalDate.of(2020, 12, 30),
                LocalDateTime.of(2020, 12, 30, 14, 2, 0),
                ExpressionTypes.LOCAL_DATE, ExpressionTypes.LOCAL_DATE_TIME
        );
        checkValue0(sql, SqlColumnType.TIMESTAMP, LocalDate.of(2020, 12, 30).atStartOfDay());

        putBiValue(
                LocalDateTime.of(2020, 12, 30, 14, 2, 0),
                LocalDate.of(2020, 12, 30),
                ExpressionTypes.LOCAL_DATE_TIME, ExpressionTypes.LOCAL_DATE
        );
        checkValue0(sql, SqlColumnType.TIMESTAMP, LocalDateTime.of(2020, 12, 30, 14, 2, 0));

        putBiValue(
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC),
                LocalTime.of(14, 2, 0),
                ExpressionTypes.OFFSET_DATE_TIME, ExpressionTypes.LOCAL_TIME
        );
        checkValue0(
                sql, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC));

        putBiValue(
                LocalTime.of(14, 2, 0),
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC),
                ExpressionTypes.LOCAL_TIME, ExpressionTypes.OFFSET_DATE_TIME
        );
        checkValue0(
                sql, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                ZonedDateTime.of(LocalDateTime.of(LocalDate.now(), LocalTime.of(14, 2, 0)),
                        ZoneId.systemDefault()).toOffsetDateTime());

        putBiValue(
                LocalDateTime.of(2020, 12, 30, 14, 2, 0),
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC),
                ExpressionTypes.LOCAL_DATE_TIME, ExpressionTypes.OFFSET_DATE_TIME
        );
        checkValue0(sql, SqlColumnType.TIMESTAMP, LocalDateTime.of(2020, 12, 30, 14, 2, 0));

        putBiValue(
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC),
                LocalDateTime.of(2020, 12, 30, 14, 2, 0),
                ExpressionTypes.OFFSET_DATE_TIME, ExpressionTypes.LOCAL_DATE_TIME
        );
        checkValue0(
                sql, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC));
    }

    @Test
    public void numericWithObject() {
        String sql = "select case when 1 = 1 then field1 else field2 end from map";
        SerializableDummy field2 = new SerializableDummy();

        putBiValue((byte) 1, field2, ExpressionTypes.BYTE, ExpressionTypes.OBJECT);
        checkValue0(sql, SqlColumnType.OBJECT, (byte) 1);

        putBiValue((short) 1, field2, ExpressionTypes.SHORT, ExpressionTypes.OBJECT);
        checkValue0(sql, SqlColumnType.OBJECT, (short) 1);

        putBiValue(1, field2, ExpressionTypes.INTEGER, ExpressionTypes.OBJECT);
        checkValue0(sql, SqlColumnType.OBJECT, 1);

        putBiValue(1L, field2, ExpressionTypes.LONG, ExpressionTypes.OBJECT);
        checkValue0(sql, SqlColumnType.OBJECT, 1L);

        putBiValue(BigDecimal.ONE, field2, ExpressionTypes.BIG_DECIMAL, ExpressionTypes.OBJECT);
        checkValue0(sql, SqlColumnType.OBJECT, BigDecimal.ONE);

        putBiValue(BigInteger.ONE, field2, ExpressionTypes.BIG_INTEGER, ExpressionTypes.OBJECT);
        checkValue0(sql, SqlColumnType.OBJECT, BigDecimal.ONE);

        putBiValue(1.0, field2, ExpressionTypes.DOUBLE, ExpressionTypes.OBJECT);
        checkValue0(sql, SqlColumnType.OBJECT, 1.0);

        putBiValue(1.0f, field2, ExpressionTypes.FLOAT, ExpressionTypes.OBJECT);
        checkValue0(sql, SqlColumnType.OBJECT, 1.0f);
    }

    private static class SerializableDummy implements Serializable {
    }

    private void putBiValue(Object field1, Object field2, ExpressionType<?> type1, ExpressionType<?> type2) {
        ExpressionBiValue value = ExpressionBiValue.createBiValue(
                ExpressionBiValue.createBiClass(type1, type2),
                field1,
                field2
        );

        put(value);
    }

    @Test
    public void testEquality() {
        checkEquals(
                when1eq1_then1_else10(),
                when1eq1_then1_else10(),
                true);

        checkEquals(
                when1eq1_then1_else10(),
                when1eq10_then1_else10(),
                false);

        checkEquals(
                when1eq1_then1_else10(),
                when1eq1_then_someText_else_anotherText(),
                false
        );
    }

    @Test
    public void testSerialization() {
        CaseExpression<?> original = when1eq1_then1_else10();
        CaseExpression<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_CASE);

        checkEquals(original, restored, true);
    }

    private CaseExpression<?> when1eq1_then_someText_else_anotherText() {
        return CaseExpression.create(
                new Expression[]{
                        ComparisonPredicate.create(ConstantExpression.create(1, INT), ConstantExpression.create(1, INT), ComparisonMode.EQUALS),
                        ConstantExpression.create("someText", VARCHAR),
                        ConstantExpression.create("anotherText", VARCHAR),
                });
    }

    private CaseExpression<?> when1eq1_then1_else10() {
        return CaseExpression.create(
                new Expression[]{
                        ComparisonPredicate.create(ConstantExpression.create(1, INT), ConstantExpression.create(1, INT), ComparisonMode.EQUALS),
                        ConstantExpression.create(10, INT),
                        ConstantExpression.create(20, INT),
                });
    }

    private CaseExpression<?> when1eq10_then1_else10() {
        return CaseExpression.create(
                new Expression[]{
                        ComparisonPredicate.create(ConstantExpression.create(1, INT), ConstantExpression.create(10, INT), ComparisonMode.EQUALS),
                        ConstantExpression.create(10, INT),
                        ConstantExpression.create(20, INT),
                });
    }
}
