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

package com.hazelcast.jet.sql.impl.expression.math;

import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.math.MinusFunction;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static com.hazelcast.sql.SqlColumnType.BIGINT;
import static com.hazelcast.sql.SqlColumnType.BOOLEAN;
import static com.hazelcast.sql.SqlColumnType.DATE;
import static com.hazelcast.sql.SqlColumnType.DECIMAL;
import static com.hazelcast.sql.SqlColumnType.DOUBLE;
import static com.hazelcast.sql.SqlColumnType.INTEGER;
import static com.hazelcast.sql.SqlColumnType.OBJECT;
import static com.hazelcast.sql.SqlColumnType.REAL;
import static com.hazelcast.sql.SqlColumnType.SMALLINT;
import static com.hazelcast.sql.SqlColumnType.TIME;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;
import static com.hazelcast.sql.impl.SqlErrorCode.DATA_EXCEPTION;
import static com.hazelcast.sql.impl.SqlErrorCode.PARSING;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.time.temporal.ChronoUnit.YEARS;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MinusOperatorIntegrationTest extends ArithmeticOperatorIntegrationTest {
    @Override
    protected String operator() {
        return "-";
    }

    @Test
    public void testTinyint() {
        // NULL
        putAndCheckValue((byte) 0, sql("this", "null"), SMALLINT, null);
        putAndCheckValue((byte) 0, sql("null", "this"), SMALLINT, null);

        // Columns
        checkFields((byte) 0, (byte) 1, SMALLINT, (short) -1);
        checkFields(Byte.MAX_VALUE, Byte.MIN_VALUE, SMALLINT, (short) (Byte.MAX_VALUE - Byte.MIN_VALUE));
        checkFields(Byte.MIN_VALUE, Byte.MAX_VALUE, SMALLINT, (short) (Byte.MIN_VALUE - Byte.MAX_VALUE));

        checkFields((byte) 0, (short) 1, INTEGER, -1);
        checkFields(Byte.MAX_VALUE, Short.MIN_VALUE, INTEGER, Byte.MAX_VALUE - Short.MIN_VALUE);
        checkFields(Byte.MIN_VALUE, Short.MAX_VALUE, INTEGER, Byte.MIN_VALUE - Short.MAX_VALUE);

        checkFields((byte) 0, 1, BIGINT, -1L);
        checkFields(Byte.MAX_VALUE, Integer.MIN_VALUE, BIGINT, (long) Byte.MAX_VALUE - Integer.MIN_VALUE);
        checkFields(Byte.MIN_VALUE, Integer.MAX_VALUE, BIGINT, (long) Byte.MIN_VALUE - Integer.MAX_VALUE);

        checkFields((byte) 0, 1L, BIGINT, -1L);
        checkError(Byte.MAX_VALUE, Long.MIN_VALUE, DATA_EXCEPTION, overflowError());
        checkError(Byte.MIN_VALUE, Long.MAX_VALUE, DATA_EXCEPTION, overflowError());

        checkFields((byte) 0, BigInteger.ONE, DECIMAL, decimal("-1"));
        checkFields((byte) 0, decimal("1.1"), DECIMAL, decimal("-1.1"));
        checkFields((byte) 0, 1f, REAL, -1f);
        checkFields((byte) 0, 1d, DOUBLE, -1d);

        // Parameters
        putAndCheckFailure((byte) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), CHAR_VAL);
        putAndCheckFailure((byte) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), STRING_VAL);
        putAndCheckFailure((byte) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue((byte) 0, sql("this", "?"), BIGINT, -1L, (byte) 1);
        putAndCheckValue((byte) 0, sql("this", "?"), BIGINT, -1L, (short) 1);
        putAndCheckValue((byte) 0, sql("this", "?"), BIGINT, -1L, 1);
        putAndCheckValue((byte) 0, sql("this", "?"), BIGINT, -1L, 1L);
        putAndCheckFailure((byte) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BIG_INTEGER_VAL);
        putAndCheckFailure((byte) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BIG_DECIMAL_VAL);
        putAndCheckFailure((byte) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, REAL), FLOAT_VAL);
        putAndCheckFailure((byte) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DOUBLE), DOUBLE_VAL);
        putAndCheckFailure((byte) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure((byte) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure((byte) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure((byte) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure((byte) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testSmallint() {
        // NULL
        putAndCheckValue((short) 0, sql("this", "null"), INTEGER, null);
        putAndCheckValue((short) 0, sql("null", "this"), INTEGER, null);

        // Columns
        checkFields((short) 0, (byte) 1, INTEGER, -1);
        checkFields(Short.MAX_VALUE, Byte.MIN_VALUE, INTEGER, Short.MAX_VALUE - Byte.MIN_VALUE);
        checkFields(Short.MIN_VALUE, Byte.MAX_VALUE, INTEGER, Short.MIN_VALUE - Byte.MAX_VALUE);

        checkFields((short) 0, (short) 1, INTEGER, -1);
        checkFields(Short.MAX_VALUE, Short.MIN_VALUE, INTEGER, Short.MAX_VALUE - Short.MIN_VALUE);
        checkFields(Short.MIN_VALUE, Short.MAX_VALUE, INTEGER, Short.MIN_VALUE - Short.MAX_VALUE);

        checkFields((short) 0, 1, BIGINT, -1L);
        checkFields(Short.MAX_VALUE, Integer.MIN_VALUE, BIGINT, Short.MAX_VALUE - (long) Integer.MIN_VALUE);
        checkFields(Short.MIN_VALUE, Integer.MAX_VALUE, BIGINT, Short.MIN_VALUE - (long) Integer.MAX_VALUE);

        checkFields((short) 0, 1L, BIGINT, -1L);
        checkError(Short.MAX_VALUE, Long.MIN_VALUE, DATA_EXCEPTION, overflowError());
        checkError(Short.MIN_VALUE, Long.MAX_VALUE, DATA_EXCEPTION, overflowError());

        checkFields((short) 0, BigInteger.ONE, DECIMAL, decimal("-1"));
        checkFields((short) 0, decimal("1.1"), DECIMAL, decimal("-1.1"));
        checkFields((short) 0, 1f, REAL, -1f);
        checkFields((short) 0, 1d, DOUBLE, -1d);

        // Parameters
        putAndCheckFailure((short) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), CHAR_VAL);
        putAndCheckFailure((short) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), STRING_VAL);
        putAndCheckFailure((short) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue((short) 0, sql("this", "?"), BIGINT, -1L, (byte) 1);
        putAndCheckValue((short) 0, sql("this", "?"), BIGINT, -1L, (short) 1);
        putAndCheckValue((short) 0, sql("this", "?"), BIGINT, -1L, 1);
        putAndCheckValue((short) 0, sql("this", "?"), BIGINT, -1L, 1L);
        putAndCheckFailure((short) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BIG_INTEGER_VAL);
        putAndCheckFailure((short) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BIG_DECIMAL_VAL);
        putAndCheckFailure((short) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, REAL), FLOAT_VAL);
        putAndCheckFailure((short) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DOUBLE), DOUBLE_VAL);
        putAndCheckFailure((short) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure((short) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure((short) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure((short) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure((short) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testInteger() {
        // NULL
        putAndCheckValue(0, sql("this", "null"), BIGINT, null);
        putAndCheckValue(0, sql("null", "this"), BIGINT, null);

        // Columns
        checkFields(0, (byte) 1, BIGINT, -1L);
        checkFields(Integer.MAX_VALUE, Byte.MIN_VALUE, BIGINT, (long) Integer.MAX_VALUE - Byte.MIN_VALUE);
        checkFields(Integer.MIN_VALUE, Byte.MAX_VALUE, BIGINT, (long) Integer.MIN_VALUE - Byte.MAX_VALUE);

        checkFields(0, (short) 1, BIGINT, -1L);
        checkFields(Integer.MAX_VALUE, Short.MIN_VALUE, BIGINT, (long) Integer.MAX_VALUE - Short.MIN_VALUE);
        checkFields(Integer.MIN_VALUE, Short.MAX_VALUE, BIGINT, (long) Integer.MIN_VALUE - Short.MAX_VALUE);

        checkFields(0, 1, BIGINT, -1L);
        checkFields(Integer.MAX_VALUE, Integer.MIN_VALUE, BIGINT, Integer.MAX_VALUE - (long) Integer.MIN_VALUE);
        checkFields(Integer.MIN_VALUE, Integer.MAX_VALUE, BIGINT, Integer.MIN_VALUE - (long) Integer.MAX_VALUE);

        checkFields(0, 1L, BIGINT, -1L);
        checkError(Integer.MAX_VALUE, Long.MIN_VALUE, DATA_EXCEPTION, overflowError());
        checkError(Integer.MIN_VALUE, Long.MAX_VALUE, DATA_EXCEPTION, overflowError());

        checkFields(0, BigInteger.ONE, DECIMAL, decimal("-1"));
        checkFields(0, decimal("1.1"), DECIMAL, decimal("-1.1"));
        checkFields(0, 1f, REAL, -1f);
        checkFields(0, 1d, DOUBLE, -1d);

        // Parameters
        putAndCheckFailure(0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), CHAR_VAL);
        putAndCheckFailure(0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), STRING_VAL);
        putAndCheckFailure(0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(0, sql("this", "?"), BIGINT, -1L, (byte) 1);
        putAndCheckValue(0, sql("this", "?"), BIGINT, -1L, (short) 1);
        putAndCheckValue(0, sql("this", "?"), BIGINT, -1L, 1);
        putAndCheckValue(0, sql("this", "?"), BIGINT, -1L, 1L);
        putAndCheckFailure(0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BIG_INTEGER_VAL);
        putAndCheckFailure(0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BIG_DECIMAL_VAL);
        putAndCheckFailure(0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, REAL), FLOAT_VAL);
        putAndCheckFailure(0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DOUBLE), DOUBLE_VAL);
        putAndCheckFailure(0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure(0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure(0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure(0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure(0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testBigint() {
        // NULL
        putAndCheckValue(0L, sql("this", "null"), BIGINT, null);
        putAndCheckValue(0L, sql("null", "this"), BIGINT, null);

        // Columns
        checkFields(0L, (byte) 1, BIGINT, -1L);
        checkError(Long.MAX_VALUE, Byte.MIN_VALUE, DATA_EXCEPTION, overflowError());
        checkError(Long.MIN_VALUE, Byte.MAX_VALUE, DATA_EXCEPTION, overflowError());

        checkFields(0L, (short) 1, BIGINT, -1L);
        checkError(Long.MAX_VALUE, Short.MIN_VALUE, DATA_EXCEPTION, overflowError());
        checkError(Long.MIN_VALUE, Short.MAX_VALUE, DATA_EXCEPTION, overflowError());

        checkFields(0, 1, BIGINT, -1L);
        checkError(Long.MAX_VALUE, Integer.MIN_VALUE, DATA_EXCEPTION, overflowError());
        checkError(Long.MIN_VALUE, Integer.MAX_VALUE, DATA_EXCEPTION, overflowError());

        checkFields(0, 1L, BIGINT, -1L);
        checkError(Long.MAX_VALUE, Long.MIN_VALUE, DATA_EXCEPTION, overflowError());
        checkError(Long.MIN_VALUE, Long.MAX_VALUE, DATA_EXCEPTION, overflowError());

        checkFields(0L, BigInteger.ONE, DECIMAL, decimal("-1"));
        checkFields(0L, decimal("1.1"), DECIMAL, decimal("-1.1"));
        checkFields(0L, 1f, REAL, -1f);
        checkFields(0L, 1d, DOUBLE, -1d);

        // Parameters
        putAndCheckFailure(0L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), CHAR_VAL);
        putAndCheckFailure(0L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), STRING_VAL);
        putAndCheckFailure(0L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(0L, sql("this", "?"), BIGINT, -1L, (byte) 1);
        putAndCheckValue(0L, sql("this", "?"), BIGINT, -1L, (short) 1);
        putAndCheckValue(0L, sql("this", "?"), BIGINT, -1L, 1);
        putAndCheckValue(0L, sql("this", "?"), BIGINT, -1L, 1L);
        putAndCheckFailure(0L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BIG_INTEGER_VAL);
        putAndCheckFailure(0L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BIG_DECIMAL_VAL);
        putAndCheckFailure(0L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, REAL), FLOAT_VAL);
        putAndCheckFailure(0L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DOUBLE), DOUBLE_VAL);
        putAndCheckFailure(0L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure(0L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure(0L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure(0L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure(0L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testDecimal_BigInteger() {
        // NULL
        putAndCheckValue(BigInteger.ZERO, sql("this", "null"), DECIMAL, null);
        putAndCheckValue(BigInteger.ZERO, sql("null", "this"), DECIMAL, null);

        // Columns
        checkFields(BigInteger.ZERO, (byte) 1, DECIMAL, decimal(-1));
        checkFields(BigInteger.ONE, Byte.MIN_VALUE, DECIMAL, decimal(1 - Byte.MIN_VALUE));
        checkFields(BigInteger.ONE.negate(), Byte.MAX_VALUE, DECIMAL, decimal(-1 - Byte.MAX_VALUE));

        checkFields(BigInteger.ZERO, (short) 1, DECIMAL, decimal(-1));
        checkFields(BigInteger.ONE, Short.MIN_VALUE, DECIMAL, decimal(1 - Short.MIN_VALUE));
        checkFields(BigInteger.ONE.negate(), Short.MAX_VALUE, DECIMAL, decimal(-1 - Short.MAX_VALUE));

        checkFields(BigInteger.ZERO, 1, DECIMAL, decimal(-1));
        checkFields(BigInteger.ONE, Integer.MIN_VALUE, DECIMAL, decimal(1L - Integer.MIN_VALUE));
        checkFields(BigInteger.ONE.negate(), Integer.MAX_VALUE, DECIMAL, decimal(-1L - Integer.MAX_VALUE));

        checkFields(BigInteger.ZERO, 1L, DECIMAL, decimal(-1));
        checkFields(BigInteger.ONE, Long.MIN_VALUE, DECIMAL, decimal(1).add(decimal(Long.MIN_VALUE).negate()));
        checkFields(BigInteger.ONE.negate(), Long.MAX_VALUE, DECIMAL, decimal(-1).add(decimal(Long.MAX_VALUE).negate()));

        checkFields(BigInteger.ZERO, BigInteger.ONE, DECIMAL, decimal(-1));
        checkFields(BigInteger.ZERO, decimal("1.1"), DECIMAL, decimal("-1.1"));
        checkFields(BigInteger.ZERO, 1f, REAL, -1f);
        checkFields(BigInteger.ZERO, 1d, DOUBLE, -1d);

        // Parameters
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), CHAR_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), STRING_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(BigInteger.ZERO, sql("this", "?"), DECIMAL, decimal(-1), (byte) 1);
        putAndCheckValue(BigInteger.ZERO, sql("this", "?"), DECIMAL, decimal(-1), (short) 1);
        putAndCheckValue(BigInteger.ZERO, sql("this", "?"), DECIMAL, decimal(-1), 1);
        putAndCheckValue(BigInteger.ZERO, sql("this", "?"), DECIMAL, decimal(-1), 1L);
        putAndCheckValue(BigInteger.ZERO, sql("this", "?"), DECIMAL, decimal(-1), BigInteger.ONE);
        putAndCheckValue(BigInteger.ZERO, sql("this", "?"), DECIMAL, decimal("-1.1"), decimal("1.1"));
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, REAL), FLOAT_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, DOUBLE), DOUBLE_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testDecimal_BigDecimal() {
        // NULL
        putAndCheckValue(BigDecimal.ZERO, sql("this", "null"), DECIMAL, null);
        putAndCheckValue(BigDecimal.ZERO, sql("null", "this"), DECIMAL, null);

        // Columns
        checkFields(BigDecimal.ZERO, (byte) 1, DECIMAL, decimal(-1));
        checkFields(BigDecimal.ONE, Byte.MIN_VALUE, DECIMAL, decimal(1 - Byte.MIN_VALUE));
        checkFields(BigDecimal.ONE.negate(), Byte.MAX_VALUE, DECIMAL, decimal(-1 - Byte.MAX_VALUE));

        checkFields(BigDecimal.ZERO, (short) 1, DECIMAL, decimal(-1));
        checkFields(BigDecimal.ONE, Short.MIN_VALUE, DECIMAL, decimal(1 - Short.MIN_VALUE));
        checkFields(BigDecimal.ONE.negate(), Short.MAX_VALUE, DECIMAL, decimal(-1 - Short.MAX_VALUE));

        checkFields(BigDecimal.ZERO, 1, DECIMAL, decimal(-1));
        checkFields(BigDecimal.ONE, Integer.MIN_VALUE, DECIMAL, decimal(1L - Integer.MIN_VALUE));
        checkFields(BigDecimal.ONE.negate(), Integer.MAX_VALUE, DECIMAL, decimal(-1L - Integer.MAX_VALUE));

        checkFields(BigDecimal.ZERO, 1L, DECIMAL, decimal(-1));
        checkFields(BigDecimal.ONE, Long.MIN_VALUE, DECIMAL, decimal(1).add(decimal(Long.MIN_VALUE).negate()));
        checkFields(BigDecimal.ONE.negate(), Long.MAX_VALUE, DECIMAL, decimal(-1).add(decimal(Long.MAX_VALUE).negate()));

        checkFields(BigDecimal.ZERO, BigInteger.ONE, DECIMAL, decimal(-1));
        checkFields(BigDecimal.ZERO, decimal("1.1"), DECIMAL, decimal("-1.1"));
        checkFields(BigDecimal.ZERO, 1f, REAL, -1f);
        checkFields(BigDecimal.ZERO, 1d, DOUBLE, -1d);

        // Parameters
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), CHAR_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), STRING_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(BigDecimal.ZERO, sql("this", "?"), DECIMAL, decimal(-1), (byte) 1);
        putAndCheckValue(BigDecimal.ZERO, sql("this", "?"), DECIMAL, decimal(-1), (short) 1);
        putAndCheckValue(BigDecimal.ZERO, sql("this", "?"), DECIMAL, decimal(-1), 1);
        putAndCheckValue(BigDecimal.ZERO, sql("this", "?"), DECIMAL, decimal(-1), 1L);
        putAndCheckValue(BigDecimal.ZERO, sql("this", "?"), DECIMAL, decimal(-1), BigInteger.ONE);
        putAndCheckValue(BigDecimal.ZERO, sql("this", "?"), DECIMAL, decimal("-1.1"), decimal("1.1"));
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, REAL), FLOAT_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, DOUBLE), DOUBLE_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testReal() {
        putAndCheckValue(0f, sql("this", "null"), REAL, null);
        putAndCheckValue(0f, sql("null", "this"), REAL, null);

        checkFields(1f, 2f, REAL, -1f);
        checkFields(1f, 2d, DOUBLE, -1d);

        putAndCheckFailure(1f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, VARCHAR), CHAR_VAL);
        putAndCheckFailure(1f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, VARCHAR), STRING_VAL);
        putAndCheckFailure(1f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(1f, sql("this", "?"), REAL, -1f, (byte) 2);
        putAndCheckValue(1f, sql("this", "?"), REAL, -1f, (short) 2);
        putAndCheckValue(1f, sql("this", "?"), REAL, -1f, 2);
        putAndCheckValue(1f, sql("this", "?"), REAL, -1f, 2L);
        putAndCheckValue(1f, sql("this", "?"), REAL, 2f, BigInteger.ONE.negate());
        putAndCheckValue(1f, sql("this", "?"), REAL, -1f, decimal("2"));
        putAndCheckValue(1f, sql("this", "?"), REAL, -1f, 2f);
        putAndCheckFailure(1f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, DOUBLE), DOUBLE_VAL);
        putAndCheckFailure(1f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure(1f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure(1f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure(1f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure(1f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testDouble() {
        putAndCheckValue(0d, sql("this", "null"), DOUBLE, null);
        putAndCheckValue(0d, sql("null", "this"), DOUBLE, null);

        checkFields(1d, 2d, DOUBLE, -1d);

        putAndCheckFailure(1d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, VARCHAR), CHAR_VAL);
        putAndCheckFailure(1d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, VARCHAR), STRING_VAL);
        putAndCheckFailure(1d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(1d, sql("this", "?"), DOUBLE, -1d, (byte) 2);
        putAndCheckValue(1d, sql("this", "?"), DOUBLE, -1d, (short) 2);
        putAndCheckValue(1d, sql("this", "?"), DOUBLE, -1d, 2);
        putAndCheckValue(1d, sql("this", "?"), DOUBLE, -1d, 2L);
        putAndCheckValue(1d, sql("this", "?"), DOUBLE, 2d, BigInteger.ONE.negate());
        putAndCheckValue(1d, sql("this", "?"), DOUBLE, -1d, decimal("2"));
        putAndCheckValue(1d, sql("this", "?"), DOUBLE, -1d, 2f);
        putAndCheckValue(1d, sql("this", "?"), DOUBLE, -1d, 2d);
        putAndCheckFailure(1d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure(1d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure(1d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure(1d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure(1d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testDate() {
        LocalDate date = LocalDate.now();

        // Check null values when one side is interval
        putAndCheckValue(date, sql(null, "INTERVAL '1' SECOND"), TIMESTAMP, null);
        putAndCheckFailure(date, sql("INTERVAL '1' SECOND", null), PARSING, signatureError("INTERVAL_SECOND", TIMESTAMP));

        // Check null values when one side is temporal type. Since we cannot deduce the type of the other side, we fail.
        putAndCheckFailure(date, sql("this", null), PARSING, signatureError(DATE, DATE));
        putAndCheckFailure(date, sql(null, "this"), PARSING, signatureError(DATE, DATE));

        // Check normal operations
        putAndCheckValue(date, sql("this", "INTERVAL '1' SECOND"), TIMESTAMP, date.atStartOfDay().minus(1, SECONDS));
        putAndCheckValue(date, sql("this", "INTERVAL '1' MINUTE"), TIMESTAMP, date.atStartOfDay().minus(1, MINUTES));
        putAndCheckValue(date, sql("this", "INTERVAL '1' HOUR"), TIMESTAMP, date.atStartOfDay().minus(1, HOURS));
        putAndCheckValue(date, sql("this", "INTERVAL '1' DAY"), TIMESTAMP, date.atStartOfDay().minus(1, DAYS));
        putAndCheckValue(date, sql("this", "INTERVAL '1 00:00:01' DAY TO SECOND"), TIMESTAMP,
                date.atStartOfDay().minus(1, DAYS).minus(1, SECONDS));

        putAndCheckValue(date, sql("this", "INTERVAL '1' MONTH"), TIMESTAMP, date.atStartOfDay().minus(1, MONTHS));
        putAndCheckValue(date, sql("this", "INTERVAL '1' YEAR"), TIMESTAMP, date.atStartOfDay().minus(1, YEARS));
        putAndCheckValue(date, sql("this", "INTERVAL '1-1' YEAR TO MONTH"), TIMESTAMP,
                date.atStartOfDay().minus(1, YEARS).minus(1, MONTHS));

        // Check the inverse order of operands
        putAndCheckFailure(date, sql("INTERVAL '1' SECOND", "this"), PARSING, signatureError("INTERVAL_SECOND", DATE));

        // Check parameter as temporal operand
        putAndCheckValue(date, sql("?", "INTERVAL '1' SECOND"), TIMESTAMP, date.atStartOfDay().minus(1, SECONDS), date);
        putAndCheckFailure(date, sql("INTERVAL '1' SECOND", "?"), PARSING, signatureError("INTERVAL_SECOND", TIMESTAMP));

        // Parameter on the other side of temporal operand should fail, because we do not expose interval literals.
        putAndCheckFailure(date, sql("this", "?"), PARSING, signatureError(DATE, DATE));
        putAndCheckFailure(date, sql("?", "this"), PARSING, signatureError(DATE, DATE));
    }

    @Test
    public void testTime() {
        LocalTime time = LocalTime.now();

        // Check null values when one side is interval
        putAndCheckValue(time, sql(null, "INTERVAL '1' SECOND"), TIMESTAMP, null);
        putAndCheckFailure(time, sql("INTERVAL '1' SECOND", null), PARSING, signatureError("INTERVAL_SECOND", TIMESTAMP));

        // Check null values when one side is temporal type. Since we cannot deduce the type of the other side, we fail.
        putAndCheckFailure(time, sql("this", null), PARSING, signatureError(TIME, TIME));
        putAndCheckFailure(time, sql(null, "this"), PARSING, signatureError(TIME, TIME));

        // Check normal operations
        putAndCheckValue(time, sql("this", "INTERVAL '1' SECOND"), TIME, time.minus(1, SECONDS));
        putAndCheckValue(time, sql("this", "INTERVAL '1' MINUTE"), TIME, time.minus(1, MINUTES));
        putAndCheckValue(time, sql("this", "INTERVAL '1' HOUR"), TIME, time.minus(1, HOURS));
        putAndCheckValue(time, sql("this", "INTERVAL '1' DAY"), TIME, time);
        putAndCheckValue(time, sql("this", "INTERVAL '1 00:00:01' DAY TO SECOND"), TIME, time.minus(1, SECONDS));

        putAndCheckValue(time, sql("this", "INTERVAL '1' MONTH"), TIME, time);
        putAndCheckValue(time, sql("this", "INTERVAL '1' YEAR"), TIME, time);
        putAndCheckValue(time, sql("this", "INTERVAL '1-1' YEAR TO MONTH"), TIME, time);

        // Check the inverse order of operands
        putAndCheckFailure(time, sql("INTERVAL '1' SECOND", "this"), PARSING, signatureError("INTERVAL_SECOND", TIME));

        // Check parameter as temporal operand. TIME is extended to TIMESTAMP
        putAndCheckValue(time, sql("?", "INTERVAL '1' SECOND"), TIMESTAMP,
                time.atDate(LocalDate.now()).minus(1, SECONDS), time);
        putAndCheckFailure(time, sql("INTERVAL '1' SECOND", "?"), PARSING, signatureError("INTERVAL_SECOND", TIMESTAMP));

        // Parameter on the other side of temporal operand should fail, because we do not expose interval literals.
        putAndCheckFailure(time, sql("this", "?"), PARSING, signatureError(TIME, TIME));
        putAndCheckFailure(time, sql("?", "this"), PARSING, signatureError(TIME, TIME));
    }

    @Test
    public void testTimestamp() {
        LocalDateTime timestamp = LocalDateTime.now();

        // Check null values when one side is interval
        putAndCheckValue(timestamp, sql(null, "INTERVAL '1' SECOND"), TIMESTAMP, null);
        putAndCheckFailure(timestamp, sql("INTERVAL '1' SECOND", null), PARSING, signatureError("INTERVAL_SECOND", TIMESTAMP));

        // Check null values when one side is temporal type. Since we cannot deduce the type of the other side, we fail.
        putAndCheckFailure(timestamp, sql("this", null), PARSING, signatureError(TIMESTAMP, TIMESTAMP));
        putAndCheckFailure(timestamp, sql(null, "this"), PARSING, signatureError(TIMESTAMP, TIMESTAMP));

        // Check normal operations
        putAndCheckValue(timestamp, sql("this", "INTERVAL '1' SECOND"), TIMESTAMP, timestamp.minus(1, SECONDS));
        putAndCheckValue(timestamp, sql("this", "INTERVAL '1' MINUTE"), TIMESTAMP, timestamp.minus(1, MINUTES));
        putAndCheckValue(timestamp, sql("this", "INTERVAL '1' HOUR"), TIMESTAMP, timestamp.minus(1, HOURS));
        putAndCheckValue(timestamp, sql("this", "INTERVAL '1' DAY"), TIMESTAMP, timestamp.minus(1, DAYS));
        putAndCheckValue(timestamp, sql("this", "INTERVAL '1 00:00:01' DAY TO SECOND"), TIMESTAMP,
                timestamp.minus(1, DAYS).minus(1, SECONDS));

        putAndCheckValue(timestamp, sql("this", "INTERVAL '1' MONTH"), TIMESTAMP, timestamp.minus(1, MONTHS));
        putAndCheckValue(timestamp, sql("this", "INTERVAL '1' YEAR"), TIMESTAMP, timestamp.minus(1, YEARS));
        putAndCheckValue(timestamp, sql("this", "INTERVAL '1-1' YEAR TO MONTH"), TIMESTAMP,
                timestamp.minus(1, YEARS).minus(1, MONTHS));

        // Check the inverse order of operands
        putAndCheckFailure(timestamp, sql("INTERVAL '1' SECOND", "this"), PARSING, signatureError("INTERVAL_SECOND", TIMESTAMP));

        // Check parameter as temporal operand
        putAndCheckValue(timestamp, sql("?", "INTERVAL '1' SECOND"), TIMESTAMP, timestamp.minus(1, SECONDS), timestamp);
        putAndCheckFailure(timestamp, sql("INTERVAL '1' SECOND", "?"), PARSING, signatureError("INTERVAL_SECOND", TIMESTAMP));

        // Parameter on the other side of temporal operand should fail, because we do not expose interval literals.
        putAndCheckFailure(timestamp, sql("this", "?"), PARSING, signatureError(TIMESTAMP, TIMESTAMP));
        putAndCheckFailure(timestamp, sql("?", "this"), PARSING, signatureError(TIMESTAMP, TIMESTAMP));
    }

    @Test
    public void testTimestampWithTimezone() {
        OffsetDateTime timestamp = OffsetDateTime.now();

        // Check null values when one side is interval
        putAndCheckValue(timestamp, sql(null, "INTERVAL '1' SECOND"), TIMESTAMP, null);
        putAndCheckFailure(timestamp, sql("INTERVAL '1' SECOND", null), PARSING, signatureError("INTERVAL_SECOND", TIMESTAMP));

        // Check null values when one side is temporal type. Since we cannot deduce the type of the other side, we fail.
        putAndCheckFailure(timestamp, sql("this", null), PARSING,
                signatureError(TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(timestamp, sql(null, "this"), PARSING,
                signatureError(TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE));

        // Check normal operations
        putAndCheckValue(timestamp, sql("this", "INTERVAL '1' SECOND"), TIMESTAMP_WITH_TIME_ZONE, timestamp.minus(1, SECONDS));
        putAndCheckValue(timestamp, sql("this", "INTERVAL '1' MINUTE"), TIMESTAMP_WITH_TIME_ZONE, timestamp.minus(1, MINUTES));
        putAndCheckValue(timestamp, sql("this", "INTERVAL '1' HOUR"), TIMESTAMP_WITH_TIME_ZONE, timestamp.minus(1, HOURS));
        putAndCheckValue(timestamp, sql("this", "INTERVAL '1' DAY"), TIMESTAMP_WITH_TIME_ZONE, timestamp.minus(1, DAYS));
        putAndCheckValue(timestamp, sql("this", "INTERVAL '1 00:00:01' DAY TO SECOND"), TIMESTAMP_WITH_TIME_ZONE,
                timestamp.minus(1, DAYS).minus(1, SECONDS));

        putAndCheckValue(timestamp, sql("this", "INTERVAL '1' MONTH"), TIMESTAMP_WITH_TIME_ZONE, timestamp.minus(1, MONTHS));
        putAndCheckValue(timestamp, sql("this", "INTERVAL '1' YEAR"), TIMESTAMP_WITH_TIME_ZONE, timestamp.minus(1, YEARS));
        putAndCheckValue(timestamp, sql("this", "INTERVAL '1-1' YEAR TO MONTH"), TIMESTAMP_WITH_TIME_ZONE,
                timestamp.minus(1, YEARS).minus(1, MONTHS));

        // Check the inverse order of operands
        putAndCheckFailure(timestamp, sql("INTERVAL '1' SECOND", "this"), PARSING,
                signatureError("INTERVAL_SECOND", TIMESTAMP_WITH_TIME_ZONE));

        // Check parameter as temporal operand
        putAndCheckFailure(timestamp, sql("?", "INTERVAL '1' SECOND"), DATA_EXCEPTION,
                parameterError(0, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE), timestamp);
        putAndCheckFailure(timestamp, sql("INTERVAL '1' SECOND", "?"), PARSING,
                signatureError("INTERVAL_SECOND", TIMESTAMP));

        // Parameter on the other side of temporal operand should fail, because we do not expose interval literals.
        putAndCheckFailure(timestamp, sql("this", "?"), PARSING,
                signatureError(TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(timestamp, sql("?", "this"), PARSING,
                signatureError(TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE));
    }

    @Test
    public void testEquality() {
        checkEquals(MinusFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT),
                MinusFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT), true);

        checkEquals(MinusFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT),
                MinusFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), QueryDataType.BIGINT), false);

        checkEquals(MinusFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT),
                MinusFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(100, INT), INT), false);
    }

    @Test
    public void testSerialization() {
        Expression<?> original = MinusFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT);
        Expression<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_MINUS);

        checkEquals(original, restored, true);
    }

    private static String overflowError() {
        return "BIGINT overflow in '-' operator (consider adding explicit CAST to DECIMAL)";
    }
}
