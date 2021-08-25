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
import com.hazelcast.sql.impl.expression.math.DivideFunction;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;

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
import static com.hazelcast.sql.impl.type.QueryDataType.INT;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DivideOperatorIntegrationTest extends ArithmeticOperatorIntegrationTest {
    @Override
    protected String operator() {
        return "/";
    }

    @Test
    public void testTinyint() {
        // NULL
        putAndCheckValue((byte) 0, sql("this", "null"), SMALLINT, null);
        putAndCheckValue((byte) 0, sql("null", "this"), SMALLINT, null);

        // Columns
        checkFields((byte) 3, (byte) 2, SMALLINT, (short) 1);
        checkFields((byte) 3, (byte) 4, SMALLINT, (short) 0);
        checkError((byte) 3, (byte) 0, DATA_EXCEPTION, divisionByZeroError());
        checkFields(Byte.MIN_VALUE, (byte) -1, SMALLINT, (short) (Byte.MAX_VALUE + 1));

        checkFields((byte) 3, (short) 2, INTEGER, 1);
        checkFields((byte) 3, (short) 4, INTEGER, 0);
        checkError((byte) 3, (short) 0, DATA_EXCEPTION, divisionByZeroError());
        checkFields(Byte.MIN_VALUE, (short) -1, INTEGER, Byte.MAX_VALUE + 1);

        checkFields((byte) 3, 2, BIGINT, 1L);
        checkFields((byte) 3, 4, BIGINT, 0L);
        checkError((byte) 3, 0, DATA_EXCEPTION, divisionByZeroError());
        checkFields(Byte.MIN_VALUE, -1, BIGINT, (long) Byte.MAX_VALUE + 1);

        checkFields((byte) 3, 2L, BIGINT, 1L);
        checkFields((byte) 3, 4L, BIGINT, 0L);
        checkError((byte) 3, 0L, DATA_EXCEPTION, divisionByZeroError());
        checkFields(Byte.MIN_VALUE, -1L, BIGINT, (long) Byte.MAX_VALUE + 1);

        checkFields((byte) 3, new BigInteger("2"), DECIMAL, decimal("1.5"));
        checkFields((byte) 3, decimal("2"), DECIMAL, decimal("1.5"));
        checkFields((byte) 3, 2f, REAL, 1.5f);
        checkFields((byte) 3, 2d, DOUBLE, 1.5d);

        // Parameters
        putAndCheckFailure((byte) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), CHAR_VAL);
        putAndCheckFailure((byte) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), STRING_VAL);
        putAndCheckFailure((byte) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue((byte) 3, sql("this", "?"), BIGINT, 1L, (byte) 2);
        putAndCheckValue((byte) 3, sql("this", "?"), BIGINT, 1L, (short) 2);
        putAndCheckValue((byte) 3, sql("this", "?"), BIGINT, 1L, 2);
        putAndCheckValue((byte) 3, sql("this", "?"), BIGINT, 1L, 2L);
        putAndCheckFailure((byte) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BIG_INTEGER_VAL);
        putAndCheckFailure((byte) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BIG_DECIMAL_VAL);
        putAndCheckFailure((byte) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, REAL), FLOAT_VAL);
        putAndCheckFailure((byte) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DOUBLE), DOUBLE_VAL);
        putAndCheckFailure((byte) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure((byte) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure((byte) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure((byte) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure((byte) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testSmallint() {
        // NULL
        putAndCheckValue((short) 0, sql("this", "null"), INTEGER, null);
        putAndCheckValue((short) 0, sql("null", "this"), INTEGER, null);

        // Columns
        checkFields((short) 3, (byte) 2, INTEGER, 1);
        checkFields((short) 3, (byte) 4, INTEGER, 0);
        checkError((short) 3, (byte) 0, DATA_EXCEPTION, divisionByZeroError());
        checkFields(Short.MIN_VALUE, (byte) -1, INTEGER, Short.MAX_VALUE + 1);

        checkFields((short) 3, (short) 2, INTEGER, 1);
        checkFields((short) 3, (short) 4, INTEGER, 0);
        checkError((short) 3, (short) 0, DATA_EXCEPTION, divisionByZeroError());
        checkFields(Short.MIN_VALUE, (short) -1, INTEGER, Short.MAX_VALUE + 1);

        checkFields((short) 3, 2, BIGINT, 1L);
        checkFields((short) 3, 4, BIGINT, 0L);
        checkError((short) 3, 0, DATA_EXCEPTION, divisionByZeroError());
        checkFields(Short.MIN_VALUE, -1, BIGINT, (long) Short.MAX_VALUE + 1);

        checkFields((short) 3, 2L, BIGINT, 1L);
        checkFields((short) 3, 4L, BIGINT, 0L);
        checkError((short) 3, 0L, DATA_EXCEPTION, divisionByZeroError());
        checkFields(Short.MIN_VALUE, -1L, BIGINT, (long) Short.MAX_VALUE + 1);

        checkFields((short) 3, new BigInteger("2"), DECIMAL, decimal("1.5"));
        checkFields((short) 3, decimal("2"), DECIMAL, decimal("1.5"));
        checkFields((short) 3, 2f, REAL, 1.5f);
        checkFields((short) 3, 2d, DOUBLE, 1.5d);

        // Parameters
        putAndCheckFailure((short) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), CHAR_VAL);
        putAndCheckFailure((short) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), STRING_VAL);
        putAndCheckFailure((short) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue((short) 3, sql("this", "?"), BIGINT, 1L, (byte) 2);
        putAndCheckValue((short) 3, sql("this", "?"), BIGINT, 1L, (short) 2);
        putAndCheckValue((short) 3, sql("this", "?"), BIGINT, 1L, 2);
        putAndCheckValue((short) 3, sql("this", "?"), BIGINT, 1L, 2L);
        putAndCheckFailure((short) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BIG_INTEGER_VAL);
        putAndCheckFailure((short) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BIG_DECIMAL_VAL);
        putAndCheckFailure((short) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, REAL), FLOAT_VAL);
        putAndCheckFailure((short) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DOUBLE), DOUBLE_VAL);
        putAndCheckFailure((short) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure((short) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure((short) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure((short) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure((short) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testInteger() {
        // NULL
        putAndCheckValue(0, sql("this", "null"), BIGINT, null);
        putAndCheckValue(0, sql("null", "this"), BIGINT, null);

        // Columns
        checkFields(3, (byte) 2, BIGINT, 1L);
        checkFields(3, (byte) 4, BIGINT, 0L);
        checkError(3, (byte) 0, DATA_EXCEPTION, divisionByZeroError());
        checkFields(Integer.MIN_VALUE, (byte) -1, BIGINT, (long) Integer.MAX_VALUE + 1);

        checkFields(3, (short) 2, BIGINT, 1L);
        checkFields(3, (short) 4, BIGINT, 0L);
        checkError(3, (short) 0, DATA_EXCEPTION, divisionByZeroError());
        checkFields(Integer.MIN_VALUE, (short) -1, BIGINT, (long) Integer.MAX_VALUE + 1);

        checkFields(3, 2, BIGINT, 1L);
        checkFields(3, 4, BIGINT, 0L);
        checkError(3, 0, DATA_EXCEPTION, divisionByZeroError());
        checkFields(Integer.MIN_VALUE, -1, BIGINT, (long) Integer.MAX_VALUE + 1);

        checkFields(3, 2L, BIGINT, 1L);
        checkFields(3, 4L, BIGINT, 0L);
        checkError(3, 0L, DATA_EXCEPTION, divisionByZeroError());
        checkFields(Integer.MIN_VALUE, -1L, BIGINT, (long) Integer.MAX_VALUE + 1);

        checkFields(3, new BigInteger("2"), DECIMAL, decimal("1.5"));
        checkFields(3, decimal("2"), DECIMAL, decimal("1.5"));
        checkFields(3, 2f, REAL, 1.5f);
        checkFields(3, 2d, DOUBLE, 1.5d);

        // Parameters
        putAndCheckFailure(2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), CHAR_VAL);
        putAndCheckFailure(2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), STRING_VAL);
        putAndCheckFailure(2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(3, sql("this", "?"), BIGINT, 1L, (byte) 2);
        putAndCheckValue(3, sql("this", "?"), BIGINT, 1L, (short) 2);
        putAndCheckValue(3, sql("this", "?"), BIGINT, 1L, 2);
        putAndCheckValue(3, sql("this", "?"), BIGINT, 1L, 2L);
        putAndCheckFailure(2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BIG_INTEGER_VAL);
        putAndCheckFailure(2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BIG_DECIMAL_VAL);
        putAndCheckFailure(2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, REAL), FLOAT_VAL);
        putAndCheckFailure(2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DOUBLE), DOUBLE_VAL);
        putAndCheckFailure(2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure(2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure(2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure(2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure(2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testBigint() {
        // NULL
        putAndCheckValue(0L, sql("this", "null"), BIGINT, null);
        putAndCheckValue(0L, sql("null", "this"), BIGINT, null);

        // Columns
        checkFields(3L, (byte) 2, BIGINT, 1L);
        checkFields(3L, (byte) 4, BIGINT, 0L);
        checkError(3L, (byte) 0, DATA_EXCEPTION, divisionByZeroError());
        checkError(Long.MIN_VALUE, (byte) -1, DATA_EXCEPTION, overflowError());

        checkFields(3L, (short) 2, BIGINT, 1L);
        checkFields(3L, (short) 4, BIGINT, 0L);
        checkError(3L, (short) 0, DATA_EXCEPTION, divisionByZeroError());
        checkError(Long.MIN_VALUE, (short) -1, DATA_EXCEPTION, overflowError());

        checkFields(3L, 2, BIGINT, 1L);
        checkFields(3L, 4, BIGINT, 0L);
        checkError(3L, 0, DATA_EXCEPTION, divisionByZeroError());
        checkError(Long.MIN_VALUE, -1, DATA_EXCEPTION, overflowError());

        checkFields(3L, 2L, BIGINT, 1L);
        checkFields(3L, 4L, BIGINT, 0L);
        checkError(3L, 0L, DATA_EXCEPTION, divisionByZeroError());
        checkError(Long.MIN_VALUE, -1L, DATA_EXCEPTION, overflowError());

        checkFields(3L, new BigInteger("2"), DECIMAL, decimal("1.5"));
        checkFields(3L, decimal("2"), DECIMAL, decimal("1.5"));
        checkFields(3L, 2f, REAL, 1.5f);
        checkFields(3L, 2d, DOUBLE, 1.5d);

        // Parameters
        putAndCheckFailure(2L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), CHAR_VAL);
        putAndCheckFailure(2L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), STRING_VAL);
        putAndCheckFailure(2L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(3L, sql("this", "?"), BIGINT, 1L, (byte) 2);
        putAndCheckValue(3L, sql("this", "?"), BIGINT, 1L, (short) 2);
        putAndCheckValue(3L, sql("this", "?"), BIGINT, 1L, 2);
        putAndCheckValue(3L, sql("this", "?"), BIGINT, 1L, 2L);
        putAndCheckFailure(2L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BIG_INTEGER_VAL);
        putAndCheckFailure(2L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BIG_DECIMAL_VAL);
        putAndCheckFailure(2L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, REAL), FLOAT_VAL);
        putAndCheckFailure(2L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DOUBLE), DOUBLE_VAL);
        putAndCheckFailure(2L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure(2L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure(2L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure(2L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure(2L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testDecimal_BigInteger() {
        // NULL
        putAndCheckValue(BigInteger.ZERO, sql("this", "null"), DECIMAL, null);
        putAndCheckValue(BigInteger.ZERO, sql("null", "this"), DECIMAL, null);

        // Columns
        checkFields(new BigInteger("3"), (byte) 2, DECIMAL, decimal("1.5"));
        checkFields(new BigInteger("3"), (short) 2, DECIMAL, decimal("1.5"));
        checkFields(new BigInteger("3"), 2, DECIMAL, decimal("1.5"));
        checkFields(new BigInteger("3"), 2L, DECIMAL, decimal("1.5"));
        checkFields(new BigInteger("3"), new BigInteger("2"), DECIMAL, decimal("1.5"));
        checkFields(new BigInteger("3"), decimal("2"), DECIMAL, decimal("1.5"));
        checkFields(new BigInteger("3"), 2f, REAL, 1.5f);
        checkFields(new BigInteger("3"), 2d, DOUBLE, 1.5d);
        checkError(new BigInteger("3"), decimal("0"), DATA_EXCEPTION, divisionByZeroError());

        // Parameters
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), CHAR_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), STRING_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(new BigInteger("3"), sql("this", "?"), DECIMAL, decimal("1.5"), (byte) 2);
        putAndCheckValue(new BigInteger("3"), sql("this", "?"), DECIMAL, decimal("1.5"), (short) 2);
        putAndCheckValue(new BigInteger("3"), sql("this", "?"), DECIMAL, decimal("1.5"), 2);
        putAndCheckValue(new BigInteger("3"), sql("this", "?"), DECIMAL, decimal("1.5"), 2L);
        putAndCheckValue(new BigInteger("3"), sql("this", "?"), DECIMAL, decimal("1.5"), new BigInteger("2"));
        putAndCheckValue(new BigInteger("3"), sql("this", "?"), DECIMAL, decimal("1.5"), decimal("2"));
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
        checkFields(new BigDecimal("3"), (byte) 2, DECIMAL, decimal("1.5"));
        checkFields(new BigDecimal("3"), (short) 2, DECIMAL, decimal("1.5"));
        checkFields(new BigDecimal("3"), 2, DECIMAL, decimal("1.5"));
        checkFields(new BigDecimal("3"), 2L, DECIMAL, decimal("1.5"));
        checkFields(new BigDecimal("3"), new BigInteger("2"), DECIMAL, decimal("1.5"));
        checkFields(new BigDecimal("3"), decimal("2"), DECIMAL, decimal("1.5"));
        checkFields(new BigDecimal("3"), 2f, REAL, 1.5f);
        checkFields(new BigDecimal("3"), 2d, DOUBLE, 1.5d);
        checkError(new BigDecimal("3"), decimal("0"), DATA_EXCEPTION, divisionByZeroError());

        // Parameters
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), CHAR_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), STRING_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(new BigDecimal("3"), sql("this", "?"), DECIMAL, decimal("1.5"), (byte) 2);
        putAndCheckValue(new BigDecimal("3"), sql("this", "?"), DECIMAL, decimal("1.5"), (short) 2);
        putAndCheckValue(new BigDecimal("3"), sql("this", "?"), DECIMAL, decimal("1.5"), 2);
        putAndCheckValue(new BigDecimal("3"), sql("this", "?"), DECIMAL, decimal("1.5"), 2L);
        putAndCheckValue(new BigDecimal("3"), sql("this", "?"), DECIMAL, decimal("1.5"), new BigInteger("2"));
        putAndCheckValue(new BigDecimal("3"), sql("this", "?"), DECIMAL, decimal("1.5"), decimal("2"));
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

        checkFields(3f, 2f, REAL, 1.5f);
        checkError(3f, 0.0f, DATA_EXCEPTION, divisionByZeroError());

        checkFields(3f, 2d, DOUBLE, 1.5d);
        checkError(3f, 0.0d, DATA_EXCEPTION, divisionByZeroError());

        putAndCheckFailure(2f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, VARCHAR), CHAR_VAL);
        putAndCheckFailure(2f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, VARCHAR), STRING_VAL);
        putAndCheckFailure(2f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(3f, sql("this", "?"), REAL, 1.5f, (byte) 2);
        putAndCheckValue(3f, sql("this", "?"), REAL, 1.5f, (short) 2);
        putAndCheckValue(3f, sql("this", "?"), REAL, 1.5f, 2);
        putAndCheckValue(3f, sql("this", "?"), REAL, 1.5f, 2L);
        putAndCheckValue(3f, sql("this", "?"), REAL, 1.5f, new BigInteger("2"));
        putAndCheckValue(3f, sql("this", "?"), REAL, 1.5f, decimal("2"));
        putAndCheckValue(3f, sql("this", "?"), REAL, 1.5f, 2f);
        putAndCheckFailure(2f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, DOUBLE), DOUBLE_VAL);
        putAndCheckFailure(2f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure(2f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure(2f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure(2f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure(2f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testDouble() {
        putAndCheckValue(0d, sql("this", "null"), DOUBLE, null);
        putAndCheckValue(0d, sql("null", "this"), DOUBLE, null);

        checkFields(3d, 2d, DOUBLE, 1.5d);
        checkError(3d, 0.0d, DATA_EXCEPTION, divisionByZeroError());

        putAndCheckFailure(2d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, VARCHAR), CHAR_VAL);
        putAndCheckFailure(2d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, VARCHAR), STRING_VAL);
        putAndCheckFailure(2d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(3d, sql("this", "?"), DOUBLE, 1.5d, (byte) 2);
        putAndCheckValue(3d, sql("this", "?"), DOUBLE, 1.5d, (short) 2);
        putAndCheckValue(3d, sql("this", "?"), DOUBLE, 1.5d, 2);
        putAndCheckValue(3d, sql("this", "?"), DOUBLE, 1.5d, 2L);
        putAndCheckValue(3d, sql("this", "?"), DOUBLE, 1.5d, new BigInteger("2"));
        putAndCheckValue(3d, sql("this", "?"), DOUBLE, 1.5d, decimal("2"));
        putAndCheckValue(3d, sql("this", "?"), DOUBLE, 1.5d, 2f);
        putAndCheckValue(3d, sql("this", "?"), DOUBLE, 1.5d, 2d);
        putAndCheckFailure(2d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure(2d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure(2d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure(2d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure(2d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testTemporal() {
        checkUnsupportedForAllTypesCommute(LOCAL_DATE_VAL, DATE);
        checkUnsupportedForAllTypesCommute(LOCAL_TIME_VAL, TIME);
        checkUnsupportedForAllTypesCommute(LOCAL_DATE_TIME_VAL, TIMESTAMP);
        checkUnsupportedForAllTypesCommute(OFFSET_DATE_TIME_VAL, TIMESTAMP_WITH_TIME_ZONE);
    }

    @Test
    public void testEquality() {
        checkEquals(DivideFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT),
                DivideFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT), true);

        checkEquals(DivideFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT),
                DivideFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), QueryDataType.BIGINT), false);

        checkEquals(DivideFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT),
                DivideFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(100, INT), INT), false);
    }

    @Test
    public void testSerialization() {
        DivideFunction<?> original =
                DivideFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT);
        DivideFunction<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_DIVIDE);

        checkEquals(original, restored, true);
    }

    private static String divisionByZeroError() {
        return "Division by zero";
    }

    private static String overflowError() {
        return "BIGINT overflow in '/' operator (consider adding explicit CAST to DECIMAL)";
    }
}
