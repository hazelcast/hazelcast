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
import com.hazelcast.sql.impl.expression.math.MultiplyFunction;
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
public class MultiplyOperatorIntegrationTest extends ArithmeticOperatorIntegrationTest {
    @Override
    protected String operator() {
        return "*";
    }

    @Test
    public void testTinyint() {
        // NULL
        putAndCheckValue((byte) 0, sql("this", "null"), SMALLINT, null);
        putAndCheckValue((byte) 0, sql("null", "this"), SMALLINT, null);

        // Columns
        checkFieldsCommute((byte) 2, (byte) 3, SMALLINT, (short) 6);
        checkFieldsCommute(Byte.MAX_VALUE, Byte.MAX_VALUE, SMALLINT, (short) (Byte.MAX_VALUE * Byte.MAX_VALUE));

        checkFieldsCommute((byte) 2, (short) 3, INTEGER, 6);
        checkFieldsCommute(Byte.MAX_VALUE, Short.MAX_VALUE, INTEGER, Byte.MAX_VALUE * Short.MAX_VALUE);

        checkFieldsCommute((byte) 2, 3, BIGINT, 6L);
        checkFieldsCommute(Byte.MAX_VALUE, Integer.MAX_VALUE, BIGINT, (long) Byte.MAX_VALUE * Integer.MAX_VALUE);

        checkFieldsCommute((byte) 2, 3L, BIGINT, 6L);
        checkErrorCommute(Byte.MAX_VALUE, Long.MAX_VALUE, DATA_EXCEPTION, overflowError());

        checkFieldsCommute((byte) 2, new BigInteger("3"), DECIMAL, decimal("6"));
        checkFieldsCommute((byte) 2, decimal("3.3"), DECIMAL, decimal("6.6"));
        checkFieldsCommute((byte) 2, 3f, REAL, 6f);
        checkFieldsCommute((byte) 2, 3d, DOUBLE, 6d);

        // Parameters
        putAndCheckFailure((byte) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), CHAR_VAL);
        putAndCheckFailure((byte) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), STRING_VAL);
        putAndCheckFailure((byte) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue((byte) 2, sql("this", "?"), BIGINT, 6L, (byte) 3);
        putAndCheckValue((byte) 2, sql("this", "?"), BIGINT, 6L, (short) 3);
        putAndCheckValue((byte) 2, sql("this", "?"), BIGINT, 6L, 3);
        putAndCheckValue((byte) 2, sql("this", "?"), BIGINT, 6L, 3L);
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
        checkFieldsCommute((short) 2, (byte) 3, INTEGER, 6);
        checkFieldsCommute(Short.MAX_VALUE, Byte.MAX_VALUE, INTEGER, Short.MAX_VALUE * Byte.MAX_VALUE);

        checkFieldsCommute((short) 2, (short) 3, INTEGER, 6);
        checkFieldsCommute(Short.MAX_VALUE, Short.MAX_VALUE, INTEGER, Short.MAX_VALUE * Short.MAX_VALUE);

        checkFieldsCommute((short) 2, 3, BIGINT, 6L);
        checkFieldsCommute(Short.MAX_VALUE, Integer.MAX_VALUE, BIGINT, (long) Short.MAX_VALUE * Integer.MAX_VALUE);

        checkFieldsCommute((short) 2, 3L, BIGINT, 6L);
        checkErrorCommute(Short.MAX_VALUE, Long.MAX_VALUE, DATA_EXCEPTION, overflowError());

        checkFieldsCommute((short) 2, new BigInteger("3"), DECIMAL, decimal("6"));
        checkFieldsCommute((short) 2, decimal("3.3"), DECIMAL, decimal("6.6"));
        checkFieldsCommute((short) 2, 3f, REAL, 6f);
        checkFieldsCommute((short) 2, 3d, DOUBLE, 6d);

        // Parameters
        putAndCheckFailure((short) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), CHAR_VAL);
        putAndCheckFailure((short) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), STRING_VAL);
        putAndCheckFailure((short) 2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue((short) 2, sql("this", "?"), BIGINT, 6L, (byte) 3);
        putAndCheckValue((short) 2, sql("this", "?"), BIGINT, 6L, (short) 3);
        putAndCheckValue((short) 2, sql("this", "?"), BIGINT, 6L, 3);
        putAndCheckValue((short) 2, sql("this", "?"), BIGINT, 6L, 3L);
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
        checkFieldsCommute(2, (byte) 3, BIGINT, 6L);
        checkFieldsCommute(Integer.MAX_VALUE, Byte.MAX_VALUE, BIGINT, (long) Integer.MAX_VALUE * Byte.MAX_VALUE);

        checkFieldsCommute(2, (short) 3, BIGINT, 6L);
        checkFieldsCommute(Integer.MAX_VALUE, Short.MAX_VALUE, BIGINT, (long) Integer.MAX_VALUE * Short.MAX_VALUE);

        checkFieldsCommute(2, 3, BIGINT, 6L);
        checkFieldsCommute(Integer.MAX_VALUE, Integer.MAX_VALUE, BIGINT, (long) Integer.MAX_VALUE * Integer.MAX_VALUE);

        checkFieldsCommute(2, 3L, BIGINT, 6L);
        checkErrorCommute(Integer.MAX_VALUE, Long.MAX_VALUE, DATA_EXCEPTION, overflowError());

        checkFieldsCommute(2, new BigInteger("3"), DECIMAL, decimal("6"));
        checkFieldsCommute(2, decimal("3.3"), DECIMAL, decimal("6.6"));
        checkFieldsCommute(2, 3f, REAL, 6f);
        checkFieldsCommute(2, 3d, DOUBLE, 6d);

        // Parameters
        putAndCheckFailure(2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), CHAR_VAL);
        putAndCheckFailure(2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), STRING_VAL);
        putAndCheckFailure(2, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(2, sql("this", "?"), BIGINT, 6L, (byte) 3);
        putAndCheckValue(2, sql("this", "?"), BIGINT, 6L, (short) 3);
        putAndCheckValue(2, sql("this", "?"), BIGINT, 6L, 3);
        putAndCheckValue(2, sql("this", "?"), BIGINT, 6L, 3L);
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
        checkFieldsCommute(2L, (byte) 3, BIGINT, 6L);
        checkErrorCommute(Long.MAX_VALUE, Byte.MAX_VALUE, DATA_EXCEPTION, overflowError());

        checkFieldsCommute(2L, (short) 3, BIGINT, 6L);
        checkErrorCommute(Long.MAX_VALUE, Short.MAX_VALUE, DATA_EXCEPTION, overflowError());

        checkFieldsCommute(2L, 3, BIGINT, 6L);
        checkErrorCommute(Long.MAX_VALUE, Integer.MAX_VALUE, DATA_EXCEPTION, overflowError());

        checkFieldsCommute(2L, 3L, BIGINT, 6L);
        checkErrorCommute(Long.MAX_VALUE, Long.MAX_VALUE, DATA_EXCEPTION, overflowError());

        checkFieldsCommute(2L, new BigInteger("3"), DECIMAL, decimal("6"));
        checkFieldsCommute(2L, decimal("3.3"), DECIMAL, decimal("6.6"));
        checkFieldsCommute(2L, 3f, REAL, 6f);
        checkFieldsCommute(2L, 3d, DOUBLE, 6d);

        // Parameters
        putAndCheckFailure(2L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), CHAR_VAL);
        putAndCheckFailure(2L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), STRING_VAL);
        putAndCheckFailure(2L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(2L, sql("this", "?"), BIGINT, 6L, (byte) 3);
        putAndCheckValue(2L, sql("this", "?"), BIGINT, 6L, (short) 3);
        putAndCheckValue(2L, sql("this", "?"), BIGINT, 6L, 3);
        putAndCheckValue(2L, sql("this", "?"), BIGINT, 6L, 3L);
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
        checkFieldsCommute(new BigInteger("2"), (byte) 3, DECIMAL, decimal(6));
        checkFieldsCommute(new BigInteger("2"), (short) 3, DECIMAL, decimal(6));
        checkFieldsCommute(new BigInteger("2"), 3, DECIMAL, decimal(6));
        checkFieldsCommute(new BigInteger("2"), 3L, DECIMAL, decimal(6));
        checkFieldsCommute(new BigInteger("2"), new BigInteger("3"), DECIMAL, decimal(6));
        checkFieldsCommute(new BigInteger("2"), decimal("3.3"), DECIMAL, decimal("6.6"));
        checkFieldsCommute(new BigInteger("2"), 3f, REAL, 6f);
        checkFieldsCommute(new BigInteger("2"), 3d, DOUBLE, 6d);

        // Parameters
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), CHAR_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), STRING_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(new BigInteger("2"), sql("this", "?"), DECIMAL, decimal(6), (byte) 3);
        putAndCheckValue(new BigInteger("2"), sql("this", "?"), DECIMAL, decimal(6), (short) 3);
        putAndCheckValue(new BigInteger("2"), sql("this", "?"), DECIMAL, decimal(6), 3);
        putAndCheckValue(new BigInteger("2"), sql("this", "?"), DECIMAL, decimal(6), 3L);
        putAndCheckValue(new BigInteger("2"), sql("this", "?"), DECIMAL, decimal(6), new BigInteger("3"));
        putAndCheckValue(new BigInteger("2"), sql("this", "?"), DECIMAL, decimal("6.6"), decimal("3.3"));
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
        checkFieldsCommute(new BigDecimal("2"), (byte) 3, DECIMAL, decimal(6));
        checkFieldsCommute(new BigDecimal("2"), (short) 3, DECIMAL, decimal(6));
        checkFieldsCommute(new BigDecimal("2"), 3, DECIMAL, decimal(6));
        checkFieldsCommute(new BigDecimal("2"), 3L, DECIMAL, decimal(6));
        checkFieldsCommute(new BigDecimal("2"), new BigInteger("3"), DECIMAL, decimal(6));
        checkFieldsCommute(new BigDecimal("2"), decimal("3.3"), DECIMAL, decimal("6.6"));
        checkFieldsCommute(new BigDecimal("2"), 3f, REAL, 6f);
        checkFieldsCommute(new BigDecimal("2"), 3d, DOUBLE, 6d);

        // Parameters
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), CHAR_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), STRING_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(new BigDecimal("2"), sql("this", "?"), DECIMAL, decimal(6), (byte) 3);
        putAndCheckValue(new BigDecimal("2"), sql("this", "?"), DECIMAL, decimal(6), (short) 3);
        putAndCheckValue(new BigDecimal("2"), sql("this", "?"), DECIMAL, decimal(6), 3);
        putAndCheckValue(new BigDecimal("2"), sql("this", "?"), DECIMAL, decimal(6), 3L);
        putAndCheckValue(new BigDecimal("2"), sql("this", "?"), DECIMAL, decimal(6), new BigInteger("3"));
        putAndCheckValue(new BigDecimal("2"), sql("this", "?"), DECIMAL, decimal("6.6"), decimal("3.3"));
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

        checkFieldsCommute(2f, 3f, REAL, 6f);
        checkFieldsCommute(2f, 3d, DOUBLE, 6d);

        putAndCheckFailure(2f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, VARCHAR), CHAR_VAL);
        putAndCheckFailure(2f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, VARCHAR), STRING_VAL);
        putAndCheckFailure(2f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(2f, sql("this", "?"), REAL, 6f, (byte) 3);
        putAndCheckValue(2f, sql("this", "?"), REAL, 6f, (short) 3);
        putAndCheckValue(2f, sql("this", "?"), REAL, 6f, 3);
        putAndCheckValue(2f, sql("this", "?"), REAL, 6f, 3L);
        putAndCheckValue(2f, sql("this", "?"), REAL, 6f, new BigInteger("3"));
        putAndCheckValue(2f, sql("this", "?"), REAL, 6f, decimal("3"));
        putAndCheckValue(2f, sql("this", "?"), REAL, 6f, 3f);
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

        checkFieldsCommute(2d, 3d, DOUBLE, 6d);

        putAndCheckFailure(2d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, VARCHAR), CHAR_VAL);
        putAndCheckFailure(2d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, VARCHAR), STRING_VAL);
        putAndCheckFailure(2d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(2d, sql("this", "?"), DOUBLE, 6d, (byte) 3);
        putAndCheckValue(2d, sql("this", "?"), DOUBLE, 6d, (short) 3);
        putAndCheckValue(2d, sql("this", "?"), DOUBLE, 6d, 3);
        putAndCheckValue(2d, sql("this", "?"), DOUBLE, 6d, 3L);
        putAndCheckValue(2d, sql("this", "?"), DOUBLE, 6d, new BigInteger("3"));
        putAndCheckValue(2d, sql("this", "?"), DOUBLE, 6d, decimal("3"));
        putAndCheckValue(2d, sql("this", "?"), DOUBLE, 6d, 3f);
        putAndCheckValue(2d, sql("this", "?"), DOUBLE, 6d, 3d);
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
        checkEquals(MultiplyFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT),
                MultiplyFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT), true);

        checkEquals(MultiplyFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT),
                MultiplyFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), QueryDataType.BIGINT), false);

        checkEquals(MultiplyFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT),
                MultiplyFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(100, INT), INT), false);
    }

    @Test
    public void testSerialization() {
        MultiplyFunction<?> original =
                MultiplyFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT);
        MultiplyFunction<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_MULTIPLY);

        checkEquals(original, restored, true);
    }

    private static String overflowError() {
        return "BIGINT overflow in '*' operator (consider adding explicit CAST to DECIMAL)";
    }
}
