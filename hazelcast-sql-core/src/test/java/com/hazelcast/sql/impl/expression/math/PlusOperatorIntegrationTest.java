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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
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

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PlusOperatorIntegrationTest extends ArithmeticOperatorIntegrationTest {
    @Override
    protected String operator() {
        return "+";
    }

    @Test
    public void testTinyint() {
        // NULL
        putAndCheckValue((byte) 0, sql("this", "null"), SMALLINT, null);
        putAndCheckValue((byte) 0, sql("null", "this"), SMALLINT, null);

        // Columns
        checkFieldsCommute((byte) 0, (byte) 1, SMALLINT, (short) 1);
        checkFieldsCommute((byte) 1, Byte.MAX_VALUE, SMALLINT, (short) (Byte.MAX_VALUE + 1));
        checkFieldsCommute((byte) -1, Byte.MIN_VALUE, SMALLINT, (short) (Byte.MIN_VALUE - 1));
        checkFieldsCommute(Byte.MAX_VALUE, Byte.MAX_VALUE, SMALLINT, (short) (Byte.MAX_VALUE * 2));
        checkFieldsCommute(Byte.MIN_VALUE, Byte.MIN_VALUE, SMALLINT, (short) (Byte.MIN_VALUE * 2));

        checkFieldsCommute((byte) 0, (short) 1, INTEGER, 1);
        checkFieldsCommute((byte) 1, Short.MAX_VALUE, INTEGER, Short.MAX_VALUE + 1);
        checkFieldsCommute((byte) -1, Short.MIN_VALUE, INTEGER, Short.MIN_VALUE - 1);
        checkFieldsCommute(Byte.MAX_VALUE, Short.MAX_VALUE, INTEGER, Byte.MAX_VALUE + Short.MAX_VALUE);
        checkFieldsCommute(Byte.MIN_VALUE, Short.MIN_VALUE, INTEGER, Byte.MIN_VALUE + Short.MIN_VALUE);

        checkFieldsCommute((byte) 0, 1, BIGINT, 1L);
        checkFieldsCommute((byte) 1, Integer.MAX_VALUE, BIGINT, Integer.MAX_VALUE + 1L);
        checkFieldsCommute((byte) -1, Integer.MIN_VALUE, BIGINT, Integer.MIN_VALUE - 1L);
        checkFieldsCommute(Byte.MAX_VALUE, Integer.MAX_VALUE, BIGINT, Byte.MAX_VALUE + (long) Integer.MAX_VALUE);
        checkFieldsCommute(Byte.MIN_VALUE, Integer.MIN_VALUE, BIGINT, Byte.MIN_VALUE + (long) Integer.MIN_VALUE);

        checkFieldsCommute((byte) 0, 1L, BIGINT, 1L);
        checkErrorCommute((byte) 1, Long.MAX_VALUE, DATA_EXCEPTION, overflowError());
        checkErrorCommute((byte) -1, Long.MIN_VALUE, DATA_EXCEPTION, overflowError());
        checkErrorCommute(Byte.MAX_VALUE, Long.MAX_VALUE, DATA_EXCEPTION, overflowError());
        checkErrorCommute(Byte.MIN_VALUE, Long.MIN_VALUE, DATA_EXCEPTION, overflowError());

        checkFieldsCommute((byte) 0, BigInteger.ONE, DECIMAL, decimal("1"));
        checkFieldsCommute((byte) 0, decimal("1.1"), DECIMAL, decimal("1.1"));
        checkFieldsCommute((byte) 0, 1f, REAL, 1f);
        checkFieldsCommute((byte) 0, 1d, DOUBLE, 1d);

        // Parameters
        putAndCheckFailure((byte) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), CHAR_VAL);
        putAndCheckFailure((byte) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), STRING_VAL);
        putAndCheckFailure((byte) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue((byte) 0, sql("this", "?"), BIGINT, 1L, (byte) 1);
        putAndCheckValue((byte) 0, sql("this", "?"), BIGINT, 1L, (short) 1);
        putAndCheckValue((byte) 0, sql("this", "?"), BIGINT, 1L, 1);
        putAndCheckValue((byte) 0, sql("this", "?"), BIGINT, 1L, 1L);
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
        checkFieldsCommute((short) 0, (byte) 1, INTEGER, 1);
        checkFieldsCommute((short) 1, Byte.MAX_VALUE, INTEGER, Byte.MAX_VALUE + 1);
        checkFieldsCommute((short) -1, Byte.MIN_VALUE, INTEGER, Byte.MIN_VALUE - 1);
        checkFieldsCommute(Short.MAX_VALUE, Byte.MAX_VALUE, INTEGER, Short.MAX_VALUE + Byte.MAX_VALUE);
        checkFieldsCommute(Short.MIN_VALUE, Byte.MIN_VALUE, INTEGER, Short.MIN_VALUE + Byte.MIN_VALUE);

        checkFieldsCommute((short) 0, (short) 1, INTEGER, 1);
        checkFieldsCommute((short) 1, Short.MAX_VALUE, INTEGER, Short.MAX_VALUE + 1);
        checkFieldsCommute((short) -1, Short.MIN_VALUE, INTEGER, Short.MIN_VALUE - 1);
        checkFieldsCommute(Short.MAX_VALUE, Short.MAX_VALUE, INTEGER, Short.MAX_VALUE + Short.MAX_VALUE);
        checkFieldsCommute(Short.MIN_VALUE, Short.MIN_VALUE, INTEGER, Short.MIN_VALUE + Short.MIN_VALUE);

        checkFieldsCommute((short) 0, 1, BIGINT, 1L);
        checkFieldsCommute((short) 1, Integer.MAX_VALUE, BIGINT, Integer.MAX_VALUE + 1L);
        checkFieldsCommute((short) -1, Integer.MIN_VALUE, BIGINT, Integer.MIN_VALUE - 1L);
        checkFieldsCommute(Short.MAX_VALUE, Integer.MAX_VALUE, BIGINT, Short.MAX_VALUE + (long) Integer.MAX_VALUE);
        checkFieldsCommute(Short.MIN_VALUE, Integer.MIN_VALUE, BIGINT, Short.MIN_VALUE + (long) Integer.MIN_VALUE);

        checkFieldsCommute((short) 0, 1L, BIGINT, 1L);
        checkErrorCommute((short) 1, Long.MAX_VALUE, DATA_EXCEPTION, overflowError());
        checkErrorCommute((short) -1, Long.MIN_VALUE, DATA_EXCEPTION, overflowError());
        checkErrorCommute(Short.MAX_VALUE, Long.MAX_VALUE, DATA_EXCEPTION, overflowError());
        checkErrorCommute(Short.MIN_VALUE, Long.MIN_VALUE, DATA_EXCEPTION, overflowError());

        checkFieldsCommute((short) 0, BigInteger.ONE, DECIMAL, decimal("1"));
        checkFieldsCommute((short) 0, decimal("1.1"), DECIMAL, decimal("1.1"));
        checkFieldsCommute((short) 0, 1f, REAL, 1f);
        checkFieldsCommute((short) 0, 1d, DOUBLE, 1d);

        // Parameters
        putAndCheckFailure((short) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), CHAR_VAL);
        putAndCheckFailure((short) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), STRING_VAL);
        putAndCheckFailure((short) 0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue((short) 0, sql("this", "?"), BIGINT, 1L, (byte) 1);
        putAndCheckValue((short) 0, sql("this", "?"), BIGINT, 1L, (short) 1);
        putAndCheckValue((short) 0, sql("this", "?"), BIGINT, 1L, 1);
        putAndCheckValue((short) 0, sql("this", "?"), BIGINT, 1L, 1L);
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
        checkFieldsCommute(0, (byte) 1, BIGINT, 1L);
        checkFieldsCommute(1, Byte.MAX_VALUE, BIGINT, Byte.MAX_VALUE + 1L);
        checkFieldsCommute(-1, Byte.MIN_VALUE, BIGINT, Byte.MIN_VALUE - 1L);
        checkFieldsCommute(Integer.MAX_VALUE, Byte.MAX_VALUE, BIGINT, (long) Integer.MAX_VALUE + Byte.MAX_VALUE);
        checkFieldsCommute(Integer.MIN_VALUE, Byte.MIN_VALUE, BIGINT, (long) Integer.MIN_VALUE + Byte.MIN_VALUE);

        checkFieldsCommute(0, (short) 1, BIGINT, 1L);
        checkFieldsCommute(1, Short.MAX_VALUE, BIGINT, Short.MAX_VALUE + 1L);
        checkFieldsCommute(-1, Short.MIN_VALUE, BIGINT, Short.MIN_VALUE - 1L);
        checkFieldsCommute(Integer.MAX_VALUE, Short.MAX_VALUE, BIGINT, (long) Integer.MAX_VALUE + Short.MAX_VALUE);
        checkFieldsCommute(Integer.MIN_VALUE, Short.MIN_VALUE, BIGINT, (long) Integer.MIN_VALUE + Short.MIN_VALUE);

        checkFieldsCommute(0, 1, BIGINT, 1L);
        checkFieldsCommute(1, Integer.MAX_VALUE, BIGINT, Integer.MAX_VALUE + 1L);
        checkFieldsCommute(-1, Integer.MIN_VALUE, BIGINT, Integer.MIN_VALUE - 1L);
        checkFieldsCommute(Integer.MAX_VALUE, Integer.MAX_VALUE, BIGINT, Integer.MAX_VALUE + (long) Integer.MAX_VALUE);
        checkFieldsCommute(Integer.MIN_VALUE, Integer.MIN_VALUE, BIGINT, Integer.MIN_VALUE + (long) Integer.MIN_VALUE);

        checkFieldsCommute(0, 1L, BIGINT, 1L);
        checkErrorCommute(1, Long.MAX_VALUE, DATA_EXCEPTION, overflowError());
        checkErrorCommute(-1, Long.MIN_VALUE, DATA_EXCEPTION, overflowError());
        checkErrorCommute(Integer.MAX_VALUE, Long.MAX_VALUE, DATA_EXCEPTION, overflowError());
        checkErrorCommute(Integer.MIN_VALUE, Long.MIN_VALUE, DATA_EXCEPTION, overflowError());

        checkFieldsCommute(0, BigInteger.ONE, DECIMAL, decimal("1"));
        checkFieldsCommute(0, decimal("1.1"), DECIMAL, decimal("1.1"));
        checkFieldsCommute(0, 1f, REAL, 1f);
        checkFieldsCommute(0, 1d, DOUBLE, 1d);

        // Parameters
        putAndCheckFailure(0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), CHAR_VAL);
        putAndCheckFailure(0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), STRING_VAL);
        putAndCheckFailure(0, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(0, sql("this", "?"), BIGINT, 1L, (byte) 1);
        putAndCheckValue(0, sql("this", "?"), BIGINT, 1L, (short) 1);
        putAndCheckValue(0, sql("this", "?"), BIGINT, 1L, 1);
        putAndCheckValue(0, sql("this", "?"), BIGINT, 1L, 1L);
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
        checkFieldsCommute(0L, (byte) 1, BIGINT, 1L);
        checkFieldsCommute(1L, Byte.MAX_VALUE, BIGINT, Byte.MAX_VALUE + 1L);
        checkFieldsCommute(-1L, Byte.MIN_VALUE, BIGINT, Byte.MIN_VALUE - 1L);
        checkErrorCommute(Long.MAX_VALUE, Byte.MAX_VALUE, DATA_EXCEPTION, overflowError());
        checkErrorCommute(Long.MIN_VALUE, Byte.MIN_VALUE, DATA_EXCEPTION, overflowError());

        checkFieldsCommute(0L, (short) 1, BIGINT, 1L);
        checkFieldsCommute(1L, Short.MAX_VALUE, BIGINT, Short.MAX_VALUE + 1L);
        checkFieldsCommute(-1L, Short.MIN_VALUE, BIGINT, Short.MIN_VALUE - 1L);
        checkErrorCommute(Long.MAX_VALUE, Short.MAX_VALUE, DATA_EXCEPTION, overflowError());
        checkErrorCommute(Long.MIN_VALUE, Short.MIN_VALUE, DATA_EXCEPTION, overflowError());

        checkFieldsCommute(0, 1, BIGINT, 1L);
        checkFieldsCommute(1, Integer.MAX_VALUE, BIGINT, Integer.MAX_VALUE + 1L);
        checkFieldsCommute(-1, Integer.MIN_VALUE, BIGINT, Integer.MIN_VALUE - 1L);
        checkErrorCommute(Long.MAX_VALUE, Integer.MAX_VALUE, DATA_EXCEPTION, overflowError());
        checkErrorCommute(Long.MIN_VALUE, Integer.MIN_VALUE, DATA_EXCEPTION, overflowError());

        checkFieldsCommute(0, 1L, BIGINT, 1L);
        checkErrorCommute(1, Long.MAX_VALUE, DATA_EXCEPTION, overflowError());
        checkErrorCommute(-1, Long.MIN_VALUE, DATA_EXCEPTION, overflowError());
        checkErrorCommute(Long.MAX_VALUE, Long.MAX_VALUE, DATA_EXCEPTION, overflowError());
        checkErrorCommute(Long.MIN_VALUE, Long.MIN_VALUE, DATA_EXCEPTION, overflowError());

        checkFieldsCommute(0L, BigInteger.ONE, DECIMAL, decimal("1"));
        checkFieldsCommute(0L, decimal("1.1"), DECIMAL, decimal("1.1"));
        checkFieldsCommute(0L, 1f, REAL, 1f);
        checkFieldsCommute(0L, 1d, DOUBLE, 1d);

        // Parameters
        putAndCheckFailure(0L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), CHAR_VAL);
        putAndCheckFailure(0L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), STRING_VAL);
        putAndCheckFailure(0L, sql("this", "?"), DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(0L, sql("this", "?"), BIGINT, 1L, (byte) 1);
        putAndCheckValue(0L, sql("this", "?"), BIGINT, 1L, (short) 1);
        putAndCheckValue(0L, sql("this", "?"), BIGINT, 1L, 1);
        putAndCheckValue(0L, sql("this", "?"), BIGINT, 1L, 1L);
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
        checkFieldsCommute(BigInteger.ZERO, (byte) 1, DECIMAL, decimal(1));
        checkFieldsCommute(BigInteger.ONE, Byte.MAX_VALUE, DECIMAL, decimal(Byte.MAX_VALUE + 1));
        checkFieldsCommute(BigInteger.ONE.negate(), Byte.MIN_VALUE, DECIMAL, decimal(Byte.MIN_VALUE - 1));

        checkFieldsCommute(BigInteger.ZERO, (short) 1, DECIMAL, decimal(1));
        checkFieldsCommute(BigInteger.ONE, Short.MAX_VALUE, DECIMAL, decimal(Short.MAX_VALUE + 1));
        checkFieldsCommute(BigInteger.ONE.negate(), Short.MIN_VALUE, DECIMAL, decimal(Short.MIN_VALUE - 1));

        checkFieldsCommute(BigInteger.ZERO, 1, DECIMAL, decimal(1));
        checkFieldsCommute(BigInteger.ONE, Integer.MAX_VALUE, DECIMAL, decimal(Integer.MAX_VALUE + 1L));
        checkFieldsCommute(BigInteger.ONE.negate(), Integer.MIN_VALUE, DECIMAL, decimal(Integer.MIN_VALUE - 1L));

        checkFieldsCommute(BigInteger.ZERO, 1L, DECIMAL, decimal(1));
        checkFieldsCommute(BigInteger.ONE, Long.MAX_VALUE, DECIMAL, decimal(Long.MAX_VALUE).add(decimal(1)));
        checkFieldsCommute(BigInteger.ONE.negate(), Long.MIN_VALUE, DECIMAL, decimal(Long.MIN_VALUE).add(decimal(-1)));

        checkFieldsCommute(BigInteger.ZERO, BigInteger.ONE, DECIMAL, decimal(1));
        checkFieldsCommute(BigInteger.ZERO, decimal("1.1"), DECIMAL, decimal("1.1"));
        checkFieldsCommute(BigInteger.ZERO, 1f, REAL, 1f);
        checkFieldsCommute(BigInteger.ZERO, 1d, DOUBLE, 1d);

        // Parameters
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), CHAR_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), STRING_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(BigInteger.ZERO, sql("this", "?"), DECIMAL, decimal(1), (byte) 1);
        putAndCheckValue(BigInteger.ZERO, sql("this", "?"), DECIMAL, decimal(1), (short) 1);
        putAndCheckValue(BigInteger.ZERO, sql("this", "?"), DECIMAL, decimal(1), 1);
        putAndCheckValue(BigInteger.ZERO, sql("this", "?"), DECIMAL, decimal(1), 1L);
        putAndCheckValue(BigInteger.ZERO, sql("this", "?"), DECIMAL, decimal(1), BigInteger.ONE);
        putAndCheckValue(BigInteger.ZERO, sql("this", "?"), DECIMAL, decimal("1.1"), decimal("1.1"));
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
        checkFieldsCommute(BigDecimal.ZERO, (byte) 1, DECIMAL, decimal(1));
        checkFieldsCommute(BigDecimal.ONE, Byte.MAX_VALUE, DECIMAL, decimal(Byte.MAX_VALUE + 1));
        checkFieldsCommute(BigDecimal.ONE.negate(), Byte.MIN_VALUE, DECIMAL, decimal(Byte.MIN_VALUE - 1));

        checkFieldsCommute(BigDecimal.ZERO, (short) 1, DECIMAL, decimal(1));
        checkFieldsCommute(BigDecimal.ONE, Short.MAX_VALUE, DECIMAL, decimal(Short.MAX_VALUE + 1));
        checkFieldsCommute(BigDecimal.ONE.negate(), Short.MIN_VALUE, DECIMAL, decimal(Short.MIN_VALUE - 1));

        checkFieldsCommute(BigDecimal.ZERO, 1, DECIMAL, decimal(1));
        checkFieldsCommute(BigDecimal.ONE, Integer.MAX_VALUE, DECIMAL, decimal(Integer.MAX_VALUE + 1L));
        checkFieldsCommute(BigDecimal.ONE.negate(), Integer.MIN_VALUE, DECIMAL, decimal(Integer.MIN_VALUE - 1L));

        checkFieldsCommute(BigDecimal.ZERO, 1L, DECIMAL, decimal(1));
        checkFieldsCommute(BigDecimal.ONE, Long.MAX_VALUE, DECIMAL, decimal(Long.MAX_VALUE).add(decimal(1)));
        checkFieldsCommute(BigDecimal.ONE.negate(), Long.MIN_VALUE, DECIMAL, decimal(Long.MIN_VALUE).add(decimal(-1)));

        checkFieldsCommute(BigDecimal.ZERO, BigInteger.ONE, DECIMAL, decimal(1));
        checkFieldsCommute(BigDecimal.ZERO, decimal("1.1"), DECIMAL, decimal("1.1"));
        checkFieldsCommute(BigDecimal.ZERO, 1f, REAL, 1f);
        checkFieldsCommute(BigDecimal.ZERO, 1d, DOUBLE, 1d);

        // Parameters
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), CHAR_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), STRING_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(BigDecimal.ZERO, sql("this", "?"), DECIMAL, decimal(1), (byte) 1);
        putAndCheckValue(BigDecimal.ZERO, sql("this", "?"), DECIMAL, decimal(1), (short) 1);
        putAndCheckValue(BigDecimal.ZERO, sql("this", "?"), DECIMAL, decimal(1), 1);
        putAndCheckValue(BigDecimal.ZERO, sql("this", "?"), DECIMAL, decimal(1), 1L);
        putAndCheckValue(BigDecimal.ZERO, sql("this", "?"), DECIMAL, decimal(1), BigInteger.ONE);
        putAndCheckValue(BigDecimal.ZERO, sql("this", "?"), DECIMAL, decimal("1.1"), decimal("1.1"));
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

        checkFieldsCommute(1f, 2f, REAL, 3f);
        checkFieldsCommute(1f, 2d, DOUBLE, 3d);

        putAndCheckFailure(1f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, VARCHAR), CHAR_VAL);
        putAndCheckFailure(1f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, VARCHAR), STRING_VAL);
        putAndCheckFailure(1f, sql("this", "?"), DATA_EXCEPTION, parameterError(0, REAL, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(1f, sql("this", "?"), REAL, 3f, (byte) 2);
        putAndCheckValue(1f, sql("this", "?"), REAL, 3f, (short) 2);
        putAndCheckValue(1f, sql("this", "?"), REAL, 3f, 2);
        putAndCheckValue(1f, sql("this", "?"), REAL, 3f, 2L);
        putAndCheckValue(1f, sql("this", "?"), REAL, 2f, BigInteger.ONE);
        putAndCheckValue(1f, sql("this", "?"), REAL, 3.1f, decimal("2.1"));
        putAndCheckValue(1f, sql("this", "?"), REAL, 3.1f, 2.1f);
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

        checkFieldsCommute(1d, 2d, DOUBLE, 3d);

        putAndCheckFailure(1d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, VARCHAR), CHAR_VAL);
        putAndCheckFailure(1d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, VARCHAR), STRING_VAL);
        putAndCheckFailure(1d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(1d, sql("this", "?"), DOUBLE, 3d, (byte) 2);
        putAndCheckValue(1d, sql("this", "?"), DOUBLE, 3d, (short) 2);
        putAndCheckValue(1d, sql("this", "?"), DOUBLE, 3d, 2);
        putAndCheckValue(1d, sql("this", "?"), DOUBLE, 3d, 2L);
        putAndCheckValue(1d, sql("this", "?"), DOUBLE, 2d, BigInteger.ONE);
        putAndCheckValue(1d, sql("this", "?"), DOUBLE, 3.1d, decimal("2.1"));
        putAndCheckValue(1d, sql("this", "?"), DOUBLE, 3d, 2f);
        putAndCheckValue(1d, sql("this", "?"), DOUBLE, 3.1d, 2.1d);
        putAndCheckFailure(1d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure(1d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure(1d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure(1d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure(1d, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DOUBLE, OBJECT), OBJECT_VAL);
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
        checkEquals(PlusFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT),
            PlusFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT), true);

        checkEquals(PlusFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT),
            PlusFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), QueryDataType.BIGINT), false);

        checkEquals(PlusFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT),
            PlusFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(100, INT), INT), false);
    }

    @Test
    public void testSerialization() {
        PlusFunction<?> original = PlusFunction.create(ConstantExpression.create(3, INT), ConstantExpression.create(2, INT), INT);
        PlusFunction<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_PLUS);

        checkEquals(original, restored, true);
    }

    private static String overflowError() {
        return "BIGINT overflow in '+' operator (consider adding explicit CAST to DECIMAL)";
    }
}
