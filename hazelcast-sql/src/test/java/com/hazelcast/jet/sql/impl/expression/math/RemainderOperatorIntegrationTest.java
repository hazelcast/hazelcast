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
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.math.RemainderFunction;
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
import static com.hazelcast.sql.SqlColumnType.NULL;
import static com.hazelcast.sql.SqlColumnType.OBJECT;
import static com.hazelcast.sql.SqlColumnType.REAL;
import static com.hazelcast.sql.SqlColumnType.SMALLINT;
import static com.hazelcast.sql.SqlColumnType.TIME;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.SqlColumnType.TINYINT;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;
import static com.hazelcast.sql.impl.SqlErrorCode.DATA_EXCEPTION;
import static com.hazelcast.sql.impl.expression.ConstantExpression.create;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RemainderOperatorIntegrationTest extends ArithmeticOperatorIntegrationTest {
    @Override
    protected String operator() {
        return "%";
    }

    @Test
    public void testTinyint_left() {
        // NULL
        putAndCheckValue((byte) 0, sql("this", "null"), TINYINT, null);
        putAndCheckValue((byte) 0, sql("null", "this"), TINYINT, null);

        // Columns
        checkFields((byte) 3, (byte) 2, TINYINT, (byte) 1);
        checkFields((byte) 3, (byte) 4, TINYINT, (byte) 3);
        checkError((byte) 3, (byte) 0, DATA_EXCEPTION, divisionByZeroError());

        checkFields((byte) 3, (short) 2, SMALLINT, (short) 1);
        checkFields((byte) 3, (short) 4, SMALLINT, (short) 3);
        checkError((byte) 3, (short) 0, DATA_EXCEPTION, divisionByZeroError());

        checkFields((byte) 3, 2, INTEGER, 1);
        checkFields((byte) 3, 4, INTEGER, 3);
        checkError((byte) 3, 0, DATA_EXCEPTION, divisionByZeroError());

        checkFields((byte) 3, 2L, BIGINT, 1L);
        checkFields((byte) 3, 4L, BIGINT, 3L);
        checkError((byte) 3, 0L, DATA_EXCEPTION, divisionByZeroError());

        checkFields((byte) 3, decimal("1.4"), DECIMAL, decimal("0.2"));
        checkError((byte) 3, decimal("0.0"), DATA_EXCEPTION, divisionByZeroError());

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
    public void testSmallint_left() {
        // NULL
        putAndCheckValue((short) 0, sql("this", "null"), SMALLINT, null);
        putAndCheckValue((short) 0, sql("null", "this"), SMALLINT, null);

        // Columns
        checkFields((short) 3, (byte) 2, SMALLINT, (short) 1);
        checkFields((short) 3, (byte) 4, SMALLINT, (short) 3);
        checkError((short) 3, (byte) 0, DATA_EXCEPTION, divisionByZeroError());

        checkFields((short) 3, (short) 2, SMALLINT, (short) 1);
        checkFields((short) 3, (short) 4, SMALLINT, (short) 3);
        checkError((short) 3, (short) 0, DATA_EXCEPTION, divisionByZeroError());

        checkFields((short) 3, 2, INTEGER, 1);
        checkFields((short) 3, 4, INTEGER, 3);
        checkError((short) 3, 0, DATA_EXCEPTION, divisionByZeroError());

        checkFields((short) 3, 2L, BIGINT, 1L);
        checkFields((short) 3, 4L, BIGINT, 3L);
        checkError((short) 3, 0L, DATA_EXCEPTION, divisionByZeroError());

        checkFields((short) 3, decimal("1.4"), DECIMAL, decimal("0.2"));
        checkError((short) 3, decimal("0.0"), DATA_EXCEPTION, divisionByZeroError());

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
    public void testInteger_left() {
        // NULL
        putAndCheckValue(0, sql("this", "null"), INTEGER, null);
        putAndCheckValue(0, sql("null", "this"), INTEGER, null);

        // Columns
        checkFields(3, (byte) 2, INTEGER, 1);
        checkFields(3, (byte) 4, INTEGER, 3);
        checkError(3, (byte) 0, DATA_EXCEPTION, divisionByZeroError());

        checkFields(3, (short) 2, INTEGER, 1);
        checkFields(3, (short) 4, INTEGER, 3);
        checkError(3, (short) 0, DATA_EXCEPTION, divisionByZeroError());

        checkFields(3, 2, INTEGER, 1);
        checkFields(3, 4, INTEGER, 3);
        checkError(3, 0, DATA_EXCEPTION, divisionByZeroError());

        checkFields(3, 2L, BIGINT, 1L);
        checkFields(3, 4L, BIGINT, 3L);
        checkError(3, 0L, DATA_EXCEPTION, divisionByZeroError());

        checkFields(3, decimal("1.4"), DECIMAL, decimal("0.2"));
        checkError(3, decimal("0.0"), DATA_EXCEPTION, divisionByZeroError());

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
    public void testBigint_left() {
        // NULL
        putAndCheckValue(0L, sql("this", "null"), BIGINT, null);
        putAndCheckValue(0L, sql("null", "this"), BIGINT, null);

        // Columns
        checkFields(3L, (byte) 2, BIGINT, 1L);
        checkFields(3L, (byte) 4, BIGINT, 3L);
        checkError(3L, (byte) 0, DATA_EXCEPTION, divisionByZeroError());

        checkFields(3L, (short) 2, BIGINT, 1L);
        checkFields(3L, (short) 4, BIGINT, 3L);
        checkError(3L, (short) 0, DATA_EXCEPTION, divisionByZeroError());

        checkFields(3L, 2, BIGINT, 1L);
        checkFields(3L, 4, BIGINT, 3L);
        checkError(3L, 0, DATA_EXCEPTION, divisionByZeroError());

        checkFields(3L, 2L, BIGINT, 1L);
        checkFields(3L, 4L, BIGINT, 3L);
        checkError(3L, 0L, DATA_EXCEPTION, divisionByZeroError());

        checkFields(3L, decimal("1.4"), DECIMAL, decimal("0.2"));
        checkError(3L, decimal("0.0"), DATA_EXCEPTION, divisionByZeroError());

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
    public void testDecimal_BigInteger_left() {
        // NULL
        putAndCheckValue(BigInteger.ZERO, sql("this", "null"), DECIMAL, null);
        putAndCheckValue(BigInteger.ZERO, sql("null", "this"), DECIMAL, null);

        // Columns
        checkFields(new BigInteger("3"), (byte) 2, DECIMAL, decimal("1"));
        checkFields(new BigInteger("3"), (short) 2, DECIMAL, decimal("1"));
        checkFields(new BigInteger("3"), 2, DECIMAL, decimal("1"));
        checkFields(new BigInteger("3"), 2L, DECIMAL, decimal("1"));
        checkFields(new BigInteger("3"), new BigInteger("2"), DECIMAL, decimal("1"));
        checkFields(new BigInteger("3"), decimal("1.4"), DECIMAL, decimal("0.2"));

        checkError(new BigInteger("3"), (byte) 0, DATA_EXCEPTION, divisionByZeroError());
        checkError(new BigInteger("3"), (short) 0, DATA_EXCEPTION, divisionByZeroError());
        checkError(new BigInteger("3"), 0, DATA_EXCEPTION, divisionByZeroError());
        checkError(new BigInteger("3"), 0L, DATA_EXCEPTION, divisionByZeroError());
        checkError(new BigInteger("3"), BigInteger.ZERO, DATA_EXCEPTION, divisionByZeroError());
        checkError(new BigInteger("3"), BigDecimal.ZERO, DATA_EXCEPTION, divisionByZeroError());
        checkError(new BigInteger("3"), decimal("0"), DATA_EXCEPTION, divisionByZeroError());

        // Parameters
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), CHAR_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), STRING_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(new BigInteger("3"), sql("this", "?"), DECIMAL, decimal("1"), (byte) 2);
        putAndCheckValue(new BigInteger("3"), sql("this", "?"), DECIMAL, decimal("1"), (short) 2);
        putAndCheckValue(new BigInteger("3"), sql("this", "?"), DECIMAL, decimal("1"), 2);
        putAndCheckValue(new BigInteger("3"), sql("this", "?"), DECIMAL, decimal("1"), 2L);
        putAndCheckValue(new BigInteger("3"), sql("this", "?"), DECIMAL, decimal("1"), new BigInteger("2"));
        putAndCheckValue(new BigInteger("3"), sql("this", "?"), DECIMAL, decimal("0.2"), decimal("1.4"));
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, REAL), FLOAT_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, DOUBLE), DOUBLE_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure(BigInteger.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testDecimal_BigDecimal_left() {
        // NULL
        putAndCheckValue(BigDecimal.ZERO, sql("this", "null"), DECIMAL, null);
        putAndCheckValue(BigDecimal.ZERO, sql("null", "this"), DECIMAL, null);

        // Columns
        checkFields(new BigDecimal("3"), (byte) 2, DECIMAL, decimal("1"));
        checkFields(new BigDecimal("3"), (short) 2, DECIMAL, decimal("1"));
        checkFields(new BigDecimal("3"), 2, DECIMAL, decimal("1"));
        checkFields(new BigDecimal("3"), 2L, DECIMAL, decimal("1"));
        checkFields(new BigDecimal("3"), new BigInteger("2"), DECIMAL, decimal("1"));
        checkFields(new BigDecimal("3"), decimal("1.4"), DECIMAL, decimal("0.2"));

        checkError(new BigDecimal("3"), (byte) 0, DATA_EXCEPTION, divisionByZeroError());
        checkError(new BigDecimal("3"), (short) 0, DATA_EXCEPTION, divisionByZeroError());
        checkError(new BigDecimal("3"), 0, DATA_EXCEPTION, divisionByZeroError());
        checkError(new BigDecimal("3"), 0L, DATA_EXCEPTION, divisionByZeroError());
        checkError(new BigDecimal("3"), BigInteger.ZERO, DATA_EXCEPTION, divisionByZeroError());
        checkError(new BigDecimal("3"), BigDecimal.ZERO, DATA_EXCEPTION, divisionByZeroError());
        checkError(new BigDecimal("3"), decimal("0"), DATA_EXCEPTION, divisionByZeroError());

        // Parameters
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), CHAR_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), STRING_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, BOOLEAN), BOOLEAN_VAL);
        putAndCheckValue(new BigDecimal("3"), sql("this", "?"), DECIMAL, decimal("1"), (byte) 2);
        putAndCheckValue(new BigDecimal("3"), sql("this", "?"), DECIMAL, decimal("1"), (short) 2);
        putAndCheckValue(new BigDecimal("3"), sql("this", "?"), DECIMAL, decimal("1"), 2);
        putAndCheckValue(new BigDecimal("3"), sql("this", "?"), DECIMAL, decimal("1"), 2L);
        putAndCheckValue(new BigDecimal("3"), sql("this", "?"), DECIMAL, decimal("1"), new BigInteger("2"));
        putAndCheckValue(new BigDecimal("3"), sql("this", "?"), DECIMAL, decimal("0.2"), decimal("1.4"));
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, REAL), FLOAT_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, DOUBLE), DOUBLE_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, DATE), LOCAL_DATE_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, TIME), LOCAL_TIME_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        putAndCheckFailure(BigDecimal.ZERO, sql("this", "?"), DATA_EXCEPTION, parameterError(0, DECIMAL, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testReal_left() {
        checkUnsupportedForAllTypesCommute(1.0f, REAL);
    }

    @Test
    public void testDouble_left() {
        checkUnsupportedForAllTypesCommute(1.0d, DOUBLE);
    }

    @Test
    public void testTemporal_left() {
        checkUnsupportedForAllTypesCommute(LOCAL_DATE_VAL, DATE);
        checkUnsupportedForAllTypesCommute(LOCAL_TIME_VAL, TIME);
        checkUnsupportedForAllTypesCommute(LOCAL_DATE_TIME_VAL, TIMESTAMP);
        checkUnsupportedForAllTypesCommute(OFFSET_DATE_TIME_VAL, TIMESTAMP_WITH_TIME_ZONE);
    }

    @Override
    @Test
    public void testNullLiteral() {
        put(1);

        checkFailure0(sql("null", "null"), SqlErrorCode.PARSING, signatureError(NULL, NULL));

        checkFailure0(sql("'foo'", "null"), SqlErrorCode.PARSING, signatureError(VARCHAR, VARCHAR));
        checkFailure0(sql("null", "'foo'"), SqlErrorCode.PARSING, signatureError(VARCHAR, VARCHAR));

        checkFailure0(sql("true", "null"), SqlErrorCode.PARSING, signatureError(BOOLEAN, BOOLEAN));
        checkFailure0(sql("null", "true"), SqlErrorCode.PARSING, signatureError(BOOLEAN, BOOLEAN));

        checkValue0(sql("null", 1), TINYINT, null);
        checkValue0(sql(1, "null"), TINYINT, null);

        checkValue0(sql("null", Byte.MAX_VALUE), TINYINT, null);
        checkValue0(sql(Byte.MAX_VALUE, "null"), TINYINT, null);

        checkValue0(sql("null", Short.MAX_VALUE), SMALLINT, null);
        checkValue0(sql(Short.MAX_VALUE, "null"), SMALLINT, null);

        checkValue0(sql("null", Integer.MAX_VALUE), INTEGER, null);
        checkValue0(sql(Integer.MAX_VALUE, "null"), INTEGER, null);

        checkValue0(sql("null", Long.MAX_VALUE), BIGINT, null);
        checkValue0(sql(Long.MAX_VALUE, "null"), BIGINT, null);

        checkValue0(sql("null", "1.1"), DECIMAL, null);
        checkValue0(sql("1.1", "null"), DECIMAL, null);

        checkFailure0(sql("null", "1.1E1"), SqlErrorCode.PARSING, signatureError(DOUBLE, DOUBLE));
        checkFailure0(sql("1.1E1", "null"), SqlErrorCode.PARSING, signatureError(DOUBLE, DOUBLE));
    }

    @Test
    public void testEquality() {
        checkEquals(RemainderFunction.create(create(3, INT), create(2, INT), INT),
                RemainderFunction.create(create(3, INT), create(2, INT), INT), true);

        checkEquals(RemainderFunction.create(create(3, INT), create(2, INT), INT),
                RemainderFunction.create(create(3, INT), create(2, INT), QueryDataType.BIGINT), false);

        checkEquals(RemainderFunction.create(create(3, INT), create(2, INT), INT),
                RemainderFunction.create(create(3, INT), create(100, INT), INT), false);
    }

    @Test
    public void testSerialization() {
        RemainderFunction<?> original = RemainderFunction.create(create(3, INT), create(2, INT), INT);
        RemainderFunction<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_REMAINDER);

        checkEquals(original, restored, true);
    }

    private static String divisionByZeroError() {
        return "Division by zero";
    }
}
