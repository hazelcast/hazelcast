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

package com.hazelcast.jet.sql.impl.expression.string;

import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.string.TrimFunction;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue;
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
import static com.hazelcast.sql.SqlColumnType.TINYINT;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TrimFunctionIntegrationTest extends ExpressionTestSupport {
    @Test
    public void test2Arg() {
        // Columns
        putAndCheckValue(new ExpressionValue.StringVal(), "SELECT TRIM(LEADING field1) FROM map", VARCHAR, null);
        putAndCheckValue(new ExpressionValue.StringVal(), "SELECT TRIM(TRAILING field1) FROM map", VARCHAR, null);
        putAndCheckValue(new ExpressionValue.StringVal(), "SELECT TRIM(BOTH field1) FROM map", VARCHAR, null);

        putAndCheckValue(" abc ", "SELECT TRIM(LEADING this) FROM map", VARCHAR, "abc ");
        putAndCheckValue(" abc ", "SELECT TRIM(TRAILING this) FROM map", VARCHAR, " abc");
        putAndCheckValue(" abc ", "SELECT TRIM(BOTH this) FROM map", VARCHAR, "abc");

        putAndCheckFailure(true, "SELECT TRIM(LEADING this) FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", BOOLEAN));
        putAndCheckFailure((byte) 1, "SELECT TRIM(LEADING this) FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", TINYINT));
        putAndCheckFailure((short) 1, "SELECT TRIM(LEADING this) FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", SMALLINT));
        putAndCheckFailure(1, "SELECT TRIM(LEADING this) FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", INTEGER));
        putAndCheckFailure(1L, "SELECT TRIM(LEADING this) FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", BIGINT));
        putAndCheckFailure(BigInteger.ONE, "SELECT TRIM(LEADING this) FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", DECIMAL));
        putAndCheckFailure(BigDecimal.ONE, "SELECT TRIM(LEADING this) FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", DECIMAL));
        putAndCheckFailure(1f, "SELECT TRIM(LEADING this) FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", REAL));
        putAndCheckFailure(1d, "SELECT TRIM(LEADING this) FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", DOUBLE));
        putAndCheckFailure(LOCAL_DATE_VAL, "SELECT TRIM(LEADING this) FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", DATE));
        putAndCheckFailure(LOCAL_TIME_VAL, "SELECT TRIM(LEADING this) FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", TIME));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, "SELECT TRIM(LEADING this) FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", TIMESTAMP));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, "SELECT TRIM(LEADING this) FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(OBJECT_VAL, "SELECT TRIM(LEADING this) FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", OBJECT));

        // Literals
        checkValue0("SELECT TRIM(LEADING null) FROM map", VARCHAR, null);
        checkValue0("SELECT TRIM(TRAILING null) FROM map", VARCHAR, null);
        checkValue0("SELECT TRIM(BOTH null) FROM map", VARCHAR, null);

        checkValue0("SELECT TRIM(LEADING ' abc ') FROM map", VARCHAR, "abc ");
        checkValue0("SELECT TRIM(TRAILING ' abc ') FROM map", VARCHAR, " abc");
        checkValue0("SELECT TRIM(BOTH ' abc ') FROM map", VARCHAR, "abc");

        checkFailure0("SELECT TRIM(LEADING 1) FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", TINYINT));

        // Parameters
        checkValue0("SELECT TRIM(LEADING ?) FROM map", VARCHAR, null, new Object[]{null});
        checkValue0("SELECT TRIM(TRAILING ?) FROM map", VARCHAR, null, new Object[]{null});
        checkValue0("SELECT TRIM(BOTH ?) FROM map", VARCHAR, null, new Object[]{null});

        checkValue0("SELECT TRIM(LEADING ?) FROM map", VARCHAR, "abc ", " abc ");
        checkValue0("SELECT TRIM(TRAILING ?) FROM map", VARCHAR, " abc", " abc ");
        checkValue0("SELECT TRIM(BOTH ?) FROM map", VARCHAR, "abc", " abc ");

        checkFailure0("SELECT TRIM(LEADING ?) FROM map", SqlErrorCode.DATA_EXCEPTION, parameterError(0, VARCHAR, INTEGER), 1);
    }

    @Test
    public void test3Arg() {
        // Columns
        putAndCheckValue(new ExpressionValue.StringVal(), "SELECT TRIM(LEADING field1 FROM 'abab_c_abab') FROM map", VARCHAR, null);
        putAndCheckValue(new ExpressionValue.StringVal(), "SELECT TRIM(TRAILING field1 FROM 'abab_c_abab') FROM map", VARCHAR, null);
        putAndCheckValue(new ExpressionValue.StringVal(), "SELECT TRIM(BOTH field1 FROM 'abab_c_abab') FROM map", VARCHAR, null);

        putAndCheckValue("ab", "SELECT TRIM(LEADING this FROM 'abab_c_abab') FROM map", VARCHAR, "_c_abab");
        putAndCheckValue("ab", "SELECT TRIM(TRAILING this FROM 'abab_c_abab') FROM map", VARCHAR, "abab_c_");
        putAndCheckValue("ab", "SELECT TRIM(BOTH this FROM 'abab_c_abab') FROM map", VARCHAR, "_c_");

        putAndCheckFailure(true, "SELECT TRIM(LEADING this FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", BOOLEAN, VARCHAR));
        putAndCheckFailure((byte) 1, "SELECT TRIM(LEADING this FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", TINYINT, VARCHAR));
        putAndCheckFailure((short) 1, "SELECT TRIM(LEADING this FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", SMALLINT, VARCHAR));
        putAndCheckFailure(1, "SELECT TRIM(LEADING this FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", INTEGER, VARCHAR));
        putAndCheckFailure(1L, "SELECT TRIM(LEADING this FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", BIGINT, VARCHAR));
        putAndCheckFailure(BigInteger.ONE, "SELECT TRIM(LEADING this FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", DECIMAL, VARCHAR));
        putAndCheckFailure(BigDecimal.ONE, "SELECT TRIM(LEADING this FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", DECIMAL, VARCHAR));
        putAndCheckFailure(1f, "SELECT TRIM(LEADING this FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", REAL, VARCHAR));
        putAndCheckFailure(1d, "SELECT TRIM(LEADING this FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", DOUBLE, VARCHAR));
        putAndCheckFailure(LOCAL_DATE_VAL, "SELECT TRIM(LEADING this FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", DATE, VARCHAR));
        putAndCheckFailure(LOCAL_TIME_VAL, "SELECT TRIM(LEADING this FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", TIME, VARCHAR));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, "SELECT TRIM(LEADING this FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", TIMESTAMP, VARCHAR));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, "SELECT TRIM(LEADING this FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", TIMESTAMP_WITH_TIME_ZONE, VARCHAR));
        putAndCheckFailure(OBJECT_VAL, "SELECT TRIM(LEADING this FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", OBJECT, VARCHAR));

        // Literals
        checkValue0("SELECT TRIM(LEADING null FROM 'abab_c_abab') FROM map", VARCHAR, null);
        checkValue0("SELECT TRIM(TRAILING null FROM 'abab_c_abab') FROM map", VARCHAR, null);
        checkValue0("SELECT TRIM(BOTH null FROM 'abab_c_abab') FROM map", VARCHAR, null);

        checkValue0("SELECT TRIM(LEADING 'ab' FROM 'abab_c_abab') FROM map", VARCHAR, "_c_abab");
        checkValue0("SELECT TRIM(TRAILING 'ab' FROM 'abab_c_abab') FROM map", VARCHAR, "abab_c_");
        checkValue0("SELECT TRIM(BOTH 'ab' FROM 'abab_c_abab') FROM map", VARCHAR, "_c_");

        checkFailure0("SELECT TRIM(LEADING true FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", BOOLEAN, VARCHAR));
        checkFailure0("SELECT TRIM(LEADING 1 FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", TINYINT, VARCHAR));
        checkFailure0("SELECT TRIM(LEADING 1.1 FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", DECIMAL, VARCHAR));
        checkFailure0("SELECT TRIM(LEADING 1.1E1 FROM 'abab_c_abab') FROM map", SqlErrorCode.PARSING, signatureErrorFunction("TRIM", DOUBLE, VARCHAR));

        // Parameters
        checkValue0("SELECT TRIM(LEADING ? FROM 'abab_c_abab') FROM map", VARCHAR, "_c_abab", "ab");
        checkValue0("SELECT TRIM(TRAILING ? FROM 'abab_c_abab') FROM map", VARCHAR, "abab_c_", "ab");
        checkValue0("SELECT TRIM(BOTH ? FROM 'abab_c_abab') FROM map", VARCHAR, "_c_", "ab");

        checkFailure0("SELECT TRIM(LEADING ? FROM 'abab_c_abab') FROM map", SqlErrorCode.DATA_EXCEPTION, parameterError(0, VARCHAR, BOOLEAN), true);
        checkFailure0("SELECT TRIM(LEADING ? FROM 'abab_c_abab') FROM map", SqlErrorCode.DATA_EXCEPTION, parameterError(0, VARCHAR, TINYINT), (byte) 1);
        checkFailure0("SELECT TRIM(LEADING ? FROM 'abab_c_abab') FROM map", SqlErrorCode.DATA_EXCEPTION, parameterError(0, VARCHAR, SMALLINT), (short) 1);
        checkFailure0("SELECT TRIM(LEADING ? FROM 'abab_c_abab') FROM map", SqlErrorCode.DATA_EXCEPTION, parameterError(0, VARCHAR, INTEGER), 1);
        checkFailure0("SELECT TRIM(LEADING ? FROM 'abab_c_abab') FROM map", SqlErrorCode.DATA_EXCEPTION, parameterError(0, VARCHAR, BIGINT), 1L);
        checkFailure0("SELECT TRIM(LEADING ? FROM 'abab_c_abab') FROM map", SqlErrorCode.DATA_EXCEPTION, parameterError(0, VARCHAR, DECIMAL), BigInteger.ONE);
        checkFailure0("SELECT TRIM(LEADING ? FROM 'abab_c_abab') FROM map", SqlErrorCode.DATA_EXCEPTION, parameterError(0, VARCHAR, DECIMAL), BigDecimal.ONE);
        checkFailure0("SELECT TRIM(LEADING ? FROM 'abab_c_abab') FROM map", SqlErrorCode.DATA_EXCEPTION, parameterError(0, VARCHAR, REAL), 1f);
        checkFailure0("SELECT TRIM(LEADING ? FROM 'abab_c_abab') FROM map", SqlErrorCode.DATA_EXCEPTION, parameterError(0, VARCHAR, DOUBLE), 1d);
        checkFailure0("SELECT TRIM(LEADING ? FROM 'abab_c_abab') FROM map", SqlErrorCode.DATA_EXCEPTION, parameterError(0, VARCHAR, DATE), LOCAL_DATE_VAL);
        checkFailure0("SELECT TRIM(LEADING ? FROM 'abab_c_abab') FROM map", SqlErrorCode.DATA_EXCEPTION, parameterError(0, VARCHAR, TIME), LOCAL_TIME_VAL);
        checkFailure0("SELECT TRIM(LEADING ? FROM 'abab_c_abab') FROM map", SqlErrorCode.DATA_EXCEPTION, parameterError(0, VARCHAR, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        checkFailure0("SELECT TRIM(LEADING ? FROM 'abab_c_abab') FROM map", SqlErrorCode.DATA_EXCEPTION, parameterError(0, VARCHAR, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        checkFailure0("SELECT TRIM(LEADING ? FROM 'abab_c_abab') FROM map", SqlErrorCode.DATA_EXCEPTION, parameterError(0, VARCHAR, OBJECT), OBJECT_VAL);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testEquals() {
        Expression<?> input1 = constant("a");
        Expression<?> input2 = constant("b");

        Expression<?> characters1 = constant("c");
        Expression<?> characters2 = constant("d");

        boolean leading1 = true;
        boolean leading2 = false;

        boolean trailing1 = true;
        boolean trailing2 = false;

        TrimFunction function = TrimFunction.create(input1, characters1, leading1, trailing1);

        checkEquals(function, TrimFunction.create(input1, characters1, leading1, trailing1), true);
        checkEquals(function, TrimFunction.create(input2, characters1, leading1, trailing1), false);
        checkEquals(function, TrimFunction.create(input1, characters2, leading1, trailing1), false);
        checkEquals(function, TrimFunction.create(input1, characters1, leading2, trailing1), false);
        checkEquals(function, TrimFunction.create(input1, characters1, leading1, trailing2), false);
    }

    @Test
    public void testSerialization() {
        TrimFunction original = TrimFunction.create(constant("a"), constant("b"), true, true);
        TrimFunction restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_TRIM);

        checkEquals(original, restored, true);
    }

    @Test
    public void testSimplification() {
        TrimFunction function = TrimFunction.create(constant("a"), constant("b"), true, true);
        assertEquals(ConstantExpression.create("b", QueryDataType.VARCHAR), function.getCharacters());

        function = TrimFunction.create(constant("a"), constant(" "), true, true);
        assertNull(function.getCharacters());

        function = TrimFunction.create(constant("a"), null, true, true);
        assertNull(function.getCharacters());
    }

    private ConstantExpression<?> constant(String value) {
        return ConstantExpression.create(value, QueryDataType.VARCHAR);
    }
}
