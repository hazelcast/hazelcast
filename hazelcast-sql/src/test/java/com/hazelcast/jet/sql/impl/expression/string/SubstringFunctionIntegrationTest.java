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

import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.string.SubstringFunction;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;

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
import static com.hazelcast.sql.impl.SqlErrorCode.DATA_EXCEPTION;
import static com.hazelcast.sql.impl.SqlErrorCode.PARSING;
import static java.util.Arrays.asList;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@SuppressWarnings("SpellCheckingInspection")
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SubstringFunctionIntegrationTest extends ExpressionTestSupport {
    @Parameterized.Parameter
    public boolean useFunctionalSyntax;

    @Parameterized.Parameters(name = "useFunctionalSyntax:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true},
                {false},
        });
    }

    @Test
    public void testLogic() {
        putAndCheckValue("abcde", sql2("null", "1"), VARCHAR, null);
        putAndCheckValue("abcde", sql2("null", "null"), VARCHAR, null);
        putAndCheckValue("abcde", sql2("this", "null"), VARCHAR, null);
        putAndCheckValue("abcde", sql2("this", "1"), VARCHAR, "abcde");
        putAndCheckValue("abcde", sql2("this", "2"), VARCHAR, "bcde");
        putAndCheckValue("abcde", sql2("this", "5"), VARCHAR, "e");
        putAndCheckValue("abcde", sql2("this", "6"), VARCHAR, "");
        putAndCheckValue("abcde", sql2("this", "7"), VARCHAR, "");
        putAndCheckFailure("abcde", sql2("this", "0"), DATA_EXCEPTION, "SUBSTRING \"start\" operand must be positive");
        putAndCheckFailure("abcde", sql2("this", "-1"), DATA_EXCEPTION, "SUBSTRING \"start\" operand must be positive");

        putAndCheckValue("abcde", sql3("this", "1", "null"), VARCHAR, null);
        putAndCheckValue("abcde", sql3("this", "null", "1"), VARCHAR, null);
        putAndCheckValue("abcde", sql3("this", "null", "null"), VARCHAR, null);
        putAndCheckValue("abcde", sql3("null", "null", "null"), VARCHAR, null);
        putAndCheckValue("abcde", sql3("this", "1", "0"), VARCHAR, "");
        putAndCheckValue("abcde", sql3("this", "1", "1"), VARCHAR, "a");
        putAndCheckValue("abcde", sql3("this", "1", "2"), VARCHAR, "ab");
        putAndCheckValue("abcde", sql3("this", "1", "5"), VARCHAR, "abcde");
        putAndCheckValue("abcde", sql3("this", "1", "6"), VARCHAR, "abcde");
        putAndCheckValue("abcde", sql3("this", "5", "6"), VARCHAR, "e");
        putAndCheckFailure("abcde", sql3("this", "1", "-1"), DATA_EXCEPTION, "SUBSTRING \"length\" operand cannot be negative");
    }

    @Test
    public void testArg1() {
        // Column
        putAndCheckValue("abcde", sql2("this", "2"), VARCHAR, "bcde");
        putAndCheckValue('a', sql2("this", "1"), VARCHAR, "a");
        putAndCheckFailure(true, sql2("this", "1"), PARSING, signatureError(BOOLEAN, INTEGER));
        putAndCheckFailure((byte) 1, sql2("this", "1"), PARSING, signatureError(TINYINT, INTEGER));
        putAndCheckFailure((short) 1, sql2("this", "1"), PARSING, signatureError(SMALLINT, INTEGER));
        putAndCheckFailure(1, sql2("this", "1"), PARSING, signatureError(INTEGER, INTEGER));
        putAndCheckFailure(1L, sql2("this", "1"), PARSING, signatureError(BIGINT, INTEGER));
        putAndCheckFailure(BigInteger.ONE, sql2("this", "1"), PARSING, signatureError(DECIMAL, INTEGER));
        putAndCheckFailure(BigDecimal.ONE, sql2("this", "1"), PARSING, signatureError(DECIMAL, INTEGER));
        putAndCheckFailure(1f, sql2("this", "1"), PARSING, signatureError(REAL, INTEGER));
        putAndCheckFailure(1d, sql2("this", "1"), PARSING, signatureError(DOUBLE, INTEGER));
        putAndCheckFailure(LOCAL_DATE_VAL, sql2("this", "1"), PARSING, signatureError(DATE, INTEGER));
        putAndCheckFailure(LOCAL_TIME_VAL, sql2("this", "1"), PARSING, signatureError(TIME, INTEGER));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql2("this", "1"), PARSING, signatureError(TIMESTAMP, INTEGER));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql2("this", "1"), PARSING, signatureError(TIMESTAMP_WITH_TIME_ZONE, INTEGER));
        putAndCheckFailure(OBJECT_VAL, sql2("field1", "1"), PARSING, signatureError(OBJECT, INTEGER));

        // Literal
        checkValue0(sql2("null", "1"), VARCHAR, null);
        checkValue0(sql2("'abcde'", "2"), VARCHAR, "bcde");
        checkFailure0(sql2("true", "1"), PARSING, signatureError(BOOLEAN, INTEGER));
        checkFailure0(sql2("1", "1"), PARSING, signatureError(TINYINT, INTEGER));
        checkFailure0(sql2("1.1", "1"), PARSING, signatureError(DECIMAL, INTEGER));
        checkFailure0(sql2("1.1E1", "1"), PARSING, signatureError(DOUBLE, INTEGER));

        // Parameter
        checkValue0(sql2("?", "1"), VARCHAR, null, (String) null);
        checkValue0(sql2("?", "2"), VARCHAR, "bcde", "abcde");
        checkValue0(sql2("?", "1"), VARCHAR, "a", 'a');
        checkFailure0(sql2("?", "1"), DATA_EXCEPTION, parameterError(0, VARCHAR, BOOLEAN), true);
        checkFailure0(sql2("?", "1"), DATA_EXCEPTION, parameterError(0, VARCHAR, TINYINT), (byte) 1);
        checkFailure0(sql2("?", "1"), DATA_EXCEPTION, parameterError(0, VARCHAR, SMALLINT), (short) 1);
        checkFailure0(sql2("?", "1"), DATA_EXCEPTION, parameterError(0, VARCHAR, INTEGER), 1);
        checkFailure0(sql2("?", "1"), DATA_EXCEPTION, parameterError(0, VARCHAR, BIGINT), 1L);
        checkFailure0(sql2("?", "1"), DATA_EXCEPTION, parameterError(0, VARCHAR, DECIMAL), BigInteger.ONE);
        checkFailure0(sql2("?", "1"), DATA_EXCEPTION, parameterError(0, VARCHAR, DECIMAL), BigDecimal.ONE);
        checkFailure0(sql2("?", "1"), DATA_EXCEPTION, parameterError(0, VARCHAR, REAL), 1f);
        checkFailure0(sql2("?", "1"), DATA_EXCEPTION, parameterError(0, VARCHAR, DOUBLE), 1d);
        checkFailure0(sql2("?", "1"), DATA_EXCEPTION, parameterError(0, VARCHAR, DATE), LOCAL_DATE_VAL);
        checkFailure0(sql2("?", "1"), DATA_EXCEPTION, parameterError(0, VARCHAR, TIME), LOCAL_TIME_VAL);
        checkFailure0(sql2("?", "1"), DATA_EXCEPTION, parameterError(0, VARCHAR, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        checkFailure0(sql2("?", "1"), DATA_EXCEPTION, parameterError(0, VARCHAR, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        checkFailure0(sql2("?", "1"), DATA_EXCEPTION, parameterError(0, VARCHAR, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testArg2() {
        // Column
        putAndCheckFailure(true, sql2("'abcde'", "this"), PARSING, signatureError(VARCHAR, BOOLEAN));
        putAndCheckValue((byte) 2, sql2("'abcde'", "this"), VARCHAR, "bcde");
        putAndCheckValue((short) 2, sql2("'abcde'", "this"), VARCHAR, "bcde");
        putAndCheckValue(2, sql2("'abcde'", "this"), VARCHAR, "bcde");
        putAndCheckFailure(2L, sql2("'abcde'", "this"), PARSING, signatureError(VARCHAR, BIGINT));
        putAndCheckFailure(BigInteger.ONE, sql2("'abcde'", "this"), PARSING, signatureError(VARCHAR, DECIMAL));
        putAndCheckFailure(BigDecimal.ONE, sql2("'abcde'", "this"), PARSING, signatureError(VARCHAR, DECIMAL));
        putAndCheckFailure(2f, sql2("'abcde'", "this"), PARSING, signatureError(VARCHAR, REAL));
        putAndCheckFailure(2d, sql2("'abcde'", "this"), PARSING, signatureError(VARCHAR, DOUBLE));
        putAndCheckFailure(LOCAL_DATE_VAL, sql2("'abcde'", "this"), PARSING, signatureError(VARCHAR, DATE));
        putAndCheckFailure(LOCAL_TIME_VAL, sql2("'abcde'", "this"), PARSING, signatureError(VARCHAR, TIME));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql2("'abcde'", "this"), PARSING, signatureError(VARCHAR, TIMESTAMP));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql2("'abcde'", "this"), PARSING, signatureError(VARCHAR, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(OBJECT_VAL, sql2("'abcde'", "this"), PARSING, signatureError(VARCHAR, OBJECT));

        // Literal
        checkValue0(sql2("'abcde'", "null"), VARCHAR, null);
        checkFailure0(sql2("'abcde'", "true"), PARSING, signatureError(VARCHAR, BOOLEAN));
        checkFailure0(sql2("'abcde'", "'2'"), PARSING, signatureError(VARCHAR, VARCHAR));
        checkValue0(sql2("'abcde'", "2"), VARCHAR, "bcde");
        checkFailure0(sql2("'abcde'", "2.2"), PARSING, signatureError(VARCHAR, DECIMAL));
        checkFailure0(sql2("'abcde'", "2.2E2"), PARSING, signatureError(VARCHAR, DOUBLE));

        // Parameter
        checkValue0(sql2("'abcde'", "?"), VARCHAR, null, (Integer) null);
        checkFailure0(sql2("'abcde'", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, BOOLEAN), true);
        checkFailure0(sql2("'abcde'", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, VARCHAR), "2");
        checkFailure0(sql2("'abcde'", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, VARCHAR), '2');
        checkValue0(sql2("'abcde'", "?"), VARCHAR, "bcde", (byte) 2);
        checkValue0(sql2("'abcde'", "?"), VARCHAR, "bcde", (short) 2);
        checkValue0(sql2("'abcde'", "?"), VARCHAR, "bcde", 2);
        checkValue0(sql2("'abcde'", "?"), VARCHAR, "bcde", 2L);
        checkFailure0(sql2("'abcde'", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, DECIMAL), BigInteger.ONE);
        checkFailure0(sql2("'abcde'", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, DECIMAL), BigDecimal.ONE);
        checkFailure0(sql2("'abcde'", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, REAL), 2f);
        checkFailure0(sql2("'abcde'", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, DOUBLE), 2d);
        checkFailure0(sql2("'abcde'", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, DATE), LOCAL_DATE_VAL);
        checkFailure0(sql2("'abcde'", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, TIME), LOCAL_TIME_VAL);
        checkFailure0(sql2("'abcde'", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        checkFailure0(sql2("'abcde'", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        checkFailure0(sql2("'abcde'", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testArg3() {
        // Column
        putAndCheckFailure(true, sql3("'abcde'", "2", "this"), PARSING, signatureError(VARCHAR, INTEGER, BOOLEAN));
        putAndCheckValue((byte) 2, sql3("'abcde'", "2", "this"), VARCHAR, "bc");
        putAndCheckValue((short) 2, sql3("'abcde'", "2", "this"), VARCHAR, "bc");
        putAndCheckValue(2, sql3("'abcde'", "2", "this"), VARCHAR, "bc");
        putAndCheckFailure(2L, sql3("'abcde'", "2", "this"), PARSING, signatureError(VARCHAR, INTEGER, BIGINT));
        putAndCheckFailure(BigInteger.ONE, sql3("'abcde'", "2", "this"), PARSING, signatureError(VARCHAR, INTEGER, DECIMAL));
        putAndCheckFailure(BigDecimal.ONE, sql3("'abcde'", "2", "this"), PARSING, signatureError(VARCHAR, INTEGER, DECIMAL));
        putAndCheckFailure(2f, sql3("'abcde'", "2", "this"), PARSING, signatureError(VARCHAR, INTEGER, REAL));
        putAndCheckFailure(2d, sql3("'abcde'", "2", "this"), PARSING, signatureError(VARCHAR, INTEGER, DOUBLE));
        putAndCheckFailure(LOCAL_DATE_VAL, sql3("'abcde'", "2", "this"), PARSING, signatureError(VARCHAR, INTEGER, DATE));
        putAndCheckFailure(LOCAL_TIME_VAL, sql3("'abcde'", "2", "this"), PARSING, signatureError(VARCHAR, INTEGER, TIME));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql3("'abcde'", "2", "this"), PARSING, signatureError(VARCHAR, INTEGER, TIMESTAMP));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql3("'abcde'", "2", "this"), PARSING, signatureError(VARCHAR, INTEGER, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(OBJECT_VAL, sql3("'abcde'", "2", "this"), PARSING, signatureError(VARCHAR, INTEGER, OBJECT));

        // Literal
        checkValue0(sql3("'abcde'", "2", "null"), VARCHAR, null);
        checkFailure0(sql3("'abcde'", "2", "true"), PARSING, signatureError(VARCHAR, INTEGER, BOOLEAN));
        checkFailure0(sql3("'abcde'", "2", "'2'"), PARSING, signatureError(VARCHAR, INTEGER, VARCHAR));
        checkValue0(sql3("'abcde'", "2", "2"), VARCHAR, "bc");
        checkFailure0(sql3("'abcde'", "2", "2.2"), PARSING, signatureError(VARCHAR, INTEGER, DECIMAL));
        checkFailure0(sql3("'abcde'", "2", "2.2E2"), PARSING, signatureError(VARCHAR, INTEGER, DOUBLE));

        // Parameter
        checkValue0(sql3("'abcde'", "2", "?"), VARCHAR, null, (Integer) null);
        checkFailure0(sql3("'abcde'", "2", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, BOOLEAN), true);
        checkFailure0(sql3("'abcde'", "2", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, VARCHAR), "2");
        checkFailure0(sql3("'abcde'", "2", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, VARCHAR), '2');
        checkValue0(sql3("'abcde'", "2", "?"), VARCHAR, "bc", (byte) 2);
        checkValue0(sql3("'abcde'", "2", "?"), VARCHAR, "bc", (short) 2);
        checkValue0(sql3("'abcde'", "2", "?"), VARCHAR, "bc", 2);
        checkValue0(sql3("'abcde'", "2", "?"), VARCHAR, "bc", 2L);
        checkFailure0(sql3("'abcde'", "2", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, DECIMAL), BigInteger.ONE);
        checkFailure0(sql3("'abcde'", "2", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, DECIMAL), BigDecimal.ONE);
        checkFailure0(sql3("'abcde'", "2", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, REAL), 2f);
        checkFailure0(sql3("'abcde'", "2", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, DOUBLE), 2d);
        checkFailure0(sql3("'abcde'", "2", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, DATE), LOCAL_DATE_VAL);
        checkFailure0(sql3("'abcde'", "2", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, TIME), LOCAL_TIME_VAL);
        checkFailure0(sql3("'abcde'", "2", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        checkFailure0(sql3("'abcde'", "2", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        checkFailure0(sql3("'abcde'", "2", "?"), DATA_EXCEPTION, parameterError(0, INTEGER, OBJECT), OBJECT_VAL);
    }

    private String sql2(String inputOperand, String fromOperand) {
        if (useFunctionalSyntax) {
            return "SELECT SUBSTRING(" + inputOperand + ", " + fromOperand + ") FROM map";
        } else {
            return "SELECT SUBSTRING(" + inputOperand + " FROM " + fromOperand + ") FROM map";
        }
    }

    private String sql3(String inputOperand, String fromOperand, String forOperand) {
        if (useFunctionalSyntax) {
            return "SELECT SUBSTRING(" + inputOperand + ", " + fromOperand + ", " + forOperand + ") FROM map";
        } else {
            return "SELECT SUBSTRING(" + inputOperand + " FROM " + fromOperand + " FOR " + forOperand + ") FROM map";
        }
    }

    @Test
    public void testEquals() {
        Expression<?> input1 = ConstantExpression.create("a", QueryDataType.VARCHAR);
        Expression<?> input2 = ConstantExpression.create("b", QueryDataType.VARCHAR);

        Expression<?> start1 = ConstantExpression.create(1, QueryDataType.INT);
        Expression<?> start2 = ConstantExpression.create(2, QueryDataType.INT);

        Expression<?> length1 = ConstantExpression.create(10, QueryDataType.INT);
        Expression<?> length2 = ConstantExpression.create(20, QueryDataType.INT);

        SubstringFunction function = SubstringFunction.create(input1, start1, length1);

        checkEquals(function, SubstringFunction.create(input1, start1, length1), true);
        checkEquals(function, SubstringFunction.create(input2, start1, length1), false);
        checkEquals(function, SubstringFunction.create(input1, start2, length1), false);
        checkEquals(function, SubstringFunction.create(input1, start1, length2), false);
    }

    @Test
    public void testSerialization() {
        SubstringFunction original = SubstringFunction.create(
                ConstantExpression.create("a", QueryDataType.VARCHAR),
                ConstantExpression.create(1, QueryDataType.INT),
                ConstantExpression.create(10, QueryDataType.INT)
        );

        SubstringFunction restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_SUBSTRING);

        checkEquals(original, restored, true);
    }

    private static String signatureError(SqlColumnType... columnTypes) {
        return signatureErrorFunction("SUBSTRING", columnTypes);
    }
}
