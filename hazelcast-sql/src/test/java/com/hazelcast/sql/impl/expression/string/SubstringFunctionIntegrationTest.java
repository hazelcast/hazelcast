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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;

import static java.util.Arrays.asList;

@SuppressWarnings("SpellCheckingInspection")
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SubstringFunctionIntegrationTest extends ExpressionTestSupport {

    @Parameterized.Parameter
    public boolean useFunctionalSyntax;

    @Parameterized.Parameters(name = "useFunctionalSyntax:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
            { true },
            { false },
        });
    }

    @Test
    public void test_input() {
        // String column
        put("abcde");
        checkValue0(sql("this", "1"), SqlColumnType.VARCHAR, "abcde");
        checkValue0(sql("this", "1", "1"), SqlColumnType.VARCHAR, "a");
        checkValue0(sql("this", "1", "2"), SqlColumnType.VARCHAR, "ab");
        checkValue0(sql("this", "1", "5"), SqlColumnType.VARCHAR, "abcde");
        checkValue0(sql("this", "1", "6"), SqlColumnType.VARCHAR, "abcde");
        checkFailure0(sql("this", "1", "-1"), SqlErrorCode.DATA_EXCEPTION, "SUBSTRING \"length\" operand cannot be negative");

        // Character column
        put('a');
        checkValue0(sql("this", "1"), SqlColumnType.VARCHAR, "a");
        checkValue0(sql("this", "2"), SqlColumnType.VARCHAR, "");
        checkValue0(sql("this", "1", "1"), SqlColumnType.VARCHAR, "a");

        // Null value
        put(new ExpressionValue.StringVal().field1(null));
        checkValue0(sql("field1", "1"), SqlColumnType.VARCHAR, null);

        // Other columns
        put(true);
        checkFailure0(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [BOOLEAN, INTEGER] to the 'SUBSTRING' function");

        put((byte) 1);
        checkFailure0(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [TINYINT, INTEGER] to the 'SUBSTRING' function");

        put((short) 2);
        checkFailure0(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [SMALLINT, INTEGER] to the 'SUBSTRING' function");

        put(3);
        checkFailure0(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [INTEGER, INTEGER] to the 'SUBSTRING' function");

        put(4L);
        checkFailure0(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [BIGINT, INTEGER] to the 'SUBSTRING' function");

        put(new BigInteger("5"));
        checkFailure0(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [DECIMAL, INTEGER] to the 'SUBSTRING' function");

        put(new BigDecimal("6"));
        checkFailure0(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [DECIMAL, INTEGER] to the 'SUBSTRING' function");

        put(7f);
        checkFailure0(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [REAL, INTEGER] to the 'SUBSTRING' function");

        put(8d);
        checkFailure0(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [DOUBLE, INTEGER] to the 'SUBSTRING' function");

        // Parameter
        put(1);
        checkValue0(sql("?", "1"), SqlColumnType.VARCHAR, "abcde", "abcde");
        checkValue0(sql("?", "1"), SqlColumnType.VARCHAR, "a", 'a');
        checkValue0(sql("?", "1"), SqlColumnType.VARCHAR, null, new Object[] { null });

        checkFailure0(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", (byte) 1);
        checkFailure0(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", (short) 1);
        checkFailure0(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", 1);
        checkFailure0(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", 1L);
        checkFailure0(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", BigInteger.ONE);
        checkFailure0(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", BigDecimal.ONE);
        checkFailure0(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", 1f);
        checkFailure0(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", 1d);
        checkFailure0(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", new ExpressionValue.ObjectVal());
        checkFailure0(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", LOCAL_DATE_VAL);
        checkFailure0(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", LOCAL_TIME_VAL);
        checkFailure0(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", LOCAL_DATE_TIME_VAL);
        checkFailure0(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", OFFSET_DATE_TIME_VAL);

        // Literal
        checkValue0(sql("'abc'", "1"), SqlColumnType.VARCHAR, "abc");
        checkFailure0(sql("1", "1"), SqlErrorCode.PARSING, "Cannot apply [TINYINT, INTEGER] to the 'SUBSTRING' function");
    }

    @Test
    public void test_start() {
        // Different values
        put("abcde");
        checkValue0(sql("this", "null"), SqlColumnType.VARCHAR, null);
        checkValue0(sql("this", "1"), SqlColumnType.VARCHAR, "abcde");
        checkValue0(sql("this", "2"), SqlColumnType.VARCHAR, "bcde");
        checkValue0(sql("this", "5"), SqlColumnType.VARCHAR, "e");
        checkValue0(sql("this", "6"), SqlColumnType.VARCHAR, "");
        checkValue0(sql("this", "10"), SqlColumnType.VARCHAR, "");
        checkFailure0(sql("this", "0"), SqlErrorCode.DATA_EXCEPTION, "SUBSTRING \"start\" operand must be positive");
        checkFailure0(sql("this", "-1"), SqlErrorCode.DATA_EXCEPTION, "SUBSTRING \"start\" operand must be positive");

        // Columns
        put(new ExpressionValue.IntegerVal());
        checkValue0(sql("'abcde'", "field1"), SqlColumnType.VARCHAR, null);

        put(true);
        checkFailure0(sql("'abcde'", "this"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, BOOLEAN] to the 'SUBSTRING' function (consider adding an explicit CAST)");

        put((byte) 2);
        checkValue0(sql("'abcde'", "this"), SqlColumnType.VARCHAR, "bcde");

        put((short) 2);
        checkValue0(sql("'abcde'", "this"), SqlColumnType.VARCHAR, "bcde");

        put(2);
        checkValue0(sql("'abcde'", "this"), SqlColumnType.VARCHAR, "bcde");

        put(2L);
        checkFailure0(sql("'abcde'", "this"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, BIGINT] to the 'SUBSTRING' function");

        put("2");
        checkFailure0(sql("'abcde'", "this"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, VARCHAR] to the 'SUBSTRING' function");

        put('2');
        checkFailure0(sql("'abcde'", "this"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, VARCHAR] to the 'SUBSTRING' function");

        // Parameters
        put("abcde");
        checkValue0(sql("this", "?"), SqlColumnType.VARCHAR, null, new Object[] { null});
        checkValue0(sql("this", "?"), SqlColumnType.VARCHAR, "bcde", (byte) 2);
        checkValue0(sql("this", "?"), SqlColumnType.VARCHAR, "bcde", (short) 2);
        checkValue0(sql("this", "?"), SqlColumnType.VARCHAR, "bcde", 2);
        checkFailure0(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from VARCHAR to INTEGER", "2");
        checkFailure0(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from VARCHAR to INTEGER", '2');
        checkFailure0(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from BOOLEAN to INTEGER", true);
        checkFailure0(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from BIGINT to INTEGER", 2L);
        checkFailure0(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to INTEGER", BigInteger.ONE);
        checkFailure0(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to INTEGER", BigDecimal.ONE);
        checkFailure0(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from REAL to INTEGER", 2f);
        checkFailure0(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DOUBLE to INTEGER", 2d);
        checkFailure0(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DATE to INTEGER", LOCAL_DATE_VAL);
        checkFailure0(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIME to INTEGER", LOCAL_TIME_VAL);
        checkFailure0(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP to INTEGER", LOCAL_DATE_TIME_VAL);
        checkFailure0(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP_WITH_TIME_ZONE to INTEGER", OFFSET_DATE_TIME_VAL);

        // Literals
        put("abcde");
        checkValue0(sql("this", "2"), SqlColumnType.VARCHAR, "bcde");
        checkFailure0(sql("this", "'2'"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, VARCHAR] to the 'SUBSTRING' function (consider adding an explicit CAST)");
        checkValue0(sql("this", "null"), SqlColumnType.VARCHAR, null);
        checkFailure0(sql("this", "true"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, BOOLEAN] to the 'SUBSTRING' function (consider adding an explicit CAST)");
    }

    @Test
    public void test_length() {
        // Different values
        put(1);
        checkValue0(sql("'abcde'", "2", "0"), SqlColumnType.VARCHAR, "");
        checkValue0(sql("'abcde'", "2", "2"), SqlColumnType.VARCHAR, "bc");
        checkValue0(sql("'abcde'", "2", "10"), SqlColumnType.VARCHAR, "bcde");
        checkValue0(sql("'abcde'", "2", "null"), SqlColumnType.VARCHAR, null);
        checkFailure0(sql("'abcde'", "2", "-1"), SqlErrorCode.DATA_EXCEPTION, "SUBSTRING \"length\" operand cannot be negative");

        // Columns
        put(new ExpressionValue.IntegerVal());
        checkValue0(sql("'abcde'", "2", "field1"), SqlColumnType.VARCHAR, null);

        put(true);
        checkFailure0(sql("'abcde'", "2", "this"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, INTEGER, BOOLEAN] to the 'SUBSTRING' function (consider adding an explicit CAST)");

        put((byte) 2);
        checkValue0(sql("'abcde'", "2", "this"), SqlColumnType.VARCHAR, "bc");

        put((short) 2);
        checkValue0(sql("'abcde'", "2", "this"), SqlColumnType.VARCHAR, "bc");

        put(2);
        checkValue0(sql("'abcde'", "2", "this"), SqlColumnType.VARCHAR, "bc");

        put(2L);
        checkFailure0(sql("'abcde'", "2", "this"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, INTEGER, BIGINT] to the 'SUBSTRING' function");

        // Parameters
        put(1);
        checkValue0(sql("'abcde'", "2", "?"), SqlColumnType.VARCHAR, null, new Object[] { null });
        checkValue0(sql("'abcde'", "2", "?"), SqlColumnType.VARCHAR, "bc", (byte) 2);
        checkValue0(sql("'abcde'", "2", "?"), SqlColumnType.VARCHAR, "bc", (short) 2);
        checkValue0(sql("'abcde'", "2", "?"), SqlColumnType.VARCHAR, "bc", 2);
        checkFailure0(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from VARCHAR to INTEGER", "2");
        checkFailure0(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from VARCHAR to INTEGER", '2');
        checkFailure0(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from BOOLEAN to INTEGER", true);
        checkFailure0(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from BIGINT to INTEGER", 2L);
        checkFailure0(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to INTEGER", new BigInteger("2"));
        checkFailure0(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to INTEGER", new BigDecimal("2"));
        checkFailure0(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from REAL to INTEGER", 2f);
        checkFailure0(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DOUBLE to INTEGER", 2d);
        checkFailure0(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DATE to INTEGER", LOCAL_DATE_VAL);
        checkFailure0(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIME to INTEGER", LOCAL_TIME_VAL);
        checkFailure0(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP to INTEGER", LOCAL_DATE_TIME_VAL);
        checkFailure0(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP_WITH_TIME_ZONE to INTEGER", OFFSET_DATE_TIME_VAL);

        // Literals
        put(1);

        checkValue0(sql("'abcde'", "2", "2"), SqlColumnType.VARCHAR, "bc");
        checkFailure0(sql("'abcde'", "2", "'2'"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, INTEGER, VARCHAR] to the 'SUBSTRING' function");
        checkValue0(sql("'abcde'", "2", "null"), SqlColumnType.VARCHAR, null);
        checkFailure0(sql("'abcde'", "2", "true"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, INTEGER, BOOLEAN] to the 'SUBSTRING' function (consider adding an explicit CAST)");
    }

    @Test
    public void test_parameters_only() {
        put(1);
        checkValue0(sql("?", "?"), SqlColumnType.VARCHAR, "bcde", "abcde", 2);
        checkValue0(sql("?", "?", "?"), SqlColumnType.VARCHAR, "bc", "abcde", 2, 2);
    }

    private String sql(String inputOperand, String fromOperand) {
        if (useFunctionalSyntax) {
            return "SELECT SUBSTRING(" + inputOperand + ", " + fromOperand + ") FROM map";
        } else {
            return "SELECT SUBSTRING(" + inputOperand + " FROM " + fromOperand + ") FROM map";
        }
    }

    private String sql(String inputOperand, String fromOperand, String forOperand) {
        if (useFunctionalSyntax) {
            return "SELECT SUBSTRING(" + inputOperand + ", " + fromOperand + ", " + forOperand + ") FROM map";
        } else {
            return "SELECT SUBSTRING(" + inputOperand + " FROM " + fromOperand + " FOR " + forOperand + ") FROM map";
        }
    }
}
