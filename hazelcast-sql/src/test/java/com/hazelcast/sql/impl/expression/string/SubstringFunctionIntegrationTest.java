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
import com.hazelcast.sql.impl.expression.SqlExpressionIntegrationTestSupport;
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
public class SubstringFunctionIntegrationTest extends SqlExpressionIntegrationTestSupport {

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
        checkValueInternal(sql("this", "1"), SqlColumnType.VARCHAR, "abcde");
        checkValueInternal(sql("this", "1", "1"), SqlColumnType.VARCHAR, "a");
        checkValueInternal(sql("this", "1", "2"), SqlColumnType.VARCHAR, "ab");
        checkValueInternal(sql("this", "1", "5"), SqlColumnType.VARCHAR, "abcde");
        checkValueInternal(sql("this", "1", "6"), SqlColumnType.VARCHAR, "abcde");
        checkFailureInternal(sql("this", "1", "-1"), SqlErrorCode.DATA_EXCEPTION, "SUBSTRING \"length\" operand cannot be negative");

        // Character column
        put('a');
        checkValueInternal(sql("this", "1"), SqlColumnType.VARCHAR, "a");
        checkValueInternal(sql("this", "2"), SqlColumnType.VARCHAR, "");
        checkValueInternal(sql("this", "1", "1"), SqlColumnType.VARCHAR, "a");

        // Null value
        put(new ExpressionValue.StringVal().field1(null));
        checkValueInternal(sql("field1", "1"), SqlColumnType.VARCHAR, null);

        // Other columns
        put(true);
        checkFailureInternal(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [BOOLEAN, INTEGER] to the 'SUBSTRING' function");

        put((byte) 1);
        checkFailureInternal(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [TINYINT, INTEGER] to the 'SUBSTRING' function");

        put((short) 2);
        checkFailureInternal(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [SMALLINT, INTEGER] to the 'SUBSTRING' function");

        put(3);
        checkFailureInternal(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [INTEGER, INTEGER] to the 'SUBSTRING' function");

        put(4L);
        checkFailureInternal(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [BIGINT, INTEGER] to the 'SUBSTRING' function");

        put(new BigInteger("5"));
        checkFailureInternal(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [DECIMAL, INTEGER] to the 'SUBSTRING' function");

        put(new BigDecimal("6"));
        checkFailureInternal(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [DECIMAL, INTEGER] to the 'SUBSTRING' function");

        put(7f);
        checkFailureInternal(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [REAL, INTEGER] to the 'SUBSTRING' function");

        put(8d);
        checkFailureInternal(sql("this", "1"), SqlErrorCode.PARSING, "Cannot apply [DOUBLE, INTEGER] to the 'SUBSTRING' function");

        // Parameter
        put(1);
        checkValueInternal(sql("?", "1"), SqlColumnType.VARCHAR, "abcde", "abcde");
        checkValueInternal(sql("?", "1"), SqlColumnType.VARCHAR, "a", 'a');
        checkValueInternal(sql("?", "1"), SqlColumnType.VARCHAR, null, new Object[] { null });

        checkFailureInternal(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", (byte) 1);
        checkFailureInternal(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", (short) 1);
        checkFailureInternal(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", 1);
        checkFailureInternal(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", 1L);
        checkFailureInternal(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", BigInteger.ONE);
        checkFailureInternal(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", BigDecimal.ONE);
        checkFailureInternal(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", 1f);
        checkFailureInternal(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", 1d);
        checkFailureInternal(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", new ExpressionValue.ObjectVal());
        checkFailureInternal(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", LOCAL_DATE_VAL);
        checkFailureInternal(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", LOCAL_TIME_VAL);
        checkFailureInternal(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", LOCAL_DATE_TIME_VAL);
        checkFailureInternal(sql("?", "1"), SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", OFFSET_DATE_TIME_VAL);

        // Literal
        checkValueInternal(sql("'abc'", "1"), SqlColumnType.VARCHAR, "abc");
        checkFailureInternal(sql("1", "1"), SqlErrorCode.PARSING, "Cannot apply [TINYINT, INTEGER] to the 'SUBSTRING' function");
    }

    @Test
    public void test_start() {
        // Different values
        put("abcde");
        checkValueInternal(sql("this", "null"), SqlColumnType.VARCHAR, null);
        checkValueInternal(sql("this", "1"), SqlColumnType.VARCHAR, "abcde");
        checkValueInternal(sql("this", "2"), SqlColumnType.VARCHAR, "bcde");
        checkValueInternal(sql("this", "5"), SqlColumnType.VARCHAR, "e");
        checkValueInternal(sql("this", "6"), SqlColumnType.VARCHAR, "");
        checkValueInternal(sql("this", "10"), SqlColumnType.VARCHAR, "");
        checkFailureInternal(sql("this", "0"), SqlErrorCode.DATA_EXCEPTION, "SUBSTRING \"start\" operand must be positive");
        checkFailureInternal(sql("this", "-1"), SqlErrorCode.DATA_EXCEPTION, "SUBSTRING \"start\" operand must be positive");

        // Columns
        put(new ExpressionValue.IntegerVal());
        checkValueInternal(sql("'abcde'", "field1"), SqlColumnType.VARCHAR, null);

        put(true);
        checkFailureInternal(sql("'abcde'", "this"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, BOOLEAN] to the 'SUBSTRING' function (consider adding an explicit CAST)");

        put((byte) 2);
        checkValueInternal(sql("'abcde'", "this"), SqlColumnType.VARCHAR, "bcde");

        put((short) 2);
        checkValueInternal(sql("'abcde'", "this"), SqlColumnType.VARCHAR, "bcde");

        put(2);
        checkValueInternal(sql("'abcde'", "this"), SqlColumnType.VARCHAR, "bcde");

        put(2L);
        checkFailureInternal(sql("'abcde'", "this"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, BIGINT] to the 'SUBSTRING' function");

        put("2");
        checkFailureInternal(sql("'abcde'", "this"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, VARCHAR] to the 'SUBSTRING' function");

        put('2');
        checkFailureInternal(sql("'abcde'", "this"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, VARCHAR] to the 'SUBSTRING' function");

        // Parameters
        put("abcde");
        checkValueInternal(sql("this", "?"), SqlColumnType.VARCHAR, null, new Object[] { null});
        checkValueInternal(sql("this", "?"), SqlColumnType.VARCHAR, "bcde", (byte) 2);
        checkValueInternal(sql("this", "?"), SqlColumnType.VARCHAR, "bcde", (short) 2);
        checkValueInternal(sql("this", "?"), SqlColumnType.VARCHAR, "bcde", 2);
        checkFailureInternal(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from VARCHAR to INTEGER", "2");
        checkFailureInternal(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from VARCHAR to INTEGER", '2');
        checkFailureInternal(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from BOOLEAN to INTEGER", true);
        checkFailureInternal(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from BIGINT to INTEGER", 2L);
        checkFailureInternal(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to INTEGER", BigInteger.ONE);
        checkFailureInternal(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to INTEGER", BigDecimal.ONE);
        checkFailureInternal(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from REAL to INTEGER", 2f);
        checkFailureInternal(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DOUBLE to INTEGER", 2d);
        checkFailureInternal(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DATE to INTEGER", LOCAL_DATE_VAL);
        checkFailureInternal(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIME to INTEGER", LOCAL_TIME_VAL);
        checkFailureInternal(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP to INTEGER", LOCAL_DATE_TIME_VAL);
        checkFailureInternal(sql("this", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP_WITH_TIME_ZONE to INTEGER", OFFSET_DATE_TIME_VAL);

        // Literals
        put("abcde");
        checkValueInternal(sql("this", "2"), SqlColumnType.VARCHAR, "bcde");
        checkFailureInternal(sql("this", "'2'"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, VARCHAR] to the 'SUBSTRING' function (consider adding an explicit CAST)");
        checkValueInternal(sql("this", "null"), SqlColumnType.VARCHAR, null);
        checkFailureInternal(sql("this", "true"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, BOOLEAN] to the 'SUBSTRING' function (consider adding an explicit CAST)");
    }

    @Test
    public void test_length() {
        // Different values
        put(1);
        checkValueInternal(sql("'abcde'", "2", "0"), SqlColumnType.VARCHAR, "");
        checkValueInternal(sql("'abcde'", "2", "2"), SqlColumnType.VARCHAR, "bc");
        checkValueInternal(sql("'abcde'", "2", "10"), SqlColumnType.VARCHAR, "bcde");
        checkValueInternal(sql("'abcde'", "2", "null"), SqlColumnType.VARCHAR, null);
        checkFailureInternal(sql("'abcde'", "2", "-1"), SqlErrorCode.DATA_EXCEPTION, "SUBSTRING \"length\" operand cannot be negative");

        // Columns
        put(new ExpressionValue.IntegerVal());
        checkValueInternal(sql("'abcde'", "2", "field1"), SqlColumnType.VARCHAR, null);

        put(true);
        checkFailureInternal(sql("'abcde'", "2", "this"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, INTEGER, BOOLEAN] to the 'SUBSTRING' function (consider adding an explicit CAST)");

        put((byte) 2);
        checkValueInternal(sql("'abcde'", "2", "this"), SqlColumnType.VARCHAR, "bc");

        put((short) 2);
        checkValueInternal(sql("'abcde'", "2", "this"), SqlColumnType.VARCHAR, "bc");

        put(2);
        checkValueInternal(sql("'abcde'", "2", "this"), SqlColumnType.VARCHAR, "bc");

        put(2L);
        checkFailureInternal(sql("'abcde'", "2", "this"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, INTEGER, BIGINT] to the 'SUBSTRING' function");

        // Parameters
        put(1);
        checkValueInternal(sql("'abcde'", "2", "?"), SqlColumnType.VARCHAR, null, new Object[] { null });
        checkValueInternal(sql("'abcde'", "2", "?"), SqlColumnType.VARCHAR, "bc", (byte) 2);
        checkValueInternal(sql("'abcde'", "2", "?"), SqlColumnType.VARCHAR, "bc", (short) 2);
        checkValueInternal(sql("'abcde'", "2", "?"), SqlColumnType.VARCHAR, "bc", 2);
        checkFailureInternal(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from VARCHAR to INTEGER", "2");
        checkFailureInternal(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from VARCHAR to INTEGER", '2');
        checkFailureInternal(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from BOOLEAN to INTEGER", true);
        checkFailureInternal(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from BIGINT to INTEGER", 2L);
        checkFailureInternal(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to INTEGER", new BigInteger("2"));
        checkFailureInternal(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to INTEGER", new BigDecimal("2"));
        checkFailureInternal(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from REAL to INTEGER", 2f);
        checkFailureInternal(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DOUBLE to INTEGER", 2d);
        checkFailureInternal(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DATE to INTEGER", LOCAL_DATE_VAL);
        checkFailureInternal(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIME to INTEGER", LOCAL_TIME_VAL);
        checkFailureInternal(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP to INTEGER", LOCAL_DATE_TIME_VAL);
        checkFailureInternal(sql("'abcde'", "2", "?"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP_WITH_TIME_ZONE to INTEGER", OFFSET_DATE_TIME_VAL);

        // Literals
        put(1);

        checkValueInternal(sql("'abcde'", "2", "2"), SqlColumnType.VARCHAR, "bc");
        checkFailureInternal(sql("'abcde'", "2", "'2'"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, INTEGER, VARCHAR] to the 'SUBSTRING' function");
        checkValueInternal(sql("'abcde'", "2", "null"), SqlColumnType.VARCHAR, null);
        checkFailureInternal(sql("'abcde'", "2", "true"), SqlErrorCode.PARSING, "Cannot apply [VARCHAR, INTEGER, BOOLEAN] to the 'SUBSTRING' function (consider adding an explicit CAST)");
    }

    @Test
    public void test_parameters_only() {
        put(1);
        checkValueInternal(sql("?", "?"), SqlColumnType.VARCHAR, "bcde", "abcde", 2);
        checkValueInternal(sql("?", "?", "?"), SqlColumnType.VARCHAR, "bc", "abcde", 2, 2);
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
