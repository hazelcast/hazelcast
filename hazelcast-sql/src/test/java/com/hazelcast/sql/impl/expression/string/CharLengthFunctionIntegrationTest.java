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
import com.hazelcast.sql.support.expressions.ExpressionValue.BigDecimalVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.BigIntegerVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.ByteVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.CharacterVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.DoubleVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.FloatVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.IntegerVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.LocalDateTimeVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.LocalDateVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.LocalTimeVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.LongVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.ObjectVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.OffsetDateTimeVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.ShortVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.StringVal;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CharLengthFunctionIntegrationTest extends SqlExpressionIntegrationTestSupport {

    @Parameterized.Parameter
    public String name;

    @Parameterized.Parameters(name = "name: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
            { "CHAR_LENGTH" },
            { "CHARACTER_LENGTH" },
            { "LENGTH" }
        });
    }

    @Test
    public void test_column() {
        checkColumn(new StringVal(), null);
        checkColumn(new StringVal().field1("abcde"), 5);

        checkColumn(new CharacterVal().field1('a'), 1);

        checkColumnFailure(new ByteVal().field1((byte) 100), SqlErrorCode.PARSING, "Cannot apply [TINYINT] to the '" + name + "' function (consider adding an explicit CAST)");
        checkColumnFailure(new ShortVal().field1((short) 100), SqlErrorCode.PARSING, "Cannot apply [SMALLINT] to the '" + name + "' function (consider adding an explicit CAST)");
        checkColumnFailure(new IntegerVal().field1(100), SqlErrorCode.PARSING, "Cannot apply [INTEGER] to the '" + name + "' function (consider adding an explicit CAST)");
        checkColumnFailure(new LongVal().field1((long) 100), SqlErrorCode.PARSING, "Cannot apply [BIGINT] to the '" + name + "' function (consider adding an explicit CAST)");
        checkColumnFailure(new BigIntegerVal().field1(new BigInteger("100")), SqlErrorCode.PARSING, "Cannot apply [DECIMAL] to the '" + name + "' function (consider adding an explicit CAST)");
        checkColumnFailure(new BigDecimalVal().field1(new BigDecimal("100.5")), SqlErrorCode.PARSING, "Cannot apply [DECIMAL] to the '" + name + "' function (consider adding an explicit CAST)");
        checkColumnFailure(new FloatVal().field1(100.5f), SqlErrorCode.PARSING, "Cannot apply [REAL] to the '" + name + "' function (consider adding an explicit CAST)");
        checkColumnFailure(new DoubleVal().field1(100.5d), SqlErrorCode.PARSING, "Cannot apply [DOUBLE] to the '" + name + "' function (consider adding an explicit CAST)");
        checkColumnFailure(new LocalDateVal().field1(LOCAL_DATE_VAL), SqlErrorCode.PARSING, "Cannot apply [DATE] to the '" + name + "' function (consider adding an explicit CAST)");
        checkColumnFailure(new LocalTimeVal().field1(LOCAL_TIME_VAL), SqlErrorCode.PARSING, "Cannot apply [TIME] to the '" + name + "' function (consider adding an explicit CAST)");
        checkColumnFailure(new LocalDateTimeVal().field1(LOCAL_DATE_TIME_VAL), SqlErrorCode.PARSING, "Cannot apply [TIMESTAMP] to the '" + name + "' function (consider adding an explicit CAST)");
        checkColumnFailure(new OffsetDateTimeVal().field1(OFFSET_DATE_TIME_VAL), SqlErrorCode.PARSING, "Cannot apply [TIMESTAMP_WITH_TIME_ZONE] to the '" + name + "' function (consider adding an explicit CAST)");
        checkColumnFailure(new ObjectVal(), SqlErrorCode.PARSING, "Cannot apply [OBJECT] to the '" + name + "' function (consider adding an explicit CAST)");
    }

    private void checkColumn(ExpressionValue value, Integer expectedResult) {
        put(value);

        check("field1", expectedResult);
    }

    private void checkColumnFailure(ExpressionValue value, int expectedErrorCode, String expectedErrorMessage) {
        put(value);

        checkFailure("field1", expectedErrorCode, expectedErrorMessage);
    }

    @Test
    public void test_literal() {
        put(1);

        check("null", null);

        checkFailure("true", SqlErrorCode.PARSING, "Cannot apply [BOOLEAN] to the '" + name + "' function");
        check("'true'", 4);

        checkFailure("100", SqlErrorCode.PARSING, "Cannot apply [TINYINT] to the '" + name + "' function");
        check("'100'", 3);
        check("'abcde'", 5);

        check("'100E0'", 5);
    }

    @Test
    public void test_parameter() {
        put(1);

        check("?", null, new Object[] { null });

        check("?", 0, "");
        check("?", 3, "abc");

        check("?", 1, 'a');

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", true);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", (byte) 100);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", (short) 100);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", 100);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", 100L);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", BigInteger.ONE);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", BigDecimal.ONE);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", 100f);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", 100d);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", new ObjectVal());
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", LOCAL_DATE_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", LOCAL_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", LOCAL_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of VARCHAR type", OFFSET_DATE_TIME_VAL);
    }

    private void check(Object operand, Integer expectedResult, Object... params) {
        String sql = "SELECT " + name + "(" + operand + ") FROM map";

        checkValueInternal(sql, SqlColumnType.INTEGER, expectedResult, params);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT " + name + "(" + operand + ") FROM map";

        checkFailureInternal(sql, expectedErrorCode, expectedErrorMessage, params);
    }

}
