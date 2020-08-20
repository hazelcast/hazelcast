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
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.expression.SqlExpressionIntegrationTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionValue;
import com.hazelcast.sql.support.expressions.ExpressionValue.ByteVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.CharacterVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.IntegerVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.LongVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.ObjectVal;
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

        checkColumn(new ByteVal().field1((byte) 100), 3);
        checkColumn(new ShortVal().field1((short) 100), 3);
        checkColumn(new IntegerVal().field1(100), 3);
        checkColumn(new LongVal().field1((long) 100), 3);
        checkColumn(new ExpressionValue.BigIntegerVal().field1(new BigInteger("100")), 3);
        checkColumn(new ExpressionValue.BigDecimalVal().field1(new BigDecimal("100.5")), 5);
        checkColumn(new ExpressionValue.FloatVal().field1(100.5f), Float.toString(100.5f).length());
        checkColumn(new ExpressionValue.DoubleVal().field1(100.5d), Double.toString(100.5d).length());

        put(new ObjectVal());
        checkFailure("field1", SqlErrorCode.PARSING, "Cannot apply '" + name + "' to arguments of type '" + name + "(<OBJECT>)'");
    }

    private void checkColumn(ExpressionValue value, Integer expectedResult) {
        put(value);

        check("field1", expectedResult);
    }

    @Test
    public void test_literal() {
        put(1);

        check("null", null);

        check("true", 4);

        check("100", 3);
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

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from BOOLEAN to VARCHAR", true);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TINYINT to VARCHAR", (byte) 100);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from SMALLINT to VARCHAR", (short) 100);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 100);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from BIGINT to VARCHAR", 100L);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to VARCHAR", BigInteger.ONE);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to VARCHAR", BigDecimal.ONE);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from REAL to VARCHAR", 100f);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DOUBLE to VARCHAR", 100d);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from OBJECT to VARCHAR", new ObjectVal());
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
