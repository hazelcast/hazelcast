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
import com.hazelcast.sql.support.expressions.ExpressionValue.ShortVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.StringVal;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.hazelcast.sql.support.expressions.ExpressionValue.BigDecimalVal;
import static com.hazelcast.sql.support.expressions.ExpressionValue.BigIntegerVal;
import static com.hazelcast.sql.support.expressions.ExpressionValue.DoubleVal;
import static com.hazelcast.sql.support.expressions.ExpressionValue.FloatVal;
import static com.hazelcast.sql.support.expressions.ExpressionValue.ObjectVal;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InitcapFunctionIntegrationTest extends SqlExpressionIntegrationTestSupport {
    @Test
    public void test_column() {
        checkColumn(new StringVal(), null);
        checkColumn(new StringVal().field1("-first"), "-First");
        checkColumn(new StringVal().field1("fiRst"), "First");
        checkColumn(new StringVal().field1("fiRst seCond"), "First Second");

        checkColumn(new CharacterVal().field1('a'), "A");

        checkColumn(new ByteVal().field1((byte) 100), "100");
        checkColumn(new ShortVal().field1((short) 100), "100");
        checkColumn(new IntegerVal().field1(100), "100");
        checkColumn(new LongVal().field1((long) 100), "100");
        checkColumn(new BigIntegerVal().field1(new BigInteger("100")), "100");
        checkColumn(new BigDecimalVal().field1(new BigDecimal("100.5")), "100.5");
        checkColumn(new FloatVal().field1(100.5f), Float.toString(100.5f));
        checkColumn(new DoubleVal().field1(100.5d), Double.toString(100.5d));
    }

    private void checkColumn(ExpressionValue value, String expectedResult) {
        put(value);

        check("field1", expectedResult);
    }

    @Test
    public void test_literal() {
        put(1);

        check("null", null);

        check("true", "True");

        check("100", "100");
        check("'100'", "100");
        check("'abCde'", "Abcde");

        check("'100E0'", "100e0");
    }

    @Test
    public void test_parameter() {
        put(1);

        check("?", null, new Object[] { null });

        check("?", "", "");
        check("?", "First", "fiRst");
        check("?", "First Second", "fiRst seCond");

        check("?", "A", 'a');

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

    private void check(Object operand, String expectedResult, Object... params) {
        String sql = "SELECT INITCAP(" + operand + ") FROM map";

        checkValueInternal(sql, SqlColumnType.VARCHAR, expectedResult, params);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT INITCAP(" + operand + ") FROM map";

        checkFailureInternal(sql, expectedErrorCode, expectedErrorMessage, params);
    }
}
