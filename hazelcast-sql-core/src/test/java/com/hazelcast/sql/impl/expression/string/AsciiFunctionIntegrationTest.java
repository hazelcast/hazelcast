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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AsciiFunctionIntegrationTest extends SqlExpressionIntegrationTestSupport {
    @Test
    public void test_column() {
        checkColumn(new StringVal(), null);
        checkColumn(new StringVal().field1("abcde"), codePoint('a'));

        checkColumn(new CharacterVal().field1('a'), codePoint('a'));

        checkColumn(new ByteVal().field1((byte) 100), codePoint('1'));
        checkColumn(new ShortVal().field1((short) 100), codePoint('1'));
        checkColumn(new IntegerVal().field1(100), codePoint('1'));
        checkColumn(new LongVal().field1((long) 100), codePoint('1'));
        checkColumn(new BigIntegerVal().field1(new BigInteger("100")), codePoint('1'));
        checkColumn(new BigDecimalVal().field1(new BigDecimal("100.5")), codePoint('1'));
        checkColumn(new FloatVal().field1(100.5f), codePoint('1'));
        checkColumn(new DoubleVal().field1(100.5d), codePoint('1'));
        checkColumn(new LocalDateVal().field1(LOCAL_DATE_VAL), codePoint('2'));
        checkColumn(new LocalTimeVal().field1(LOCAL_TIME_VAL), codePoint('0'));
        checkColumn(new LocalDateTimeVal().field1(LOCAL_DATE_TIME_VAL), codePoint('2'));
        checkColumn(new OffsetDateTimeVal().field1(OFFSET_DATE_TIME_VAL), codePoint('2'));

        put(new ObjectVal());
        checkFailure("field1", SqlErrorCode.PARSING, "Cannot apply 'ASCII' to arguments of type 'ASCII(<OBJECT>)'");
    }

    private void checkColumn(ExpressionValue value, Integer expectedResult) {
        put(value);

        check("field1", expectedResult);
    }

    @Test
    public void test_literal() {
        put(1);

        check("null", null);

        check("true", codePoint('t'));

        check("100", codePoint('1'));
        check("'100'", codePoint('1'));
        check("'abcde'", codePoint('a'));

        check("'100E0'", codePoint('1'));
    }

    @Test
    public void test_parameter() {
        put(1);

        check("?", null, new Object[] { null });

        check("?", 0, "");
        check("?", codePoint('a'), "abc");
        check("?", codePoint('A'), "ABC");

        check("?", codePoint('a'), 'a');

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
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DATE to VARCHAR", LOCAL_DATE_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIME to VARCHAR", LOCAL_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP to VARCHAR", LOCAL_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP_WITH_TIME_ZONE to VARCHAR", OFFSET_DATE_TIME_VAL);
    }

    private void check(Object operand, Integer expectedResult, Object... params) {
        String sql = "SELECT ASCII(" + operand + ") FROM map";

        checkValueInternal(sql, SqlColumnType.INTEGER, expectedResult, params);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT ASCII(" + operand + ") FROM map";

        checkFailureInternal(sql, expectedErrorCode, expectedErrorMessage, params);
    }

    private static Integer codePoint(Character value) {
        if (value == null) {
            return null;
        }

        return value.toString().codePoints().toArray()[0];
    }
}
