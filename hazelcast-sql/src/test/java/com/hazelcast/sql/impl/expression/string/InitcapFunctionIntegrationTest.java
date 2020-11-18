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

        checkColumnFailure(new ByteVal().field1((byte) 100), "TINYINT");
        checkColumnFailure(new ShortVal().field1((short) 100), "SMALLINT");
        checkColumnFailure(new IntegerVal().field1(100), "INTEGER");
        checkColumnFailure(new LongVal().field1((long) 100), "BIGINT");
        checkColumnFailure(new ExpressionValue.BigIntegerVal().field1(new BigInteger("100")), "DECIMAL");
        checkColumnFailure(new ExpressionValue.BigDecimalVal().field1(new BigDecimal("100.5")), "DECIMAL");
        checkColumnFailure(new ExpressionValue.FloatVal().field1(100.5f), "REAL");
        checkColumnFailure(new ExpressionValue.DoubleVal().field1(100.5d), "DOUBLE");
        checkColumnFailure(new ExpressionValue.LocalDateVal().field1(LOCAL_DATE_VAL), "DATE");
        checkColumnFailure(new ExpressionValue.LocalTimeVal().field1(LOCAL_TIME_VAL), "TIME");
        checkColumnFailure(new ExpressionValue.LocalDateTimeVal().field1(LOCAL_DATE_TIME_VAL), "TIMESTAMP");
        checkColumnFailure(new ExpressionValue.OffsetDateTimeVal().field1(OFFSET_DATE_TIME_VAL), "TIMESTAMP_WITH_TIME_ZONE");
        checkColumnFailure(new ObjectVal(), "OBJECT");
    }

    private void checkColumn(ExpressionValue value, String expectedResult) {
        put(value);

        check("field1", expectedResult);
    }

    private void checkColumnFailure(ExpressionValue value, String expectedType) {
        put(value);

        checkFailure("field1", SqlErrorCode.PARSING, "Cannot apply [" + expectedType + "] to the 'INITCAP' function");
    }

    @Test
    public void test_literal() {
        put(1);

        check("null", null);
        check("'100'", "100");
        check("'abCde'", "Abcde");

        checkFailure("100", SqlErrorCode.PARSING, "Cannot apply [TINYINT] to the 'INITCAP' function");
        checkFailure("true", SqlErrorCode.PARSING, "Cannot apply [BOOLEAN] to the 'INITCAP' function");
        checkFailure("100E0", SqlErrorCode.PARSING, "Cannot apply [DOUBLE] to the 'INITCAP' function");
    }

    @Test
    public void test_parameter() {
        put(1);

        check("?", null, new Object[] { null });

        check("?", "", "");
        check("?", "First", "fiRst");
        check("?", "First Second", "fiRst seCond");

        check("?", "A", 'a');

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

    private void check(Object operand, String expectedResult, Object... params) {
        String sql = "SELECT INITCAP(" + operand + ") FROM map";

        checkValueInternal(sql, SqlColumnType.VARCHAR, expectedResult, params);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT INITCAP(" + operand + ") FROM map";

        checkFailureInternal(sql, expectedErrorCode, expectedErrorMessage, params);
    }
}
