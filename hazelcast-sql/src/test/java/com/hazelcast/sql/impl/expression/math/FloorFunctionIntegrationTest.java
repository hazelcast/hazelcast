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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.SqlExpressionIntegrationTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionValue;
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
public class FloorFunctionIntegrationTest extends SqlExpressionIntegrationTestSupport {
    @Test
    public void testColumn() {
        checkColumn((byte) 1, SqlColumnType.TINYINT, (byte) 1);
        checkColumn((short) 1, SqlColumnType.SMALLINT, (short) 1);
        checkColumn(1, SqlColumnType.INTEGER, 1);
        checkColumn(1L, SqlColumnType.BIGINT, 1L);
        checkColumn(1.1f, SqlColumnType.REAL, 1f);
        checkColumn(1.1d, SqlColumnType.DOUBLE, 1d);
        checkColumn(BigInteger.ONE, SqlColumnType.DECIMAL, BigDecimal.ONE);
        checkColumn(new BigDecimal("1.1"), SqlColumnType.DECIMAL, BigDecimal.ONE);

        checkColumn("1.1", SqlColumnType.DECIMAL, BigDecimal.ONE);
        checkColumn('1', SqlColumnType.DECIMAL, BigDecimal.ONE);

        checkColumn(Float.POSITIVE_INFINITY, SqlColumnType.REAL, Float.POSITIVE_INFINITY);
        checkColumn(Float.NEGATIVE_INFINITY, SqlColumnType.REAL, Float.NEGATIVE_INFINITY);
        checkColumn(Float.NaN, SqlColumnType.REAL, Float.NaN);

        checkColumn(Double.POSITIVE_INFINITY, SqlColumnType.DOUBLE, Double.POSITIVE_INFINITY);
        checkColumn(Double.NEGATIVE_INFINITY, SqlColumnType.DOUBLE, Double.NEGATIVE_INFINITY);
        checkColumn(Double.NaN, SqlColumnType.DOUBLE, Double.NaN);

        put(new ExpressionValue.IntegerVal());
        checkValue("field1", SqlColumnType.INTEGER, null);

        checkColumnFailure("bad", SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL");
        checkColumnFailure('b', SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL");
        checkColumnFailure(new ExpressionValue.ObjectVal(), SqlErrorCode.PARSING, "Cannot apply 'FLOOR' to arguments of type 'FLOOR(<OBJECT>)'");
    }

    private void checkColumn(Object value, SqlColumnType expectedType, Object expectedResult) {
        put(value);

        checkValue("this", expectedType, expectedResult);
    }

    private void checkColumnFailure(Object value, int expectedErrorCode, String expectedErrorMessage) {
        put(value);

        checkFailure("this", expectedErrorCode, expectedErrorMessage);
    }

    @Test
    public void testParameter() {
        put(0);

        checkParameter((byte) 1, BigDecimal.ONE);
        checkParameter((short) 1, BigDecimal.ONE);
        checkParameter(1, BigDecimal.ONE);
        checkParameter(1L, BigDecimal.ONE);
        checkParameter(BigInteger.ONE, BigDecimal.ONE);
        checkParameter(new BigDecimal("1.1"), BigDecimal.ONE);

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from REAL to DECIMAL", 0.0f);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DOUBLE to DECIMAL", 0.0d);

        checkParameter("1.1", BigDecimal.ONE);
        checkParameter('1', BigDecimal.ONE);

        checkValue("?", SqlColumnType.DECIMAL, null, new Object[] { null });

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to DECIMAL", "bad");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to DECIMAL", 'b');

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from OBJECT to DECIMAL", new ExpressionValue.ObjectVal());
    }

    private void checkParameter(Object param, Object expectedResult) {
        checkValue("?", SqlColumnType.DECIMAL, expectedResult, param);
    }

    @Test
    public void testLiteral() {
        put(0);

        checkLiteral(1, SqlColumnType.TINYINT, (byte) 1);

        checkLiteral("null", SqlColumnType.DECIMAL, null);
        checkLiteral("1.1", SqlColumnType.DECIMAL, new BigDecimal("1"));
        checkLiteral("'1.1'", SqlColumnType.DECIMAL, new BigDecimal("1"));
        checkLiteral("1.1E0", SqlColumnType.DOUBLE, 1d);

        checkFailure("'bad'", SqlErrorCode.PARSING, "Literal ''bad'' can not be parsed to type 'DECIMAL'");
    }

    private void checkLiteral(Object literal, SqlColumnType expectedType, Object expectedResult) {
        String literalString = literal.toString();

        checkValue(literalString, expectedType, expectedResult);
    }

    private void checkValue(Object operand, SqlColumnType expectedType, Object expectedValue, Object... params) {
        String sql = "SELECT FLOOR(" + operand + ") FROM map";

        checkValueInternal(sql, expectedType, expectedValue, params);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT FLOOR(" + operand + ") FROM map";

        checkFailureInternal(sql, expectedErrorCode, expectedErrorMessage, params);
    }
}
