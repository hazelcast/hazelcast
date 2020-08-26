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

package com.hazelcast.sql.impl.expression.predicate;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.SqlExpressionIntegrationTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionBiValue;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.BooleanBigDecimalVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.BooleanBigIntegerVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.BooleanBooleanVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.BooleanByteVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.BooleanDoubleVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.BooleanLocalDateTimeVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.BooleanLocalDateVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.BooleanLocalTimeVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.BooleanLongVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.BooleanObjectVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.BooleanOffsetDateTimeVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.BooleanShortVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.CharacterBooleanVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.StringBigDecimalVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.StringBigIntegerVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.StringBooleanVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.StringByteVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.StringDoubleVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.StringFloatVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.StringIntegerVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.StringLocalDateTimeVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.StringLocalDateVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.StringLocalTimeVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.StringLongVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.StringObjectVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.StringOffsetDateTimeVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.StringShortVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.StringStringVal;
import com.hazelcast.sql.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.hazelcast.sql.support.expressions.ExpressionBiValue.BooleanFloatVal;
import static com.hazelcast.sql.support.expressions.ExpressionBiValue.BooleanIntegerVal;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AndPredicateIntegrationTest extends SqlExpressionIntegrationTestSupport {

    private static final Boolean RES_TRUE = true;
    private static final Boolean RES_FALSE = false;
    private static final Boolean RES_NULL = null;

    @Test
    public void test_three_operands() {
        put(0);

        String sql = sql("?", "?", "?");

        checkValue(sql, RES_FALSE, null, true, false);
    }

    @Test
    public void test_column() {
        // BOOLEAN/BOOLEAN
        checkColumnColumn(new BooleanBooleanVal().fields(true, true), RES_TRUE);
        checkColumnColumn(new BooleanBooleanVal().fields(true, false), RES_FALSE);
        checkColumnColumn(new BooleanBooleanVal().fields(true, null), RES_NULL);
        checkColumnColumn(new BooleanBooleanVal().fields(false, false), RES_FALSE);
        checkColumnColumn(new BooleanBooleanVal().fields(false, null), RES_FALSE);
        checkColumnColumn(new BooleanBooleanVal().fields(null, null), RES_NULL);

        // BOOLEAN/VARCHAR
        checkColumnColumn(new StringBooleanVal().fields("true", true), RES_TRUE);
        checkColumnColumn(new StringBooleanVal().fields("false", true), RES_FALSE);
        checkColumnColumn(new StringBooleanVal().fields("false", false), RES_FALSE);
        checkColumnColumnFailure(new StringBooleanVal().fields("bad", null), SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to BOOLEAN");
        checkColumnColumnFailure(new CharacterBooleanVal().fields('b', null), SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to BOOLEAN");

        // VARCHAR/VARCHAR
        checkColumnColumn(new StringStringVal().fields("true", "true"), RES_TRUE);
        checkColumnColumn(new StringStringVal().fields("false", "true"), RES_FALSE);
        checkColumnColumn(new StringStringVal().fields("false", "false"), RES_FALSE);
        checkColumnColumnFailure(new StringStringVal().fields("bad", null), SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to BOOLEAN");

        // BOOLEAN/unsupported
        checkColumnColumnFailure(new BooleanByteVal().fields(true, null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <TINYINT>'");
        checkColumnColumnFailure(new BooleanShortVal().fields(true, null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <SMALLINT>'");
        checkColumnColumnFailure(new BooleanIntegerVal().fields(true, null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <INTEGER>'");
        checkColumnColumnFailure(new BooleanLongVal().fields(true, null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <BIGINT>'");
        checkColumnColumnFailure(new BooleanBigIntegerVal().fields(true, null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <DECIMAL(38, 38)>'");
        checkColumnColumnFailure(new BooleanBigDecimalVal().fields(true, null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <DECIMAL(38, 38)>'");
        checkColumnColumnFailure(new BooleanFloatVal().fields(true, null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <REAL>'");
        checkColumnColumnFailure(new BooleanDoubleVal().fields(true, null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <DOUBLE>'");
        checkColumnColumnFailure(new BooleanLocalDateVal().fields(true, null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <DATE>'");
        checkColumnColumnFailure(new BooleanLocalTimeVal().fields(true, null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <TIME>'");
        checkColumnColumnFailure(new BooleanLocalDateTimeVal().fields(true, null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <TIMESTAMP>'");
        checkColumnColumnFailure(new BooleanOffsetDateTimeVal().fields(true, null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <TIMESTAMP_WITH_TIME_ZONE>'");
        checkColumnColumnFailure(new BooleanObjectVal().fields(true, null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <OBJECT>'");

        // VARCHAR/unsupported
        checkColumnColumnFailure(new StringByteVal().fields("true", null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<VARCHAR> AND <TINYINT>'");
        checkColumnColumnFailure(new StringShortVal().fields("true", null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<VARCHAR> AND <SMALLINT>'");
        checkColumnColumnFailure(new StringIntegerVal().fields("true", null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<VARCHAR> AND <INTEGER>'");
        checkColumnColumnFailure(new StringLongVal().fields("true", null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<VARCHAR> AND <BIGINT>'");
        checkColumnColumnFailure(new StringBigIntegerVal().fields("true", null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<VARCHAR> AND <DECIMAL(38, 38)>'");
        checkColumnColumnFailure(new StringBigDecimalVal().fields("true", null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<VARCHAR> AND <DECIMAL(38, 38)>'");
        checkColumnColumnFailure(new StringFloatVal().fields("true", null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<VARCHAR> AND <REAL>'");
        checkColumnColumnFailure(new StringDoubleVal().fields("true", null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<VARCHAR> AND <DOUBLE>'");
        checkColumnColumnFailure(new StringLocalDateVal().fields("true", null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<VARCHAR> AND <DATE>'");
        checkColumnColumnFailure(new StringLocalTimeVal().fields("true", null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<VARCHAR> AND <TIME>'");
        checkColumnColumnFailure(new StringLocalDateTimeVal().fields("true", null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<VARCHAR> AND <TIMESTAMP>'");
        checkColumnColumnFailure(new StringOffsetDateTimeVal().fields("true", null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<VARCHAR> AND <TIMESTAMP_WITH_TIME_ZONE>'");
        checkColumnColumnFailure(new StringObjectVal().fields("true", null), SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<VARCHAR> AND <OBJECT>'");

        // COLUMN/PARAMETER
        put(true);
        checkValue("this", "?", RES_TRUE, true);
        checkValue("this", "?", RES_FALSE, false);
        checkValue("this", "?", RES_NULL, new Object[] { null });
        checkFailure("this", "?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to BOOLEAN", "bad");

        // COLUMN/LITERAL
        checkValue("this", "true", RES_TRUE);
        checkValue("this", "false", RES_FALSE);
        checkValue("this", "null", RES_NULL);
        checkValue("this", "'true'", RES_TRUE);
        checkValue("this", "'false'", RES_FALSE);
        checkFailure("this", "1", SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <TINYINT>'");
        checkFailure("this", "1E0", SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <DOUBLE>'");
        checkFailure("this", "'bad'", SqlErrorCode.PARSING, "Literal ''bad'' can not be parsed to type 'BOOLEAN'");
    }

    @Test
    public void test_parameter() {
        put(1);

        checkValue("?", "?", RES_TRUE, true, true);
        checkValue("?", "?", RES_FALSE, true, false);
        checkValue("?", "?", RES_NULL, true, null);
        checkValue("?", "?", RES_FALSE, false, false);
        checkValue("?", "?", RES_FALSE, false, null);
        checkValue("?", "?", RES_NULL, null, null);

        checkValue("?", "?", RES_TRUE, true, "true");
        checkValue("?", "?", RES_FALSE, true, "false");
        checkValue("?", "?", RES_TRUE, "true", "true");
        checkValue("?", "?", RES_FALSE, "true", "false");

        checkFailure("?", "?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 1 from VARCHAR to BOOLEAN", true, "bad");

        checkFailure("?", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 1 from TINYINT to BOOLEAN", true, (byte) 1);
        checkFailure("?", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 1 from SMALLINT to BOOLEAN", true, (short) 1);
        checkFailure("?", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 1 from INTEGER to BOOLEAN", true, 1);
        checkFailure("?", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 1 from BIGINT to BOOLEAN", true, 1L);
        checkFailure("?", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 1 from DECIMAL to BOOLEAN", true, BigInteger.ONE);
        checkFailure("?", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 1 from DECIMAL to BOOLEAN", true, BigDecimal.ONE);
        checkFailure("?", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 1 from REAL to BOOLEAN", true, 1f);
        checkFailure("?", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 1 from DOUBLE to BOOLEAN", true, 1d);
        checkFailure("?", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 1 from DATE to BOOLEAN", true, LOCAL_DATE_VAL);
        checkFailure("?", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 1 from TIME to BOOLEAN", true, LOCAL_TIME_VAL);
        checkFailure("?", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 1 from TIMESTAMP to BOOLEAN", true, LOCAL_DATE_TIME_VAL);
        checkFailure("?", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 1 from TIMESTAMP_WITH_TIME_ZONE to BOOLEAN", true, OFFSET_DATE_TIME_VAL);
        checkFailure("?", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 1 from OBJECT to BOOLEAN", true, new ExpressionValue.ObjectVal());

        checkValue("?", "true", RES_TRUE, true);
        checkValue("?", "true", RES_FALSE, false);
        checkValue("?", "false", RES_FALSE, true);
        checkValue("?", "false", RES_FALSE, false);
        checkValue("?", "null", RES_NULL, true);
        checkValue("?", "null", RES_FALSE, false);

        checkValue("?", "'true'", RES_TRUE, true);
        checkValue("?", "'true'", RES_FALSE, false);
        checkValue("?", "'false'", RES_FALSE, true);
        checkValue("?", "'false'", RES_FALSE, false);

        checkFailure("?", "1", SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <TINYINT>'", true);
        checkFailure("?", "1E0", SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <DOUBLE>'", true);
        checkFailure("?", "'bad'", SqlErrorCode.PARSING, "Literal ''bad'' can not be parsed to type 'BOOLEAN'", true);
    }

    @Test
    public void test_literal() {
        put(1);

        checkValue("true", "true", RES_TRUE);
        checkValue("true", "false", RES_FALSE);
        checkValue("true", "null", RES_NULL);
        checkValue("false", "false", RES_FALSE);
        checkValue("false", "null", RES_FALSE);
        checkValue("null", "null", RES_NULL);

        checkValue("true", "'false'", RES_FALSE);

        checkFailure("true", "1", SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <TINYINT>'");
        checkFailure("true", "1E0", SqlErrorCode.PARSING, "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <DOUBLE>'");
        checkFailure("true", "'bad'", SqlErrorCode.PARSING, "Literal ''bad'' can not be parsed to type 'BOOLEAN'");
    }

    private void checkColumnColumn(ExpressionBiValue value, Boolean expectedValue) {
        put(value);

        checkValue("field1", "field2", expectedValue);
    }

    private void checkColumnColumnFailure(ExpressionBiValue value, int expectedErrorCode, String expectedErrorMessage) {
        put(value);

        checkFailure("field1", "field2", expectedErrorCode, expectedErrorMessage);
    }

    private void checkValue(String operand1, String operand2, Boolean expectedResult, Object... params) {
        checkValue(sql(operand1, operand2), expectedResult, params);
        checkValue(sql(operand2, operand1), expectedResult, params);
    }

    private void checkValue(String sql, Boolean expectedValue, Object... params) {
        checkValueInternal(sql, SqlColumnType.BOOLEAN, expectedValue, params);
    }

    private void checkFailure(
        String operand1,
        String operand2,
        int expectedErrorCode,
        String expectedErrorMessage,
        Object... params
    ) {
        checkFailureInternal(sql(operand1, operand2), expectedErrorCode, expectedErrorMessage, params);
    }

    private String sql(Object... operands) {
        assert operands != null;
        assert operands.length > 1;

        StringBuilder condition = new StringBuilder();
        condition.append(operands[0]);

        for (int i = 1; i < operands.length; i++) {
            condition.append(" AND ");
            condition.append(operands[i]);
        }

        return "SELECT " + condition.toString() + " FROM map";
    }
}
