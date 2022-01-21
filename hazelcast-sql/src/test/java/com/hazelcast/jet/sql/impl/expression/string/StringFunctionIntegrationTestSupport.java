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
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

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

public abstract class StringFunctionIntegrationTestSupport extends ExpressionTestSupport {

    protected abstract String functionName();

    protected abstract SqlColumnType resultType();

    @Test
    public void testColumn() {
        checkSupportedColumns();

        checkColumnFailure(new ExpressionValue.BooleanVal().field1(true), BOOLEAN);

        checkColumnFailure(new ExpressionValue.ByteVal().field1((byte) 100), TINYINT);
        checkColumnFailure(new ExpressionValue.ShortVal().field1((short) 100), SMALLINT);
        checkColumnFailure(new ExpressionValue.IntegerVal().field1(100), INTEGER);
        checkColumnFailure(new ExpressionValue.LongVal().field1((long) 100), BIGINT);
        checkColumnFailure(new ExpressionValue.BigIntegerVal().field1(new BigInteger("100")), DECIMAL);
        checkColumnFailure(new ExpressionValue.BigDecimalVal().field1(new BigDecimal("100.5")), DECIMAL);
        checkColumnFailure(new ExpressionValue.FloatVal().field1(100.5f), REAL);
        checkColumnFailure(new ExpressionValue.DoubleVal().field1(100.5d), DOUBLE);

        checkColumnFailure(new ExpressionValue.LocalDateVal().field1(LOCAL_DATE_VAL), DATE);
        checkColumnFailure(new ExpressionValue.LocalTimeVal().field1(LOCAL_TIME_VAL), TIME);
        checkColumnFailure(new ExpressionValue.LocalDateTimeVal().field1(LOCAL_DATE_TIME_VAL), TIMESTAMP);
        checkColumnFailure(new ExpressionValue.OffsetDateTimeVal().field1(OFFSET_DATE_TIME_VAL), TIMESTAMP_WITH_TIME_ZONE);

        checkColumnFailure(OBJECT_VAL, OBJECT);
    }

    protected abstract void checkSupportedColumns();

    @Test
    public void testLiteral() {
        put(1);

        checkSupportedLiterals();

        checkLiteralFailure("true", BOOLEAN);

        checkLiteralFailure("1", TINYINT);
        checkLiteralFailure("1.1", DECIMAL);
        checkLiteralFailure("1.1E1", DOUBLE);
    }

    protected abstract void checkSupportedLiterals();

    @Test
    public void testParameter() {
        put(1);

        checkSupportedParameters();

        checkParameterFailure(true, BOOLEAN);

        checkParameterFailure((byte) 100, TINYINT);
        checkParameterFailure((short) 100, SMALLINT);
        checkParameterFailure(100, INTEGER);
        checkParameterFailure(100L, BIGINT);
        checkParameterFailure(BigInteger.ONE, DECIMAL);
        checkParameterFailure(BigDecimal.ONE, DECIMAL);
        checkParameterFailure(100f, REAL);
        checkParameterFailure(100d, DOUBLE);

        checkParameterFailure(LOCAL_DATE_VAL, DATE);
        checkParameterFailure(LOCAL_TIME_VAL, TIME);
        checkParameterFailure(LOCAL_DATE_TIME_VAL, TIMESTAMP);
        checkParameterFailure(OFFSET_DATE_TIME_VAL, TIMESTAMP_WITH_TIME_ZONE);

        checkParameterFailure(OBJECT_VAL, OBJECT);
    }

    protected abstract void checkSupportedParameters();

    protected void checkColumn(ExpressionValue value, Object expectedResult) {
        put(value);

        String sql = "SELECT " + functionName() + "(field1) FROM map";

        checkValue0(sql, resultType(), expectedResult);
    }

    private void checkColumnFailure(ExpressionValue value, SqlColumnType typeInErrorMessage) {
        put(value);

        String sql = "SELECT " + functionName() + "(field1) FROM map";

        checkFailure0(sql, SqlErrorCode.PARSING, signatureErrorFunction(functionName(), typeInErrorMessage));
    }

    protected void checkLiteral(Object operand, Object expectedResult) {
        String sql = "SELECT " + functionName() + "(" + operand + ") FROM map";

        checkValue0(sql, resultType(), expectedResult);
    }

    private void checkLiteralFailure(Object operand, SqlColumnType typeInErrorMessage) {
        String sql = "SELECT " + functionName() + "(" + operand + ") FROM map";

        checkFailure0(sql, SqlErrorCode.PARSING, signatureErrorFunction(functionName(), typeInErrorMessage));
    }

    protected void checkParameter(Object param, Object expectedResult) {
        String sql = "SELECT " + functionName() + "(?) FROM map";

        checkValue0(sql, resultType(), expectedResult, param);
    }

    protected void checkParameterFailure(Object param, SqlColumnType typeInErrorMessage) {
        String sql = "SELECT " + functionName() + "(?) FROM map";

        checkFailure0(sql, SqlErrorCode.DATA_EXCEPTION, parameterError(0, VARCHAR, typeInErrorMessage), param);
    }
}
