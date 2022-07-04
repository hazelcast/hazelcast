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

package com.hazelcast.jet.sql.impl.expression.math;

import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.impl.expression.math.DoubleBiFunction;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
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

import static com.hazelcast.sql.SqlColumnType.BOOLEAN;
import static com.hazelcast.sql.SqlColumnType.DATE;
import static com.hazelcast.sql.SqlColumnType.DOUBLE;
import static com.hazelcast.sql.SqlColumnType.OBJECT;
import static com.hazelcast.sql.SqlColumnType.TIME;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;
import static com.hazelcast.sql.impl.expression.math.DoubleBiFunction.ATAN2;
import static com.hazelcast.sql.impl.expression.math.DoubleBiFunction.POWER;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DoubleBiFunctionIntegrationTest extends ExpressionTestSupport {
    @Parameterized.Parameter
    public FunctionInfo function;

    @Parameterized.Parameters(name = "function: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {new FunctionInfo("ATAN2", ATAN2)},
                {new FunctionInfo("POWER", POWER)}
        });
    }

    @Test
    public void testColumn() {

        // NULL
        putAndCheckValue(new ExpressionValue.DoubleVal(), sql("field1,  field1"), DOUBLE, null);

        // Numeric
        putAndCheckValue((byte) 1, sql("this, this"), DOUBLE, function.process(1d, 1d));
        putAndCheckValue((short) 1, sql("this, this"), DOUBLE, function.process(1d, 1d));
        putAndCheckValue(1, sql("this, this"), DOUBLE, function.process(1d, 1d));
        putAndCheckValue(1L, sql("this, this"), DOUBLE, function.process(1d, 1d));
        putAndCheckValue(1L, sql("this, this"), DOUBLE, function.process(1d, 1d));
        putAndCheckValue(BigInteger.ONE, sql("this, this"), DOUBLE, function.process(1d, 1d));
        putAndCheckValue(BigDecimal.ONE, sql("this, this"), DOUBLE, function.process(1d, 1d));
        putAndCheckValue(1f, sql("this, this"), DOUBLE, function.process(1d, 1d));
        putAndCheckValue(1d, sql("this, this"), DOUBLE, function.process(1d, 1d));

        // Other
        putAndCheckFailure('1', sql("this, this"), SqlErrorCode.PARSING,
                signatureErrorFunction(function.name, VARCHAR, VARCHAR));
        putAndCheckFailure("1", sql("this, this"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, VARCHAR, VARCHAR));
        putAndCheckFailure(true, sql("this, this"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, BOOLEAN, BOOLEAN));
        putAndCheckFailure(LOCAL_DATE_VAL, sql("this, this"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, DATE, DATE));
        putAndCheckFailure(LOCAL_TIME_VAL, sql("this, this"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, TIME, TIME));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql("this, this"), SqlErrorCode.PARSING,
                signatureErrorFunction(function.name, TIMESTAMP, TIMESTAMP));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql("this, this"), SqlErrorCode.PARSING,
                signatureErrorFunction(function.name, TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(OBJECT_VAL, sql("this, this"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, OBJECT, OBJECT));
    }

    @Test
    public void testLiteral() {
        put(0);

        checkValue0(sql("1, null"), DOUBLE, null);
        checkValue0(sql("null, 1"), DOUBLE, null);
        checkValue0(sql("null, null"), DOUBLE, null);
        checkValue0(sql("1, 1"), DOUBLE, function.process(1d, 1d));
        checkValue0(sql("100, 100"), DOUBLE, function.process(100d, 100d));
        checkValue0(sql("1.0001, 1.0001"), DOUBLE, function.process(1.0001d, 1.0001d));
        checkValue0(sql("0.0001, 0.0001"), DOUBLE, function.process(0.0001d, 0.0001d));
        checkValue0(sql("1.0, 1E0"), DOUBLE, function.process(1d, 1d));
        checkValue0(sql("1.0, 1.0"), DOUBLE, function.process(1d, 1d));
        checkValue0(sql("1E0, 1E0"), DOUBLE, function.process(1d, 1d));

        checkFailure0(sql("false, true"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, BOOLEAN, BOOLEAN));
        checkFailure0(sql("'1', '1'"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, VARCHAR, VARCHAR));
    }

    @Test
    public void testParameter() {
        put(0);

        checkValue0(sql("?, ?"), DOUBLE, null, null, null);

        checkValue0(sql("?, ?"), DOUBLE, function.process(1d, 1d), (byte) 1, (byte) 1);
        checkValue0(sql("?, ?"), DOUBLE, function.process(1d, 1d), (short) 1, (short) 1);
        checkValue0(sql("?, ?"), DOUBLE, function.process(1d, 1d), 1, 1);
        checkValue0(sql("?, ?"), DOUBLE, function.process(1d, 1d), 1L, 1L);
        checkValue0(sql("?, ?"), DOUBLE, function.process(1d, 1d), BigInteger.ONE, BigInteger.ONE);
        checkValue0(sql("?, ?"), DOUBLE, function.process(1d, 1d), BigDecimal.ONE, BigDecimal.ONE);
        checkValue0(sql("?, ?"), DOUBLE, function.process(1d, 1d), 1f, 1f);
        checkValue0(sql("?, ?"), DOUBLE, function.process(1d, 1d), 1d, 1d);
        checkValue0(sql("?, ?"), DOUBLE, function.process(1.001d, 1.001d), 1.001d, 1.001d);

        checkFailure0(sql("?, ?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, DOUBLE, VARCHAR), '1', '1');
        checkFailure0(sql("?, ?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, DOUBLE, VARCHAR), "1", "1");
        checkFailure0(sql("?, ?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, DOUBLE, BOOLEAN), true, false);
        checkFailure0(sql("?, ?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, DOUBLE, DATE), LOCAL_DATE_VAL, LOCAL_DATE_VAL);
        checkFailure0(sql("?, ?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, DOUBLE, TIME), LOCAL_TIME_VAL, LOCAL_TIME_VAL);
        checkFailure0(sql("?, ?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, DOUBLE, TIMESTAMP), LOCAL_DATE_TIME_VAL, LOCAL_DATE_TIME_VAL);
        checkFailure0(sql("?, ?"), SqlErrorCode.DATA_EXCEPTION,
                parameterError(0, DOUBLE, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL, OFFSET_DATE_TIME_VAL);
        checkFailure0(sql("?, ?"), SqlErrorCode.DATA_EXCEPTION,
                parameterError(0, DOUBLE, OBJECT), OBJECT_VAL, OBJECT_VAL);
    }

    private String sql(String operand) {
        return "SELECT " + function.name + "(" + operand + ") FROM map";
    }

    @Test
    public void testEquals() {
        DoubleBiFunction function = DoubleBiFunction.create(
                ConstantExpression.create(1d, QueryDataType.DOUBLE),
                ConstantExpression.create(1d, QueryDataType.DOUBLE),
                this.function.code
        );
        DoubleBiFunction sameFunction = DoubleBiFunction.create(
                ConstantExpression.create(1d, QueryDataType.DOUBLE),
                ConstantExpression.create(1d, QueryDataType.DOUBLE),
                this.function.code
        );

        DoubleBiFunction differentFunction = DoubleBiFunction.create(
                ConstantExpression.create(0.5d, QueryDataType.DOUBLE),
                ConstantExpression.create(1d, QueryDataType.DOUBLE),
                this.function.code
        );

        checkEquals(function, sameFunction, true);
        checkEquals(function, differentFunction, false);
    }

    @Test
    public void testSerialization() {
        DoubleBiFunction original = DoubleBiFunction.create(
                ConstantExpression.create(1d, QueryDataType.DOUBLE),
                ConstantExpression.create(1d, QueryDataType.DOUBLE),
                function.code
        );
        DoubleBiFunction restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_DOUBLE_DOUBLE);

        checkEquals(original, restored, true);
    }

    private static final class FunctionInfo {

        public final int code;
        public final String name;

        private FunctionInfo(String name, int code) {
            this.code = code;
            this.name = name;
        }

        public Double process(Double lhs, Double rhs) {
            if (lhs == null || rhs == null) {
                return null;
            }
            switch (code) {
                case POWER:
                    return Math.pow(lhs, rhs);
                case ATAN2:
                    return Math.atan2(lhs, rhs);
                default:
                    throw new UnsupportedOperationException("Unsupported mode: " + code);
            }
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
