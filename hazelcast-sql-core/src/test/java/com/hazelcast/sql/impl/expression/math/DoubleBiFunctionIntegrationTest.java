/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.impl.type.QueryDataType;
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

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DoubleBiFunctionIntegrationTest extends ExpressionTestSupport {
    @Parameterized.Parameter
    public Mode mode;

    @Parameterized.Parameters(name = "mode: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
            { new Mode("ATAN2") }
        });
    }

    @Test
    public void testColumn() {

        // NULL
        putAndCheckValue(new ExpressionValue.DoubleVal(), sql("field1,  field1"), DOUBLE, null);

        // Numeric
        putAndCheckValue((byte) 1, sql("this, this"), DOUBLE, mode.process(1d, 1d));
        putAndCheckValue((short) 1, sql("this, this"), DOUBLE, mode.process(1d, 1d));
        putAndCheckValue(1, sql("this, this"), DOUBLE, mode.process(1d, 1d));
        putAndCheckValue(1L, sql("this, this"), DOUBLE, mode.process(1d, 1d));
        putAndCheckValue(1L, sql("this, this"), DOUBLE, mode.process(1d, 1d));
        putAndCheckValue(BigInteger.ONE, sql("this, this"), DOUBLE, mode.process(1d, 1d));
        putAndCheckValue(BigDecimal.ONE, sql("this, this"), DOUBLE, mode.process(1d, 1d));
        putAndCheckValue(1f, sql("this, this"), DOUBLE, mode.process(1d, 1d));
        putAndCheckValue(1d, sql("this, this"), DOUBLE, mode.process(1d, 1d));

        // Other
        putAndCheckFailure('1', sql("this, this"), SqlErrorCode.PARSING,
                signatureErrorFunction(mode.mode, VARCHAR, VARCHAR));
        putAndCheckFailure("1", sql("this, this"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, VARCHAR, VARCHAR));
        putAndCheckFailure(true, sql("this, this"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, BOOLEAN, BOOLEAN));
        putAndCheckFailure(LOCAL_DATE_VAL, sql("this, this"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, DATE, DATE));
        putAndCheckFailure(LOCAL_TIME_VAL, sql("this, this"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, TIME, TIME));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql("this, this"), SqlErrorCode.PARSING,
                signatureErrorFunction(mode.mode, TIMESTAMP, TIMESTAMP));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql("this, this"), SqlErrorCode.PARSING,
                signatureErrorFunction(mode.mode, TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(OBJECT_VAL, sql("this, this"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, OBJECT, OBJECT));
    }

    @Test
    public void testLiteral() {
        put(0);

        checkValue0(sql("1, null"), DOUBLE, null);
        checkValue0(sql("null, 1"), DOUBLE, null);
        checkValue0(sql("null, null"), DOUBLE, null);
        checkValue0(sql("1, 1"), DOUBLE, mode.process(1d, 1d));
        checkValue0(sql("100, 100"), DOUBLE, mode.process(100d, 100d));
        checkValue0(sql("1.0001, 1.0001"), DOUBLE, mode.process(1.0001d, 1.0001d));
        checkValue0(sql("0.0001, 0.0001"), DOUBLE, mode.process(0.0001d, 0.0001d));
        checkValue0(sql("1.0, 1E0"), DOUBLE, mode.process(1d, 1d));
        checkValue0(sql("1.0, 1.0"), DOUBLE, mode.process(1d, 1d));
        checkValue0(sql("1E0, 1E0"), DOUBLE, mode.process(1d, 1d));

        checkFailure0(sql("false, true"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, BOOLEAN, BOOLEAN));
        checkFailure0(sql("'1', '1'"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, VARCHAR, VARCHAR));
    }

    @Test
    public void testParameter() {
        put(0);

        checkValue0(sql("?, ?"), DOUBLE, null, null, null);

        checkValue0(sql("?, ?"), DOUBLE, mode.process(1d, 1d), (byte) 1, (byte) 1);
        checkValue0(sql("?, ?"), DOUBLE, mode.process(1d, 1d), (short) 1, (short) 1);
        checkValue0(sql("?, ?"), DOUBLE, mode.process(1d, 1d), 1, 1);
        checkValue0(sql("?, ?"), DOUBLE, mode.process(1d, 1d), 1L, 1L);
        checkValue0(sql("?, ?"), DOUBLE, mode.process(1d, 1d), BigInteger.ONE, BigInteger.ONE);
        checkValue0(sql("?, ?"), DOUBLE, mode.process(1d, 1d), BigDecimal.ONE, BigDecimal.ONE);
        checkValue0(sql("?, ?"), DOUBLE, mode.process(1d, 1d), 1f, 1f);
        checkValue0(sql("?, ?"), DOUBLE, mode.process(1d, 1d), 1d, 1d);
        checkValue0(sql("?, ?"), DOUBLE, mode.process(1.001d, 1.001d), 1.001d, 1.001d);

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
        return "SELECT " + mode.mode + "(" + operand + ") FROM map";
    }

    @Test
    public void testEquals() {
        DoubleBiFunction function = DoubleBiFunction.create(
                ConstantExpression.create(1d, QueryDataType.DOUBLE),
                ConstantExpression.create(1d, QueryDataType.DOUBLE),
                ATAN2
        );
        DoubleBiFunction sameFunction = DoubleBiFunction.create(
                ConstantExpression.create(1d, QueryDataType.DOUBLE),
                ConstantExpression.create(1d, QueryDataType.DOUBLE),
                ATAN2
        );

        DoubleBiFunction differentFunction = DoubleBiFunction.create(
                ConstantExpression.create(0.5d, QueryDataType.DOUBLE),
                ConstantExpression.create(1d, QueryDataType.DOUBLE),
                ATAN2
        );

        checkEquals(function, sameFunction, true);
        checkEquals(function, differentFunction, false);
    }

    @Test
    public void testSerialization() {
        DoubleBiFunction original = DoubleBiFunction.create(
                ConstantExpression.create(1d, QueryDataType.DOUBLE),
                ConstantExpression.create(1d, QueryDataType.DOUBLE),
                ATAN2
        );
        DoubleBiFunction restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_DOUBLE_DOUBLE);

        checkEquals(original, restored, true);
    }

    private static final class Mode {

        private final String mode;

        private Mode(String mode) {
            this.mode = mode;
        }

        public Double process(Double lhs, Double rhs) {
            if (lhs == null || rhs == null) {
                return null;
            }
            //noinspection SwitchStatementWithTooFewBranches
            switch (mode) {
                case "ATAN2":
                    return Math.atan2(lhs, rhs);
                default:
                    throw new UnsupportedOperationException("Unsupported mode: " + mode);
            }
        }

        @Override
        public String toString() {
            return mode;
        }
    }
}
