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
import com.hazelcast.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionBiValue;
import com.hazelcast.sql.support.expressions.ExpressionType;
import com.hazelcast.sql.support.expressions.ExpressionTypes;
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
import java.util.List;

import static com.hazelcast.sql.support.expressions.ExpressionBiValue.createBiClass;
import static com.hazelcast.sql.support.expressions.ExpressionBiValue.createBiValue;

@SuppressWarnings("rawtypes")
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ComparisonPredicateIntegrationTest extends ExpressionTestSupport {

    private static final int RES_EQ = 0;
    private static final int RES_LT = -1;
    private static final int RES_GT = 1;
    private static final Integer RES_NULL = null;

    @Parameterized.Parameter
    public Mode mode;

    @Parameterized.Parameters(name = "mode:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
            { Mode.EQ },
            { Mode.NEQ },
            { Mode.LT },
            { Mode.LTE },
            { Mode.GT },
            { Mode.GTE },
        });
    }

    private static final Literal LITERAL_BOOLEAN = new Literal("true", SqlColumnType.BOOLEAN);
    private static final Literal LITERAL_VARCHAR = new Literal("'true'", SqlColumnType.VARCHAR);
    private static final Literal LITERAL_TINYINT = new Literal("1", SqlColumnType.TINYINT);
    private static final Literal LITERAL_DECIMAL = new Literal("1.1", SqlColumnType.DECIMAL);
    private static final Literal LITERAL_DOUBLE = new Literal("1.1E1", SqlColumnType.DOUBLE);

    @Test
    public void testString() {
        // TODO
        checkUnsupportedColumnColumn(ExpressionTypes.STRING, ExpressionTypes.allExcept(ExpressionTypes.STRING, ExpressionTypes.CHARACTER));
        checkUnsupportedColumnColumn(ExpressionTypes.CHARACTER, ExpressionTypes.allExcept(ExpressionTypes.STRING, ExpressionTypes.CHARACTER));
    }

    @Test
    public void testBoolean() {
        // Column/column
        putCheckCommute(booleanValue2(true, true), "field1", "field2", RES_EQ);
        putCheckCommute(booleanValue2(true, false), "field1", "field2", RES_GT);
        putCheckCommute(booleanValue2(true, null), "field1", "field2", RES_NULL);
        putCheckCommute(booleanValue2(false, false), "field1", "field2", RES_EQ);
        putCheckCommute(booleanValue2(false, null), "field1", "field2", RES_NULL);
        putCheckCommute(booleanValue2(null, null), "field1", "field2", RES_NULL);
        checkUnsupportedColumnColumn(ExpressionTypes.BOOLEAN, ExpressionTypes.allExcept(ExpressionTypes.BOOLEAN));

        // Column/literal
        putCheckCommute(booleanValue1(true), "field1", "true", RES_EQ);
        putCheckCommute(booleanValue1(true), "field1", "false", RES_GT);
        putCheckCommute(booleanValue1(true), "field1", "null", RES_NULL);
        putCheckCommute(booleanValue1(false), "field1", "true", RES_LT);
        putCheckCommute(booleanValue1(false), "field1", "false", RES_EQ);
        putCheckCommute(booleanValue1(false), "field1", "null", RES_NULL);
        putCheckCommute(booleanValue1(null), "field1", "true", RES_NULL);
        putCheckCommute(booleanValue1(null), "field1", "false", RES_NULL);
        putCheckCommute(booleanValue1(null), "field1", "null", RES_NULL);
        checkUnsupportedColumnLiteral(true, SqlColumnType.BOOLEAN, LITERAL_VARCHAR, LITERAL_TINYINT, LITERAL_DECIMAL, LITERAL_DOUBLE);

        // Column/parameter
        putCheckCommute(booleanValue1(true), "field1", "?", RES_EQ, true);
        putCheckCommute(booleanValue1(true), "field1", "?", RES_GT, false);
        putCheckCommute(booleanValue1(true), "field1", "?", RES_NULL, (Boolean) null);
        putCheckCommute(booleanValue1(false), "field1", "?", RES_LT, true);
        putCheckCommute(booleanValue1(false), "field1", "?", RES_EQ, false);
        putCheckCommute(booleanValue1(false), "field1", "?", RES_NULL, (Boolean) null);
        putCheckCommute(booleanValue1(null), "field1", "?", RES_NULL, true);
        putCheckCommute(booleanValue1(null), "field1", "?", RES_NULL, false);
        putCheckCommute(booleanValue1(null), "field1", "?", RES_NULL, (Boolean) null);
        checkUnsupportedColumnParameter(true, SqlColumnType.BOOLEAN, 0, ExpressionTypes.allExcept(ExpressionTypes.BOOLEAN));

        // Literal/literal
        checkCommute("true", "true", RES_EQ);
        checkCommute("true", "false", RES_GT);
        checkCommute("true", "null", RES_NULL);
        checkCommute("false", "false", RES_EQ);
        checkCommute("false", "null", RES_NULL);
        checkFailure("null", "null", SqlErrorCode.PARSING, signatureErrorOperator(mode.token(), SqlColumnType.NULL, SqlColumnType.NULL));
        checkUnsupportedLiteralLiteral("true", SqlColumnType.BOOLEAN, LITERAL_VARCHAR, LITERAL_TINYINT, LITERAL_DECIMAL, LITERAL_DOUBLE);

        // Literal/parameter
        checkCommute("true", "?", RES_EQ, true);
        checkCommute("true", "?", RES_GT, false);
        checkCommute("true", "?", RES_NULL, (Boolean) null);
        checkCommute("false", "?", RES_LT, true);
        checkCommute("false", "?", RES_EQ, false);
        checkCommute("false", "?", RES_NULL, (Boolean) null);
        checkFailure("null", "?", SqlErrorCode.PARSING, signatureErrorOperator(mode.token(), SqlColumnType.NULL, SqlColumnType.NULL), true);

        // Parameter/parameter
        checkFailure("?", "?", SqlErrorCode.PARSING, signatureErrorOperator(mode.token(), SqlColumnType.NULL, SqlColumnType.NULL), true, true);
    }

    @Test
    public void testNumeric() {
        // TODO
    }

    @Test
    public void testTemporal() {
        // TODO
    }

    @Test
    public void testObject() {
        // TODO
    }

    private void checkUnsupportedColumnColumn(ExpressionType<?> type, List<ExpressionType<?>> excludeTypes) {
        for (ExpressionType<?> expressionType : excludeTypes) {
            ExpressionBiValue value = ExpressionBiValue.createBiValue(
                ExpressionBiValue.createBiClass(type, expressionType),
                null,
                null
            );

            put(value);

            String sql = sql(mode.token(), "field1", "field2");

            String errorMessage = signatureErrorOperator(
                mode.token(),
                type.getFieldConverterType().getTypeFamily().getPublicType(),
                expressionType.getFieldConverterType().getTypeFamily().getPublicType()
            );

            checkFailure0(sql, SqlErrorCode.PARSING, errorMessage);
        }
    }

    private void checkUnsupportedColumnLiteral(Object columnValue, SqlColumnType columnType, Literal... literals) {
        for (Literal literal : literals) {
            put(columnValue);

            String sql = sql(mode.token(), "this", literal.value);

            String errorMessage = signatureErrorOperator(
                mode.token(),
                columnType,
                literal.type
            );

            checkFailure0(sql, SqlErrorCode.PARSING, errorMessage);
        }
    }

    private void checkUnsupportedColumnParameter(Object columnValue, SqlColumnType columnType, int paramPos, List<ExpressionType<?>> parameterTypes) {
        for (ExpressionType<?> parameterType : parameterTypes) {
            put(columnValue);

            String sql = sql(mode.token(), "this", "?");

            String errorMessage = parameterError(
                paramPos,
                columnType,
                parameterType.getFieldConverterType().getTypeFamily().getPublicType()
            );

            checkFailure0(sql, SqlErrorCode.DATA_EXCEPTION, errorMessage, parameterType.valueFrom());
        }
    }

    private void checkUnsupportedLiteralLiteral(String literalValue, SqlColumnType literalType, Literal... literals) {
        for (Literal literal : literals) {
            put(1);

            String sql = sql(mode.token(), literalValue, literal.value);

            String errorMessage = signatureErrorOperator(
                mode.token(),
                literalType,
                literal.type
            );

            checkFailure0(sql, SqlErrorCode.PARSING, errorMessage);
        }
    }

    private void putCheckCommute(Object value, String operand1, String operand2, Integer expectedResult, Object... params) {
        put(value);

        checkCommute(operand1, operand2, expectedResult, params);
    }

    private void checkCommute(String operand1, String operand2, Integer expectedResult, Object... params) {
        check(operand1, operand2, expectedResult, params);
        check(operand2, operand1, inverse(expectedResult), params);
    }

    private void check(
        String operand1,
        String operand2,
        Integer expectedRes,
        Object... params
    ) {
        Boolean expectedValue = compare(expectedRes);

        for (String token : mode.tokens) {
            String sql = sql(token, operand1, operand2);

            checkValue0(sql, SqlColumnType.BOOLEAN, expectedValue, params);
        }
    }






    @Test
    public void test_column_column() {
        // TINYINT/TINYINT
        Class<? extends ExpressionBiValue> clazz = createBiClass(ExpressionTypes.BYTE, ExpressionTypes.BYTE);
        checkColumnColumn(clazz, (byte) 0, (byte) 0, RES_EQ);
        checkColumnColumn(clazz, (byte) 0, Byte.MAX_VALUE, RES_LT);
        checkColumnColumn(clazz, (byte) 0, Byte.MIN_VALUE, RES_GT);
        checkColumnColumn(clazz, (byte) 0, null, RES_NULL);

        // TINYINT/SMALLINT
        clazz = createBiClass(ExpressionTypes.BYTE, ExpressionTypes.SHORT);
        checkColumnColumn(clazz, (byte) 0, (short) 0, RES_EQ);
        checkColumnColumn(clazz, (byte) 0, Short.MAX_VALUE, RES_LT);
        checkColumnColumn(clazz, (byte) 0, Short.MIN_VALUE, RES_GT);

        // TINYINT/INTEGER
        clazz = createBiClass(ExpressionTypes.BYTE, ExpressionTypes.INTEGER);
        checkColumnColumn(clazz, (byte) 0, 0, RES_EQ);
        checkColumnColumn(clazz, (byte) 0, Integer.MAX_VALUE, RES_LT);
        checkColumnColumn(clazz, (byte) 0, Integer.MIN_VALUE, RES_GT);

        // TINYINT/BIGINT
        clazz = createBiClass(ExpressionTypes.BYTE, ExpressionTypes.LONG);
        checkColumnColumn(clazz, (byte) 0, 0L, RES_EQ);
        checkColumnColumn(clazz, (byte) 0, Long.MAX_VALUE, RES_LT);
        checkColumnColumn(clazz, (byte) 0, Long.MIN_VALUE, RES_GT);

        // TINYINT/DECIMAL
        clazz = createBiClass(ExpressionTypes.BYTE, ExpressionTypes.BIG_INTEGER);
        checkColumnColumn(clazz, (byte) 0, BigInteger.ZERO, RES_EQ);
        checkColumnColumn(clazz, (byte) 0, BigInteger.ONE, RES_LT);
        checkColumnColumn(clazz, (byte) 0, BigInteger.ONE.negate(), RES_GT);

        clazz = createBiClass(ExpressionTypes.BYTE, ExpressionTypes.BIG_DECIMAL);
        checkColumnColumn(clazz, (byte) 0, BigDecimal.ZERO, RES_EQ);
        checkColumnColumn(clazz, (byte) 0, BigDecimal.ONE, RES_LT);
        checkColumnColumn(clazz, (byte) 0, BigDecimal.ONE.negate(), RES_GT);

        // TINYINT/REAL
        clazz = createBiClass(ExpressionTypes.BYTE, ExpressionTypes.FLOAT);
        checkColumnColumn(clazz, (byte) 0, 0f, RES_EQ);
        checkColumnColumn(clazz, (byte) 0, 1f, RES_LT);
        checkColumnColumn(clazz, (byte) 0, -1f, RES_GT);

        // TINYINT/DOUBLE
        clazz = createBiClass(ExpressionTypes.BYTE, ExpressionTypes.DOUBLE);
        checkColumnColumn(clazz, (byte) 0, 0d, RES_EQ);
        checkColumnColumn(clazz, (byte) 0, 1d, RES_LT);
        checkColumnColumn(clazz, (byte) 0, -1d, RES_GT);

        // TINYINT/VARCHAR
        clazz = createBiClass(ExpressionTypes.BYTE, ExpressionTypes.CHARACTER);
        checkColumnColumnFailure(clazz, (byte) 0, 'b', SqlErrorCode.PARSING, "Cannot apply [TINYINT, VARCHAR]");

        clazz = createBiClass(ExpressionTypes.BYTE, ExpressionTypes.STRING);
        checkColumnColumnFailure(clazz, (byte) 0, "bad", SqlErrorCode.PARSING, "Cannot apply [TINYINT, VARCHAR]");

        // TINYINT/OBJECT
        clazz = createBiClass(ExpressionTypes.BYTE, ExpressionTypes.OBJECT);
        checkColumnColumnFailure(clazz, (byte) 0, 1, SqlErrorCode.PARSING, "Cannot apply [TINYINT, OBJECT]");

        // SMALLINT/SMALLINT
        clazz = createBiClass(ExpressionTypes.SHORT, ExpressionTypes.SHORT);
        checkColumnColumn(clazz, (short) 0, (short) 0, RES_EQ);
        checkColumnColumn(clazz, (short) 0, Short.MAX_VALUE, RES_LT);
        checkColumnColumn(clazz, (short) 0, Short.MIN_VALUE, RES_GT);

        // SMALLINT/INTEGER
        clazz = createBiClass(ExpressionTypes.SHORT, ExpressionTypes.INTEGER);
        checkColumnColumn(clazz, (short) 0, 0, RES_EQ);
        checkColumnColumn(clazz, (short) 0, Integer.MAX_VALUE, RES_LT);
        checkColumnColumn(clazz, (short) 0, Integer.MIN_VALUE, RES_GT);

        // SMALLINT/BIGINT
        clazz = createBiClass(ExpressionTypes.SHORT, ExpressionTypes.LONG);
        checkColumnColumn(clazz, (short) 0, 0L, RES_EQ);
        checkColumnColumn(clazz, (short) 0, Long.MAX_VALUE, RES_LT);
        checkColumnColumn(clazz, (short) 0, Long.MIN_VALUE, RES_GT);

        // SMALLINT/DECIMAL
        clazz = createBiClass(ExpressionTypes.SHORT, ExpressionTypes.BIG_INTEGER);
        checkColumnColumn(clazz, (short) 0, BigInteger.ZERO, RES_EQ);
        checkColumnColumn(clazz, (short) 0, BigInteger.ONE, RES_LT);
        checkColumnColumn(clazz, (short) 0, BigInteger.ONE.negate(), RES_GT);

        clazz = createBiClass(ExpressionTypes.SHORT, ExpressionTypes.BIG_DECIMAL);
        checkColumnColumn(clazz, (short) 0, BigDecimal.ZERO, RES_EQ);
        checkColumnColumn(clazz, (short) 0, BigDecimal.ONE, RES_LT);
        checkColumnColumn(clazz, (short) 0, BigDecimal.ONE.negate(), RES_GT);

        // SMALLINT/REAL
        clazz = createBiClass(ExpressionTypes.SHORT, ExpressionTypes.FLOAT);
        checkColumnColumn(clazz, (short) 0, 0f, RES_EQ);
        checkColumnColumn(clazz, (short) 0, 1f, RES_LT);
        checkColumnColumn(clazz, (short) 0, -1f, RES_GT);

        // SMALLINT/DOUBLE
        clazz = createBiClass(ExpressionTypes.SHORT, ExpressionTypes.DOUBLE);
        checkColumnColumn(clazz, (short) 0, 0d, RES_EQ);
        checkColumnColumn(clazz, (short) 0, 1d, RES_LT);
        checkColumnColumn(clazz, (short) 0, -1d, RES_GT);

        // SMALLINT/VARCHAR
        clazz = createBiClass(ExpressionTypes.SHORT, ExpressionTypes.CHARACTER);
        checkColumnColumnFailure(clazz, (short) 0, 'b', SqlErrorCode.PARSING, "Cannot apply [SMALLINT, VARCHAR]");

        clazz = createBiClass(ExpressionTypes.SHORT, ExpressionTypes.STRING);
        checkColumnColumnFailure(clazz, (short) 0, "bad", SqlErrorCode.PARSING, "Cannot apply [SMALLINT, VARCHAR]");

        // SMALLINT/OBJECT
        clazz = createBiClass(ExpressionTypes.SHORT, ExpressionTypes.OBJECT);
        checkColumnColumnFailure(clazz, (short) 0, 1, SqlErrorCode.PARSING, "Cannot apply [SMALLINT, OBJECT]");

        // INTEGER/INTEGER
        clazz = createBiClass(ExpressionTypes.INTEGER, ExpressionTypes.INTEGER);
        checkColumnColumn(clazz, 0, 0, RES_EQ);
        checkColumnColumn(clazz, 0, Integer.MAX_VALUE, RES_LT);
        checkColumnColumn(clazz, 0, Integer.MIN_VALUE, RES_GT);

        // INTEGER/BIGINT
        clazz = createBiClass(ExpressionTypes.INTEGER, ExpressionTypes.LONG);
        checkColumnColumn(clazz, 0, 0L, RES_EQ);
        checkColumnColumn(clazz, 0, Long.MAX_VALUE, RES_LT);
        checkColumnColumn(clazz, 0, Long.MIN_VALUE, RES_GT);

        // INTEGER/DECIMAL
        clazz = createBiClass(ExpressionTypes.INTEGER, ExpressionTypes.BIG_INTEGER);
        checkColumnColumn(clazz, 0, BigInteger.ZERO, RES_EQ);
        checkColumnColumn(clazz, 0, BigInteger.ONE, RES_LT);
        checkColumnColumn(clazz, 0, BigInteger.ONE.negate(), RES_GT);

        clazz = createBiClass(ExpressionTypes.INTEGER, ExpressionTypes.BIG_DECIMAL);
        checkColumnColumn(clazz, 0, BigDecimal.ZERO, RES_EQ);
        checkColumnColumn(clazz, 0, BigDecimal.ONE, RES_LT);
        checkColumnColumn(clazz, 0, BigDecimal.ONE.negate(), RES_GT);

        // INTEGER/REAL
        clazz = createBiClass(ExpressionTypes.INTEGER, ExpressionTypes.FLOAT);
        checkColumnColumn(clazz, 0, 0f, RES_EQ);
        checkColumnColumn(clazz, 0, 1f, RES_LT);
        checkColumnColumn(clazz, 0, -1f, RES_GT);

        // INTEGER/DOUBLE
        clazz = createBiClass(ExpressionTypes.INTEGER, ExpressionTypes.DOUBLE);
        checkColumnColumn(clazz, 0, 0d, RES_EQ);
        checkColumnColumn(clazz, 0, 1d, RES_LT);
        checkColumnColumn(clazz, 0, -1d, RES_GT);

        // INTEGER/VARCHAR
        clazz = createBiClass(ExpressionTypes.INTEGER, ExpressionTypes.CHARACTER);
        checkColumnColumnFailure(clazz, 0, 'b', SqlErrorCode.PARSING, "Cannot apply [INTEGER, VARCHAR]");

        clazz = createBiClass(ExpressionTypes.INTEGER, ExpressionTypes.STRING);
        checkColumnColumnFailure(clazz, 0, "bad", SqlErrorCode.PARSING, "Cannot apply [INTEGER, VARCHAR]");

        // INTEGER/OBJECT
        clazz = createBiClass(ExpressionTypes.INTEGER, ExpressionTypes.OBJECT);
        checkColumnColumnFailure(clazz, 0, 1, SqlErrorCode.PARSING, "Cannot apply [INTEGER, OBJECT]");

        // BIGINT/BIGINT
        clazz = createBiClass(ExpressionTypes.LONG, ExpressionTypes.LONG);
        checkColumnColumn(clazz, 0L, 0L, RES_EQ);
        checkColumnColumn(clazz, 0L, Long.MAX_VALUE, RES_LT);
        checkColumnColumn(clazz, 0L, Long.MIN_VALUE, RES_GT);

        // BIGINT/DECIMAL
        clazz = createBiClass(ExpressionTypes.LONG, ExpressionTypes.BIG_INTEGER);
        checkColumnColumn(clazz, 0L, BigInteger.ZERO, RES_EQ);
        checkColumnColumn(clazz, 0L, BigInteger.ONE, RES_LT);
        checkColumnColumn(clazz, 0L, BigInteger.ONE.negate(), RES_GT);

        clazz = createBiClass(ExpressionTypes.LONG, ExpressionTypes.BIG_DECIMAL);
        checkColumnColumn(clazz, 0L, BigDecimal.ZERO, RES_EQ);
        checkColumnColumn(clazz, 0L, BigDecimal.ONE, RES_LT);
        checkColumnColumn(clazz, 0L, BigDecimal.ONE.negate(), RES_GT);

        // BIGINT/REAL
        clazz = createBiClass(ExpressionTypes.LONG, ExpressionTypes.FLOAT);
        checkColumnColumn(clazz, 0L, 0f, RES_EQ);
        checkColumnColumn(clazz, 0L, 1f, RES_LT);
        checkColumnColumn(clazz, 0L, -1f, RES_GT);

        // BIGINT/DOUBLE
        clazz = createBiClass(ExpressionTypes.LONG, ExpressionTypes.DOUBLE);
        checkColumnColumn(clazz, 0L, 0d, RES_EQ);
        checkColumnColumn(clazz, 0L, 1d, RES_LT);
        checkColumnColumn(clazz, 0L, -1d, RES_GT);

        // BIGINT/VARCHAR
        clazz = createBiClass(ExpressionTypes.LONG, ExpressionTypes.CHARACTER);
        checkColumnColumnFailure(clazz, 0L, 'b', SqlErrorCode.PARSING, "Cannot apply [BIGINT, VARCHAR]");

        clazz = createBiClass(ExpressionTypes.LONG, ExpressionTypes.STRING);
        checkColumnColumnFailure(clazz, 0L, "bad", SqlErrorCode.PARSING, "Cannot apply [BIGINT, VARCHAR]");

        // BIGINT/OBJECT
        clazz = createBiClass(ExpressionTypes.LONG, ExpressionTypes.OBJECT);
        checkColumnColumnFailure(clazz, 0L, 1, SqlErrorCode.PARSING, "Cannot apply [BIGINT, OBJECT]");

        // DECIMAL(BigInteger)/DECIMAL
        clazz = createBiClass(ExpressionTypes.BIG_INTEGER, ExpressionTypes.BIG_INTEGER);
        checkColumnColumn(clazz, BigInteger.ZERO, BigInteger.ZERO, RES_EQ);
        checkColumnColumn(clazz, BigInteger.ZERO, BigInteger.ONE, RES_LT);
        checkColumnColumn(clazz, BigInteger.ZERO, BigInteger.ONE.negate(), RES_GT);

        clazz = createBiClass(ExpressionTypes.BIG_INTEGER, ExpressionTypes.BIG_DECIMAL);
        checkColumnColumn(clazz, BigInteger.ZERO, BigDecimal.ZERO, RES_EQ);
        checkColumnColumn(clazz, BigInteger.ZERO, BigDecimal.ONE, RES_LT);
        checkColumnColumn(clazz, BigInteger.ZERO, BigDecimal.ONE.negate(), RES_GT);

        // DECIMAL(BigInteger)/REAL
        clazz = createBiClass(ExpressionTypes.BIG_INTEGER, ExpressionTypes.FLOAT);
        checkColumnColumn(clazz, BigInteger.ZERO, 0f, RES_EQ);
        checkColumnColumn(clazz, BigInteger.ZERO, 1f, RES_LT);
        checkColumnColumn(clazz, BigInteger.ZERO, -1f, RES_GT);

        // DECIMAL(BigInteger)/DOUBLE
        clazz = createBiClass(ExpressionTypes.BIG_INTEGER, ExpressionTypes.DOUBLE);
        checkColumnColumn(clazz, BigInteger.ZERO, 0d, RES_EQ);
        checkColumnColumn(clazz, BigInteger.ZERO, 1d, RES_LT);
        checkColumnColumn(clazz, BigInteger.ZERO, -1d, RES_GT);

        // DECIMAL(BigInteger)/VARCHAR
        clazz = createBiClass(ExpressionTypes.BIG_INTEGER, ExpressionTypes.CHARACTER);
        checkColumnColumnFailure(clazz, BigInteger.ZERO, 'b', SqlErrorCode.PARSING, "Cannot apply [DECIMAL, VARCHAR]");

        clazz = createBiClass(ExpressionTypes.BIG_INTEGER, ExpressionTypes.STRING);
        checkColumnColumnFailure(clazz, BigInteger.ZERO, "bad", SqlErrorCode.PARSING, "Cannot apply [DECIMAL, VARCHAR]");

        // DECIMAL(BigInteger)/OBJECT
        clazz = createBiClass(ExpressionTypes.BIG_INTEGER, ExpressionTypes.OBJECT);
        checkColumnColumnFailure(clazz, BigInteger.ZERO, 1, SqlErrorCode.PARSING, "Cannot apply [DECIMAL, OBJECT]");

        // BIGINT(BigDecimal)/DECIMAL
        clazz = createBiClass(ExpressionTypes.BIG_DECIMAL, ExpressionTypes.BIG_DECIMAL);
        checkColumnColumn(clazz, BigDecimal.ZERO, BigDecimal.ZERO, RES_EQ);
        checkColumnColumn(clazz, new BigDecimal("0"), new BigDecimal("0.0"), RES_EQ);
        checkColumnColumn(clazz, BigDecimal.ZERO, BigDecimal.ONE, RES_LT);
        checkColumnColumn(clazz, BigDecimal.ZERO, BigDecimal.ONE.negate(), RES_GT);

        // DECIMAL(BigDecimal)/REAL
        clazz = createBiClass(ExpressionTypes.BIG_DECIMAL, ExpressionTypes.FLOAT);
        checkColumnColumn(clazz, BigDecimal.ZERO, 0f, RES_EQ);
        checkColumnColumn(clazz, BigDecimal.ZERO, 1f, RES_LT);
        checkColumnColumn(clazz, BigDecimal.ZERO, -1f, RES_GT);

        // DECIMAL(BigDecimal)/DOUBLE
        clazz = createBiClass(ExpressionTypes.BIG_DECIMAL, ExpressionTypes.DOUBLE);
        checkColumnColumn(clazz, BigDecimal.ZERO, 0d, RES_EQ);
        checkColumnColumn(clazz, BigDecimal.ZERO, 1d, RES_LT);
        checkColumnColumn(clazz, BigDecimal.ZERO, -1d, RES_GT);

        // DECIMAL(BigDecimal)/VARCHAR
        clazz = createBiClass(ExpressionTypes.BIG_DECIMAL, ExpressionTypes.CHARACTER);
        checkColumnColumnFailure(clazz, BigDecimal.ZERO, 'b', SqlErrorCode.PARSING, "Cannot apply [DECIMAL, VARCHAR]");

        clazz = createBiClass(ExpressionTypes.BIG_DECIMAL, ExpressionTypes.STRING);
        checkColumnColumnFailure(clazz, BigDecimal.ZERO, "bad", SqlErrorCode.PARSING, "Cannot apply [DECIMAL, VARCHAR]");

        // DECIMAL(BigDecimal)/OBJECT
        clazz = createBiClass(ExpressionTypes.BIG_DECIMAL, ExpressionTypes.OBJECT);
        checkColumnColumnFailure(clazz, BigDecimal.ZERO, 1, SqlErrorCode.PARSING, "Cannot apply [DECIMAL, OBJECT]");

        // REAL/REAL
        clazz = createBiClass(ExpressionTypes.FLOAT, ExpressionTypes.FLOAT);
        checkColumnColumn(clazz, 0f, 0f, RES_EQ);
        checkColumnColumn(clazz, 0f, 1f, RES_LT);
        checkColumnColumn(clazz, 0f, -1f, RES_GT);

        // REAL/DOUBLE
        clazz = createBiClass(ExpressionTypes.FLOAT, ExpressionTypes.DOUBLE);
        checkColumnColumn(clazz, 0f, 0d, RES_EQ);
        checkColumnColumn(clazz, 0f, 1d, RES_LT);
        checkColumnColumn(clazz, 0f, -1d, RES_GT);

        // REAL/VARCHAR
        clazz = createBiClass(ExpressionTypes.FLOAT, ExpressionTypes.CHARACTER);
        checkColumnColumnFailure(clazz, 0f, 'b', SqlErrorCode.PARSING, "Cannot apply [REAL, VARCHAR]");

        clazz = createBiClass(ExpressionTypes.FLOAT, ExpressionTypes.STRING);
        checkColumnColumnFailure(clazz, 0f, "bad", SqlErrorCode.PARSING, "Cannot apply [REAL, VARCHAR]");

        // REAL/OBJECT
        clazz = createBiClass(ExpressionTypes.FLOAT, ExpressionTypes.OBJECT);
        checkColumnColumnFailure(clazz, 0f, 1, SqlErrorCode.PARSING, "Cannot apply [REAL, OBJECT]");

        // DOUBLE/DOUBLE
        clazz = createBiClass(ExpressionTypes.DOUBLE, ExpressionTypes.DOUBLE);
        checkColumnColumn(clazz, 0d, 0d, RES_EQ);
        checkColumnColumn(clazz, 0d, 1d, RES_LT);
        checkColumnColumn(clazz, 0d, -1d, RES_GT);

        // DOUBLE/VARCHAR
        clazz = createBiClass(ExpressionTypes.DOUBLE, ExpressionTypes.CHARACTER);
        checkColumnColumnFailure(clazz, 0d, 'b', SqlErrorCode.PARSING, "Cannot apply [DOUBLE, VARCHAR]");

        clazz = createBiClass(ExpressionTypes.DOUBLE, ExpressionTypes.STRING);
        checkColumnColumnFailure(clazz, 0d, "bad", SqlErrorCode.PARSING, "Cannot apply [DOUBLE, VARCHAR]");

        // DOUBLE/OBJECT
        clazz = createBiClass(ExpressionTypes.DOUBLE, ExpressionTypes.OBJECT);
        checkColumnColumnFailure(clazz, 0d, 1, SqlErrorCode.PARSING, "Cannot apply [DOUBLE, OBJECT]");

        // VARCHAR(char)/VARCHAR
        clazz = createBiClass(ExpressionTypes.CHARACTER, ExpressionTypes.CHARACTER);
        checkColumnColumn(clazz, 'b', 'a', RES_GT);
        checkColumnColumn(clazz, 'b', 'b', RES_EQ);
        checkColumnColumn(clazz, 'b', 'c', RES_LT);

        clazz = createBiClass(ExpressionTypes.CHARACTER, ExpressionTypes.STRING);
        checkColumnColumn(clazz, 'b', "a", RES_GT);

        // VARCHAR(char)/OBJECT
        clazz = createBiClass(ExpressionTypes.CHARACTER, ExpressionTypes.OBJECT);
        checkColumnColumnFailure(clazz, '0', 1, SqlErrorCode.PARSING, "Cannot apply [VARCHAR, OBJECT]");

        // VARCHAR(char)/VARCHAR
        clazz = createBiClass(ExpressionTypes.STRING, ExpressionTypes.STRING);
        checkColumnColumn(clazz, "abc", "ab", RES_GT);
        checkColumnColumn(clazz, "abc", "abc", RES_EQ);
        checkColumnColumn(clazz, "abc", "abcd", RES_LT);

        // VARCHAR/OBJECT
        clazz = createBiClass(ExpressionTypes.STRING, ExpressionTypes.OBJECT);
        checkColumnColumnFailure(clazz, "abc", 1, SqlErrorCode.PARSING, "Cannot apply [VARCHAR, OBJECT]");

        // OBJECT/OBJECT
        // TODO: More tests
        clazz = createBiClass(ExpressionTypes.OBJECT, ExpressionTypes.OBJECT);
        checkColumnColumn(clazz, 1, 2, RES_LT);

        // Handle special case for temporal types
        clazz = createBiClass(ExpressionTypes.INTEGER, ExpressionTypes.LOCAL_DATE);
        checkColumnColumnFailure(clazz, 0, LOCAL_DATE_VAL, SqlErrorCode.PARSING, "Cannot apply [INTEGER, DATE]");

        clazz = createBiClass(ExpressionTypes.INTEGER, ExpressionTypes.LOCAL_TIME);
        checkColumnColumnFailure(clazz, 0, LOCAL_TIME_VAL, SqlErrorCode.PARSING, "Cannot apply [INTEGER, TIME]");

        clazz = createBiClass(ExpressionTypes.INTEGER, ExpressionTypes.LOCAL_DATE_TIME);
        checkColumnColumnFailure(clazz, 0, LOCAL_DATE_TIME_VAL, SqlErrorCode.PARSING, "Cannot apply [INTEGER, TIMESTAMP]");

        clazz = createBiClass(ExpressionTypes.INTEGER, ExpressionTypes.OFFSET_DATE_TIME);
        checkColumnColumnFailure(clazz, 0, OFFSET_DATE_TIME_VAL, SqlErrorCode.PARSING, "Cannot apply [INTEGER, TIMESTAMP_WITH_TIME_ZONE]");

        clazz = createBiClass(ExpressionTypes.LOCAL_DATE, ExpressionTypes.OFFSET_DATE_TIME);
        checkColumnColumnFailure(clazz, LOCAL_DATE_VAL, OFFSET_DATE_TIME_VAL, SqlErrorCode.PARSING, "Cannot apply [DATE, TIMESTAMP_WITH_TIME_ZONE]");
    }

    @Test
    public void test_column_parameter() {
        checkColumnParameter(0, 0, RES_EQ);
        checkColumnParameter(0, Integer.MAX_VALUE, RES_LT);
        checkColumnParameter(0, Integer.MIN_VALUE, RES_GT);

        checkColumnParameter(0, null, RES_NULL);

        checkColumnParameterFailure(1, new BigDecimal("1.1"), SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to INTEGER");
    }

    @Test
    public void test_column_literal() {
        checkColumnLiteral(1, "1", RES_EQ);

        checkColumnLiteral(1, "1.1", RES_LT);
        checkColumnLiteral(1, "0.9", RES_GT);

        checkColumnLiteral(1, "1.1E0", RES_LT);
        checkColumnLiteral(1, "0.9E0", RES_GT);

        checkColumnLiteral(1, "null", RES_NULL);

        checkColumnLiteralFailure(1, "true", SqlErrorCode.PARSING, "Cannot apply [INTEGER, BOOLEAN]");
        checkColumnLiteralFailure(1, "false", SqlErrorCode.PARSING, "Cannot apply [INTEGER, BOOLEAN]");
        checkColumnLiteralFailure(1, "'true'", SqlErrorCode.PARSING, "Cannot apply [INTEGER, VARCHAR]");
        checkColumnLiteralFailure(1, "'false'", SqlErrorCode.PARSING, "Cannot apply [INTEGER, VARCHAR]");
        checkColumnLiteralFailure(1, "'bad'", SqlErrorCode.PARSING, "Cannot apply [INTEGER, VARCHAR]");
    }

    @Test
    public void test_parameter_parameter() {
        put(1);

        checkFailure("?", "?", SqlErrorCode.PARSING, "Cannot apply [UNKNOWN, UNKNOWN]");
    }

    @Test
    public void test_parameter_literal() {
        put(1);

        checkFailure("?", "null", SqlErrorCode.PARSING, "Cannot apply [UNKNOWN, UNKNOWN]", new Object[] { null });

        // Exact numeric literal
        check("?", "1", RES_LT, (byte) 0);
        check("?", "1", RES_EQ, (byte) 1);
        check("?", "1", RES_GT, (byte) 2);

        check("?", "1", RES_LT, (short) 0);
        check("?", "1", RES_EQ, (short) 1);
        check("?", "1", RES_GT, (short) 2);

        check("?", "1", RES_LT, 0);
        check("?", "1", RES_EQ, 1);
        check("?", "1", RES_GT, 2);

        check("?", "1", RES_LT, 0L);
        check("?", "1", RES_EQ, 1L);
        check("?", "1", RES_GT, 2L);

        checkFailure("?", "1", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to BIGINT", new BigInteger("1"));
        checkFailure("?", "1", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to BIGINT", new BigDecimal("1"));

        // Inexact numeric literal
        check("?", "1E0", RES_NULL, new Object[] { null });

        check("?", "1E0", RES_LT, (byte) 0);
        check("?", "1E0", RES_EQ, (byte) 1);
        check("?", "1E0", RES_GT, (byte) 2);

        check("?", "1E0", RES_LT, (short) 0);
        check("?", "1E0", RES_EQ, (short) 1);
        check("?", "1E0", RES_GT, (short) 2);

        check("?", "1E0", RES_LT, 0);
        check("?", "1E0", RES_EQ, 1);
        check("?", "1E0", RES_GT, 2);

        check("?", "1E0", RES_LT, 0L);
        check("?", "1E0", RES_EQ, 1L);
        check("?", "1E0", RES_GT, 2L);

        check("?", "1E0", RES_LT, new BigInteger("0"));
        check("?", "1E0", RES_EQ, new BigInteger("1"));
        check("?", "1E0", RES_GT, new BigInteger("2"));

        check("?", "1E0", RES_LT, new BigDecimal("0"));
        check("?", "1E0", RES_EQ, new BigDecimal("1"));
        check("?", "1E0", RES_GT, new BigDecimal("2"));

        // String literal
        check("?", "'abc'", RES_NULL, new Object[] { null });

        check("?", "'abc'", RES_LT, "ab");
        check("?", "'abc'", RES_EQ, "abc");
        check("?", "'abc'", RES_GT, "abcd");

        checkFailure("?", "'abc'", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 1);
        checkFailure("?", "'1'", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 1);

        // Boolean literal
        check("?", "true", RES_NULL, new Object[] { null });

        check("?", "true", RES_LT, false);
        check("?", "true", RES_EQ, true);

        checkFailure("?", "true", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to BOOLEAN", 1);

        // Null literal
        checkFailure("?", "null", SqlErrorCode.PARSING, "Cannot apply [UNKNOWN, UNKNOWN]", 1);
    }

    @Test
    public void test_literal_literal() {
        put(1);

        check("1", "0", RES_GT);
        check("1", "1", RES_EQ);
        check("1", "2", RES_LT);
        check("1", "2E0", RES_LT);
        checkFailure("1", "'2'", SqlErrorCode.PARSING, "Cannot apply [TINYINT, VARCHAR]");
        checkFailure("1", "'bad'", SqlErrorCode.PARSING, "Cannot apply [TINYINT, VARCHAR]");
        checkFailure("1", "true", SqlErrorCode.PARSING, "Cannot apply [TINYINT, BOOLEAN]");
        check("1", "null", RES_NULL);

        check("1E0", "0E0", RES_GT);
        check("1E0", "1E0", RES_EQ);
        check("1E0", "2E0", RES_LT);
        checkFailure("1E0", "'2'", SqlErrorCode.PARSING, "Cannot apply [DOUBLE, VARCHAR]");
        checkFailure("1E0", "'bad'", SqlErrorCode.PARSING, "Cannot apply [DOUBLE, VARCHAR]");
        checkFailure("1E0", "true", SqlErrorCode.PARSING, "Cannot apply [DOUBLE, BOOLEAN]");
        check("1E0", "null", RES_NULL);

        check("'1'", "'2'", RES_LT);
        check("'abc'", "'def'", RES_LT);
        checkFailure("'abc'", "true", SqlErrorCode.PARSING, "Cannot apply [VARCHAR, BOOLEAN]");
        check("'abc'", "null", RES_NULL);

        check("true", "false", RES_GT);
        check("true", "null", RES_NULL);

        checkFailure("null", "null", SqlErrorCode.PARSING, "Cannot apply [UNKNOWN, UNKNOWN]");
    }

    private void checkColumnColumn(
        Class<? extends ExpressionBiValue> clazz,
        Comparable operand1,
        Comparable operand2,
        Integer expectedRes
    ) {
        put(createBiValue(clazz, operand1, operand2));

        check("field1", "field2", expectedRes);
    }

    private void checkColumnColumnFailure(
        Class<? extends ExpressionBiValue> clazz,
        Comparable operand1,
        Comparable operand2,
        int expectedErrorCode,
        String expectedErrorMessage
    ) {
        put(createBiValue(clazz, operand1, operand2));

        checkFailure("field1", "field2", expectedErrorCode, expectedErrorMessage);
    }

    private void checkColumnParameter(Object value, Object param, Integer expectedRes) {
        put(value);

        check("this", "?", expectedRes, param);
    }

    private void checkColumnParameterFailure(Object value, Object param, int expectedErrorCode, String expectedErrorMessage) {
        put(value);

        checkFailure("this", "?", expectedErrorCode, expectedErrorMessage, param);
    }

    private void checkColumnLiteral(Object value, String literal, Integer expectedRes) {
        put(value);

        check("this", literal, expectedRes);
    }

    private void checkColumnLiteralFailure(Object value, String literal, int expectedErrorCode, String expectedErrorMessage) {
        put(value);

        checkFailure("this", literal, expectedErrorCode, expectedErrorMessage);
    }

    private void checkFailure(
        String operand1,
        String operand2,
        int expectedErrorCode,
        String expectedErrorMessage,
        Object... params
    ) {
        for (String token : mode.tokens) {
            String sql = sql(token, operand1, operand2);

            checkFailure0(sql, expectedErrorCode, expectedErrorMessage, params);
        }
    }

    private String sql(String token, String operand1, String operand2) {
        return "SELECT " + operand1 + " " + token + " " + operand2 + " FROM map";
    }

    private static Integer inverse(Integer result) {
        if (result == null) {
            return null;
        }

        switch (result) {
            case RES_GT:
                return RES_LT;

            case RES_LT:
                return RES_GT;

            default:
                assert result == RES_EQ;

                return RES_EQ;
        }
    }

    public Boolean compare(Integer res) {
        if (res == null) {
            return null;
        }

        switch (mode) {
            case EQ:
                return res == 0;

            case NEQ:
                return res != 0;

            case LT:
                return res < 0;

            case LTE:
                return res <= 0;

            case GT:
                return res > 0;

            default:
                assert mode == Mode.GTE;

                return res >= 0;
        }
    }

    private enum Mode {
        EQ("="),
        NEQ("<>", "!="),
        LT("<"),
        LTE("<="),
        GT(">"),
        GTE(">=");

        private final String[] tokens;

        Mode(String... tokens) {
            this.tokens = tokens;
        }

        String token() {
            return tokens[0];
        }

        Mode inverse() {
            switch (this) {
                case LT:
                    return GT;

                case LTE:
                    return GTE;

                case GT:
                    return LT;

                case GTE:
                    return LTE;

                default:
                    return this;
            }
        }
    }

    private static class Literal {
        private final String value;
        private final SqlColumnType type;

        public Literal(String value, SqlColumnType type) {
            this.value = value;
            this.type = type;
        }
    }
}
