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

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.string.LikeFunction;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.hazelcast.sql.SqlColumnType.TINYINT;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LikeFunctionIntegrationTest extends ExpressionTestSupport {

    private static final ConstantExpression<?> CONST_1 = ConstantExpression.create("1", QueryDataType.VARCHAR);
    private static final ConstantExpression<?> CONST_2 = ConstantExpression.create("2", QueryDataType.VARCHAR);
    private static final ConstantExpression<?> CONST_3 = ConstantExpression.create("3", QueryDataType.VARCHAR);
    private static final ConstantExpression<?> CONST_OTHER = ConstantExpression.create("100", QueryDataType.VARCHAR);

    @Parameterized.Parameter
    public boolean negated;

    @Parameterized.Parameters(name = "mode:{0}")
    public static List<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @Test
    public void test_wildcards() {
        put("abcde");

        check(sql("this", "'abcd'"), false);
        check(sql("this", "'bcde'"), false);
        check(sql("this", "'bcd'"), false);

        check(sql("this", "'abcde'"), true);
        check(sql("this", "'abcdef'"), false);
        check(sql("this", "'abcd'"), false);
        check(sql("this", "'bcde'"), false);

        check(sql("this", "'_bcde'"), true);
        check(sql("this", "'abcd_'"), true);
        check(sql("this", "'ab_de'"), true);
        check(sql("this", "'_b_d_'"), true);
        check(sql("this", "'abcde_'"), false);
        check(sql("this", "'_abcde'"), false);
        check(sql("this", "'_abcde_'"), false);
        check(sql("this", "'_ab_de_'"), false);
        check(sql("this", "'_____'"), true);
        check(sql("this", "'______'"), false);

        check(sql("this", "'%bcde'"), true);
        check(sql("this", "'%cde'"), true);
        check(sql("this", "'abcd%'"), true);
        check(sql("this", "'abc%'"), true);
        check(sql("this", "'ab%de'"), true);
        check(sql("this", "'a%e'"), true);
        check(sql("this", "'%b%d%'"), true);
        check(sql("this", "'%c%'"), true);
        check(sql("this", "'abcde%'"), true);
        check(sql("this", "'%abcde'"), true);
        check(sql("this", "'%abcde%'"), true);
        check(sql("this", "'%ab%de%'"), true);
        check(sql("this", "'%'"), true);

        check(sql("this", "'_bcde%'"), true);
        check(sql("this", "'_bcd%'"), true);
        check(sql("this", "'%b_d%'"), true);
        check(sql("this", "'%b__d%'"), false);
        check(sql("this", "'____%'"), true);
        check(sql("this", "'_____%'"), true);
        check(sql("this", "'______%'"), false);
    }

    @Test
    public void test_escape() {
        // Normal escape
        put("te_t");
        check(sql("this", "'te!_t'", "'!'"), true);
        check(sql("this", "'te!_t'", "null"), null);

        put("te%t");
        check(sql("this", "'te!%t'", "'!'"), true);

        // Escape is not a single character
        put("te_t");
        checkFailure(sql("this", "'te\\_t'", "''"), SqlErrorCode.GENERIC, "ESCAPE parameter must be a single character");
        checkFailure(sql("this", "'te\\_t'", "'!!'"), SqlErrorCode.GENERIC, "ESCAPE parameter must be a single character");

        // Apply escape to incorrect symbols in the pattern
        checkFailure(sql("this", "'te_!t'", "'!'"), SqlErrorCode.GENERIC, "Only '_', '%' and the escape character can be escaped");
        checkFailure(sql("this", "'te_t!'", "'!'"), SqlErrorCode.GENERIC, "Only '_', '%' and the escape character can be escaped");
    }

    @Test
    public void test_parameter() {
        // First parameter
        put("te_t");
        check(sql("?", "this"), true, "test");
        check(sql("?", "this"), null, new Object[]{null});

        // Second parameter
        put("test");
        check(sql("this", "?"), true, "te_t");
        check(sql("this", "?"), null, new Object[]{null});

        // Both parameters
        put("foo");
        check(sql("?", "?"), true, "test", "te_t");

        // Escape
        put("te_t");
        check(sql("?", "?", "?"), true, "te_t", "te\\__", "\\");
        check(sql("?", "?", "?"), false, "te_t", "te\\_", "\\");
    }

    @Test
    public void test_literals() {
        put("abcde");

        checkFailure(sql("20", "2"), SqlErrorCode.PARSING, signatureErrorOperator(name(), TINYINT, TINYINT));
        checkFailure(sql("20", "'2'"), SqlErrorCode.PARSING, signatureErrorOperator(name(), TINYINT, VARCHAR));
        checkFailure(sql("'20'", "2"), SqlErrorCode.PARSING, signatureErrorOperator(name(), VARCHAR, TINYINT));

        check(sql("'20'", "'2_'"), true);
        check(sql("null", "'2_'"), null);
        check(sql("'20'", "null"), null);
    }

    @Test
    public void test_newline() {
        put("\n");
        check(sql("this", "'_'"), true);
        check(sql("this", "'%'"), true);

        put("\n\n");
        check(sql("this", "'_'"), false);
        check(sql("this", "'__'"), true);
        check(sql("this", "'%'"), true);
    }

    @Test
    public void test_special_char_escaping() {
        put("[({|^+*?-$\\.abc})]");
        check(sql("this", "'[({|^+*?-$\\.___})]'"), true);
        check(sql("this", "'[({|^+*?-$\\.%})]'"), true);
    }

    private void check(String sql, Boolean expectedResult, Object... params) {
        if (negated && expectedResult != null) {
            expectedResult = !expectedResult;
        }

        List<SqlRow> rows = execute(sql, params);
        assertEquals(1, rows.size());

        SqlRow row = rows.get(0);
        assertEquals(1, row.getMetadata().getColumnCount());
        assertEquals(SqlColumnType.BOOLEAN, row.getMetadata().getColumn(0).getType());
        assertEquals(expectedResult, row.getObject(0));
    }

    private void checkFailure(String sql, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        try {
            execute(sql, params);

            fail("Must fail");
        } catch (HazelcastSqlException e) {
            assertEquals(expectedErrorCode + ": " + e.getMessage(), expectedErrorCode, e.getCode());

            assertFalse(expectedErrorMessage.isEmpty());
            assertTrue(e.getMessage(), e.getMessage().contains(expectedErrorMessage));
        }
    }

    private String sql(Object operand1, Object operand2) {
        return "SELECT " + operand1 + " " + name() + " " + operand2 + " FROM map";
    }

    private String sql(Object operand1, Object operand2, Object operand3) {
        return "SELECT " + operand1 + " " + name() + " " + operand2 + " ESCAPE " + operand3 + " FROM map";
    }

    private String name() {
        return negated ? "NOT LIKE" : "LIKE";
    }

    @Test
    public void testEquals() {
        LikeFunction function = LikeFunction.create(CONST_1, CONST_2, CONST_3, negated);

        checkEquals(function, LikeFunction.create(CONST_1, CONST_2, CONST_3, negated), true);
        checkEquals(function, LikeFunction.create(CONST_OTHER, CONST_2, CONST_3, negated), false);
        checkEquals(function, LikeFunction.create(CONST_1, CONST_OTHER, CONST_3, negated), false);
        checkEquals(function, LikeFunction.create(CONST_1, CONST_2, CONST_OTHER, negated), false);
        checkEquals(function, LikeFunction.create(CONST_1, CONST_2, CONST_3, !negated), false);
    }

    @Test
    public void testSerialization() {
        LikeFunction original = LikeFunction.create(CONST_1, CONST_2, CONST_3, negated);
        LikeFunction restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_LIKE);

        checkEquals(original, restored, true);
    }
}
