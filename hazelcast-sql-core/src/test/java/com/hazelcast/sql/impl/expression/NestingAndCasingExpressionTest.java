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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.sql.SqlOperator;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringJoiner;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.fail;

/**
 * Ensure that a nested function call derives the argument types as expected. This might not be the case if the
 * {@code HazelcastOperandTypeCheckerAware} doesn't work properly and the operand type information is not
 * resolved recursively.
 * <p>
 * Also ensures that function names are case-insensitive.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NestingAndCasingExpressionTest extends ExpressionTestSupport {
    @Override
    public void before0() {
        put(1);
    }

    /**
     * Check if there is a dedicated test for every expression.
     */
    @Test
    public void testAllExpressionsCovered() {
        Set<Field> checkedFields = new HashSet<>();
        Set<String> missingMethodNames = new LinkedHashSet<>();

        for (Field field : HazelcastSqlOperatorTable.class.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers()) || !Modifier.isPublic(field.getModifiers())) {
                continue;
            }

            if (!SqlOperator.class.isAssignableFrom(field.getType())
                || field.getName().equals("DESC")) {
                continue;
            }

            checkedFields.add(field);

            String testMethodName = "test_" + field.getName();

            try {
                NestingAndCasingExpressionTest.class.getMethod(testMethodName);
            } catch (NoSuchMethodException e) {
                missingMethodNames.add(testMethodName);
            }
        }

        assertFalse("No fields to check found!", checkedFields.isEmpty());

        if (!missingMethodNames.isEmpty()) {
            StringJoiner joiner = new StringJoiner("\n");

            missingMethodNames.forEach(joiner::add);

            fail("Tests not implemented: \n" + joiner.toString());
        }
    }

    @Test
    public void test_CAST() {
        check(sql("CAST(? AS INTEGER) || CAST(? AS INTEGER)"), 1, 1);
    }

    @Test
    public void test_AND() {
        check(sql("(? AND ?) || (? AND ?)"), true, true, true, true);
    }

    @Test
    public void test_OR() {
        check(sql("(? OR ?) || (? OR ?)"), true, true, true, true);
    }

    @Test
    public void test_NOT() {
        check(sql("(NOT(?)) || (NOT(?))"), true, true);
    }

    @Test
    public void test_EQUALS() {
        check(sql("(1=?) || (1=?)"), 1, 1);
    }

    @Test
    public void test_NOT_EQUALS() {
        check(sql("(1!=?) || (1!=?)"), 1, 1);
    }

    @Test
    public void test_GREATER_THAN() {
        check(sql("(1>?) || (1>?)"), 1, 1);
    }

    @Test
    public void test_GREATER_THAN_OR_EQUAL() {
        check(sql("(1>=?) || (1>=?)"), 1, 1);
    }

    @Test
    public void test_LESS_THAN() {
        check(sql("(1<?) || (1<?)"), 1, 1);
    }

    @Test
    public void test_LESS_THAN_OR_EQUAL() {
        check(sql("(1<=?) || (1<=?)"), 1, 1);
    }

    @Test
    public void test_PLUS() {
        check(sql("(1+?) || (1+?)"), 1, 1);
    }

    @Test
    public void test_MINUS() {
        check(sql("(1-?) || (1-?)"), 1, 1);
    }

    @Test
    public void test_MULTIPLY() {
        check(sql("(1*?) || (1*?)"), 1, 1);
    }

    @Test
    public void test_DIVIDE() {
        check(sql("(1/?) || (1/?)"), 1, 1);
    }

    @Test
    public void test_REMAINDER() {
        check(sql("(1%?) || (1%?)"), 1, 1);
    }

    @Test
    public void test_UNARY_PLUS() {
        check(sql("(+?) || (+?)"), 1, 1);
    }

    @Test
    public void test_UNARY_MINUS() {
        check(sql("(-?) || (-?)"), 1, 1);
    }

    @Test
    public void test_IS_TRUE() {
        check(sql("(? IS TRUE) || (? IS TRUE)"), false, false);
    }

    @Test
    public void test_IS_NOT_TRUE() {
        check(sql("(? IS NOT TRUE) || (? IS NOT TRUE)"), false, false);
    }

    @Test
    public void test_IS_FALSE() {
        check(sql("(? IS FALSE) || (? IS FALSE)"), false, false);
    }

    @Test
    public void test_IS_NOT_FALSE() {
        check(sql("(? IS NOT FALSE) || (? IS NOT FALSE)"), false, false);
    }

    @Test
    public void test_IS_NULL() {
        check(sql("(? IS NULL) || (? IS NULL)"), 1, 1);
    }

    @Test
    public void test_IS_NOT_NULL() {
        check(sql("(? IS NOT NULL) || (? IS NOT NULL)"), 1, 1);
    }

    @Test
    public void test_ABS() {
        check(sql("ABS(?) || ABS(?)"), 1, 1);
    }

    @Test
    public void test_SIGN() {
        check(sql("SIGN(?) || SIGN(?)"), 1, 1);
    }

    @Test
    public void test_RAND() {
        check(sql("RAND(?) || RAND(?)"), 1, 1);
    }

    @Test
    public void test_COS() {
        check(sql("COS(?) || COS(?)"), 1, 1);
    }

    @Test
    public void test_SIN() {
        check(sql("SIN(?) || SIN(?)"), 1, 1);
    }

    @Test
    public void test_TAN() {
        check(sql("TAN(?) || TAN(?)"), 1, 1);
    }

    @Test
    public void test_COT() {
        check(sql("COT(?) || COT(?)"), 1, 1);
    }

    @Test
    public void test_ACOS() {
        check(sql("ACOS(?) || ACOS(?)"), 1, 1);
    }

    @Test
    public void test_ASIN() {
        check(sql("ASIN(?) || ASIN(?)"), 1, 1);
    }

    @Test
    public void test_ATAN() {
        check(sql("ATAN(?) || ATAN(?)"), 1, 1);
    }

    @Test
    public void test_EXP() {
        check(sql("EXP(?) || EXP(?)"), 1, 1);
    }

    @Test
    public void test_LN() {
        check(sql("LN(?) || LN(?)"), 1, 1);
    }

    @Test
    public void test_LOG10() {
        check(sql("LOG10(?) || LOG10(?)"), 1, 1);
    }

    @Test
    public void test_DEGREES() {
        check(sql("DEGREES(?) || DEGREES(?)"), 1, 1);
    }

    @Test
    public void test_RADIANS() {
        check(sql("RADIANS(?) || RADIANS(?)"), 1, 1);
    }

    @Test
    public void test_FLOOR() {
        check(sql("FLOOR(?) || FLOOR(?)"), 1, 1);
    }

    @Test
    public void test_CEIL() {
        check(sql("CEIL(?) || CEIL(?)"), 1, 1);
    }

    @Test
    public void test_ROUND() {
        check(sql("ROUND(?) || ROUND(?)"), 1, 1);
    }

    @Test
    public void test_TRUNCATE() {
        check(sql("TRUNCATE(?) || TRUNCATE(?)"), 1, 1);
    }

    @Test
    public void test_CONCAT() {
        check(sql("(? || ?) || (? || ?)"), "a", "b", "c", "d");
    }

    @Test
    public void test_LIKE() {
        check(sql("(? LIKE ?) || (? LIKE ?)"), "a", "a", "b", "b");
    }

    @Test
    public void test_NOT_LIKE() {
        check(sql("(? NOT LIKE ?) || (? NOT LIKE ?)"), "a", "a", "b", "b");
    }

    @Test
    public void test_SUBSTRING() {
        check(sql("SUBSTRING(? FROM ?) || SUBSTRING(? FROM ?) "), "a", 1, "b", 1);
    }

    @Test
    public void test_TRIM() {
        check(sql("TRIM(LEADING ? FROM ?) || RTRIM(?)"), "1", "1", "2");
    }

    @Test
    public void test_RTRIM() {
        check(sql("RTRIM(?) || RTRIM(?)"), "1", "1");
    }

    @Test
    public void test_LTRIM() {
        check(sql("LTRIM(?) || LTRIM(?)"), "1", "1");
    }

    @Test
    public void test_BTRIM() {
        check(sql("BTRIM(?) || BTRIM(?)"), "1", "1");
    }

    @Test
    public void test_ASCII() {
        check(sql("ASCII(?) || ASCII(?)"), "1", "1");
    }

    @Test
    public void test_INITCAP() {
        check(sql("INITCAP(?) || INITCAP(?)"), "1", "1");
    }

    @Test
    public void test_CHAR_LENGTH() {
        check(sql("CHAR_LENGTH(?) || CHAR_LENGTH(?)"), "1", "1");
    }

    @Test
    public void test_CHARACTER_LENGTH() {
        check(sql("CHARACTER_LENGTH(?) || CHARACTER_LENGTH(?)"), "1", "1");
    }

    @Test
    public void test_LENGTH() {
        check(sql("LENGTH(?) || LENGTH(?)"), "1", "1");
    }

    @Test
    public void test_LOWER() {
        check(sql("LOWER(?) || LOWER(?)"), "1", "2");
    }

    @Test
    public void test_UPPER() {
        check(sql("UPPER(?) || UPPER(?)"), "1", "2");
    }

    private void check(String sql, Object... params) {
        checkValue0(sql, SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, params);
        checkValue0(sql.toLowerCase(), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, params);
    }

    private String sql(String expression) {
        return "SELECT " + expression + " FROM map";
    }
}
