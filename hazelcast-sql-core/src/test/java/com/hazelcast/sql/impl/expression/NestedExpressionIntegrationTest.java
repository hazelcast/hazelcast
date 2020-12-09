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
 * Ensure that nested function call derive argument types as expected. This might not be the case if the
 * {@code HazelcastOperandTypeCheckerAware} doesn't work  properly, and the operand type information is not
 * resolved recursively.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NestedExpressionIntegrationTest extends ExpressionTestSupport {
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

            if (!SqlOperator.class.isAssignableFrom(field.getType())) {
                continue;
            }

            checkedFields.add(field);

            String testMethodName = "test_" + field.getName();

            try {
                NestedExpressionIntegrationTest.class.getMethod(testMethodName);
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
        checkValue0(sql("CAST(? AS INTEGER) || CAST(? AS INTEGER)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_AND() {
        checkValue0(sql("(? AND ?) || (? AND ?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, true, true, true, true);
    }

    @Test
    public void test_OR() {
        checkValue0(sql("(? OR ?) || (? OR ?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, true, true, true, true);
    }

    @Test
    public void test_NOT() {
        checkValue0(sql("(NOT(?)) || (NOT(?))"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, true, true);
    }

    @Test
    public void test_EQUALS() {
        checkValue0(sql("(1=?) || (1=?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_NOT_EQUALS() {
        checkValue0(sql("(1!=?) || (1!=?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_GREATER_THAN() {
        checkValue0(sql("(1>?) || (1>?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_GREATER_THAN_OR_EQUAL() {
        checkValue0(sql("(1>=?) || (1>=?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_LESS_THAN() {
        checkValue0(sql("(1<?) || (1<?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_LESS_THAN_OR_EQUAL() {
        checkValue0(sql("(1<=?) || (1<=?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_PLUS() {
        checkValue0(sql("(1+?) || (1+?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_MINUS() {
        checkValue0(sql("(1-?) || (1-?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_MULTIPLY() {
        checkValue0(sql("(1*?) || (1*?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_DIVIDE() {
        checkValue0(sql("(1/?) || (1/?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_UNARY_PLUS() {
        checkValue0(sql("(+?) || (+?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_UNARY_MINUS() {
        checkValue0(sql("(-?) || (-?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_IS_TRUE() {
        checkValue0(sql("(? IS TRUE) || (? IS TRUE)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, false, false);
    }

    @Test
    public void test_IS_NOT_TRUE() {
        checkValue0(sql("(? IS NOT TRUE) || (? IS NOT TRUE)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, false, false);
    }

    @Test
    public void test_IS_FALSE() {
        checkValue0(sql("(? IS FALSE) || (? IS FALSE)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, false, false);
    }

    @Test
    public void test_IS_NOT_FALSE() {
        checkValue0(sql("(? IS NOT FALSE) || (? IS NOT FALSE)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, false, false);
    }

    @Test
    public void test_IS_NULL() {
        checkValue0(sql("(? IS NULL) || (? IS NULL)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_IS_NOT_NULL() {
        checkValue0(sql("(? IS NOT NULL) || (? IS NOT NULL)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_ABS() {
        checkValue0(sql("ABS(?) || ABS(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_SIGN() {
        checkValue0(sql("SIGN(?) || SIGN(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_RAND() {
        checkValue0(sql("RAND(?) || RAND(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_COS() {
        checkValue0(sql("COS(?) || COS(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_SIN() {
        checkValue0(sql("SIN(?) || SIN(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_TAN() {
        checkValue0(sql("TAN(?) || TAN(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_COT() {
        checkValue0(sql("COT(?) || COT(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_ACOS() {
        checkValue0(sql("ACOS(?) || ACOS(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_ASIN() {
        checkValue0(sql("ASIN(?) || ASIN(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_ATAN() {
        checkValue0(sql("ATAN(?) || ATAN(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_EXP() {
        checkValue0(sql("EXP(?) || EXP(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_LN() {
        checkValue0(sql("LN(?) || LN(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_LOG10() {
        checkValue0(sql("LOG10(?) || LOG10(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_DEGREES() {
        checkValue0(sql("DEGREES(?) || DEGREES(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_RADIANS() {
        checkValue0(sql("RADIANS(?) || RADIANS(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_FLOOR() {
        checkValue0(sql("FLOOR(?) || FLOOR(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_CEIL() {
        checkValue0(sql("CEIL(?) || CEIL(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }
    @Test
    public void test_ROUND() {
        checkValue0(sql("ROUND(?) || ROUND(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_TRUNCATE() {
        checkValue0(sql("TRUNCATE(?) || TRUNCATE(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, 1, 1);
    }

    @Test
    public void test_CONCAT() {
        checkValue0(sql("(? || ?) || (? || ?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, "a", "b", "c", "d");
    }

    @Test
    public void test_LIKE() {
        checkValue0(sql("(? LIKE ?) || (? LIKE ?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, "a", "a", "b", "b");
    }

    @Test
    public void test_SUBSTRING() {
        checkValue0(sql("SUBSTRING(? FROM ?) || SUBSTRING(? FROM ?) "), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, "a", 1, "b", 1);
    }

    @Test
    public void test_TRIM() {
        checkValue0(sql("TRIM(LEADING ? FROM ?) || RTRIM(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, "1", "1", "2");
    }

    @Test
    public void test_RTRIM() {
        checkValue0(sql("RTRIM(?) || RTRIM(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, "1", "1");
    }

    @Test
    public void test_LTRIM() {
        checkValue0(sql("LTRIM(?) || LTRIM(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, "1", "1");
    }

    @Test
    public void test_BTRIM() {
        checkValue0(sql("BTRIM(?) || BTRIM(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, "1", "1");
    }

    @Test
    public void test_ASCII() {
        checkValue0(sql("ASCII(?) || ASCII(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, "1", "1");
    }

    @Test
    public void test_INITCAP() {
        checkValue0(sql("INITCAP(?) || INITCAP(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, "1", "1");
    }

    @Test
    public void test_CHAR_LENGTH() {
        checkValue0(sql("CHAR_LENGTH(?) || CHAR_LENGTH(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, "1", "1");
    }

    @Test
    public void test_CHARACTER_LENGTH() {
        checkValue0(sql("CHARACTER_LENGTH(?) || CHARACTER_LENGTH(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, "1", "1");
    }

    @Test
    public void test_LENGTH() {
        checkValue0(sql("LENGTH(?) || LENGTH(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, "1", "1");
    }

    @Test
    public void test_LOWER() {
        checkValue0(sql("LOWER(?) || LOWER(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, "1", "2");
    }

    @Test
    public void test_UPPER() {
        checkValue0(sql("UPPER(?) || UPPER(?)"), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, "1", "2");
    }

    private String sql(String expression) {
        return "SELECT " + expression + " FROM map";
    }
}
