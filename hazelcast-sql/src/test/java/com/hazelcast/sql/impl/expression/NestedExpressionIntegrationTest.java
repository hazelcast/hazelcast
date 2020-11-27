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
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastOperandTypeCheckerAware;
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
 * {@link HazelcastOperandTypeCheckerAware} doesn't work  properly, and the operand type information is lost
 * when transitioning between AST and relational trees.
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
    public void test_TRIM() {
        checkValue0(sql("TRIM(LEADING ? FROM ?) || RTRIM(?)"), SqlColumnType.VARCHAR, "2", "1", "1", "2");
    }

    @Test
    public void test_RTRIM() {
        checkValue0(sql("RTRIM(?) || RTRIM(?)"), SqlColumnType.VARCHAR, "11", "1", "1");
    }

    @Test
    public void test_LTRIM() {
        checkValue0(sql("LTRIM(?) || LTRIM(?)"), SqlColumnType.VARCHAR, "11", "1", "1");
    }

    @Test
    public void test_BTRIM() {
        checkValue0(sql("BTRIM(?) || BTRIM(?)"), SqlColumnType.VARCHAR, "11", "1", "1");
    }

    @Test
    public void test_ASCII() {
        checkValue0(sql("ASCII(?) || ASCII(?)"), SqlColumnType.VARCHAR, "4949", "1", "1");
    }

    @Test
    public void test_INITCAP() {
        checkValue0(sql("INITCAP(?) || INITCAP(?)"), SqlColumnType.VARCHAR, "11", "1", "1");
    }

    @Test
    public void test_CHAR_LENGTH() {
        checkValue0(sql("CHAR_LENGTH(?) || CHAR_LENGTH(?)"), SqlColumnType.VARCHAR, "11", "1", "1");
    }

    @Test
    public void test_CHARACTER_LENGTH() {
        checkValue0(sql("CHARACTER_LENGTH(?) || CHARACTER_LENGTH(?)"), SqlColumnType.VARCHAR, "11", "1", "1");
    }

    @Test
    public void test_LENGTH() {
        checkValue0(sql("LENGTH(?) || LENGTH(?)"), SqlColumnType.VARCHAR, "11", "1", "1");
    }

    @Test
    public void test_LOWER() {
        checkValue0(sql("LOWER(?) || LOWER(?)"), SqlColumnType.VARCHAR, "12", "1", "2");
    }

    @Test
    public void test_UPPER() {
        checkValue0(sql("UPPER(?) || UPPER(?)"), SqlColumnType.VARCHAR, "12", "1", "2");
    }

    private String sql(String expression) {
        return "SELECT " + expression + " FROM map";
    }
}
