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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.SqlException;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.expression.SqlExpressionIntegrationTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LikeFunctionIntegrationTest extends SqlExpressionIntegrationTestSupport {
    @Test
    public void test_wildcards() {
        put("abcde");

        check("this LIKE 'abcd'", false);
        check("this LIKE 'bcde'", false);
        check("this LIKE 'bcd'", false);

        check("this LIKE 'abcde'", true);
        check("this LIKE 'abcdef'", false);
        check("this LIKE 'abcd'", false);
        check("this LIKE 'bcde'", false);

        check("this LIKE '_bcde'", true);
        check("this LIKE 'abcd_'", true);
        check("this LIKE 'ab_de'", true);
        check("this LIKE '_b_d_'", true);
        check("this LIKE 'abcde_'", false);
        check("this LIKE '_abcde'", false);
        check("this LIKE '_abcde_'", false);
        check("this LIKE '_ab_de_'", false);
        check("this LIKE '_____'", true);
        check("this LIKE '______'", false);

        check("this LIKE '%bcde'", true);
        check("this LIKE '%cde'", true);
        check("this LIKE 'abcd%'", true);
        check("this LIKE 'abc%'", true);
        check("this LIKE 'ab%de'", true);
        check("this LIKE 'a%e'", true);
        check("this LIKE '%b%d%'", true);
        check("this LIKE '%c%'", true);
        check("this LIKE 'abcde%'", true);
        check("this LIKE '%abcde'", true);
        check("this LIKE '%abcde%'", true);
        check("this LIKE '%ab%de%'", true);
        check("this LIKE '%'", true);

        check("this LIKE '_bcde%'", true);
        check("this LIKE '_bcd%'", true);
        check("this LIKE '%b_d%'", true);
        check("this LIKE '%b__d%'", false);
        check("this LIKE '____%'", true);
        check("this LIKE '_____%'", true);
        check("this LIKE '______%'", false);
    }

    @Test
    public void test_escape() {
        // Normal escape
        put("te_t");
        check("this LIKE 'te!_t' ESCAPE '!'", true);
        check("this LIKE 'te!_t' ESCAPE null", null);

        put("te%t");
        check("this LIKE 'te!%t' ESCAPE '!'", true);

        // Escape is not a single character
        put("te_t");
        checkFailure("this LIKE 'te\\_t' ESCAPE ''", SqlErrorCode.GENERIC, "ESCAPE parameter must be a single character");
        checkFailure("this LIKE 'te\\_t' ESCAPE '!!'", SqlErrorCode.GENERIC, "ESCAPE parameter must be a single character");

        // Apply escape to incorrect symbols in the pattern
        checkFailure("this LIKE 'te_!t' ESCAPE '!'", SqlErrorCode.GENERIC, "Only '_', '%' and the escape character can be escaped");
        checkFailure("this LIKE 'te_t!' ESCAPE '!'", SqlErrorCode.GENERIC, "Only '_', '%' and the escape character can be escaped");
    }

    @Test
    public void test_parameter() {
        // First parameter
        put("te_t");
        check("? LIKE this", true, "test");
        check("? LIKE this", null, new Object[] { null });

        // Second parameter
        put("test");
        check("this LIKE ?", true, "te_t");
        check("this LIKE ?", null, new Object[] { null });

        // Both parameters
        put("foo");
        check("? LIKE ?", true, "test", "te_t");

        // Escape
        put("te_t");
        check("? LIKE ? ESCAPE ?", true, "te_t", "te\\__", "\\");
        check("? LIKE ? ESCAPE ?", false, "te_t", "te\\_", "\\");
    }

    @Test
    public void test_literals() {
        put("abcde");

        check("20 LIKE 2", false);
        check("20 LIKE '2'", false);
        check("'20' LIKE 2", false);

        check("'20' LIKE 20", true);
        check("20 LIKE '20'", true);
        check("'20' LIKE '20'", true);

        check("20 LIKE '2_'", true);

        check("null LIKE '2_'", null);
        check("20 LIKE null", null);
    }

    @Test
    public void test_newline() {
        put("\n");
        check("this LIKE '_'", true);
        check("this LIKE '%'", true);

        put("\n\n");
        check("this LIKE '_'", false);
        check("this LIKE '__'", true);
        check("this LIKE '%'", true);
    }

    @Test
    public void test_special_char_escaping() {
        put("[({|^+*?-$\\.abc})]");
        check("this LIKE '[({|^+*?-$\\.___})]'", true);
        check("this LIKE '[({|^+*?-$\\.%})]'", true);
    }

    private void check(String operands, Boolean expectedResult, Object... params) {
        String sql = "SELECT " + operands + " FROM map";

        List<SqlRow> rows = execute(member, sql, params);
        assertEquals(1, rows.size());

        SqlRow row = rows.get(0);
        assertEquals(1, row.getMetadata().getColumnCount());
        assertEquals(SqlColumnType.BOOLEAN, row.getMetadata().getColumn(0).getType());
        assertEquals(expectedResult, row.getObject(0));
    }

    private void checkFailure(String operands, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT " + operands + " FROM map";

        try {
            execute(member, sql, params);

            fail("Must fail");
        } catch (SqlException e) {
            assertEquals(expectedErrorCode + ": " + e.getMessage(), expectedErrorCode, e.getCode());

            assertFalse(expectedErrorMessage.isEmpty());
            assertTrue(e.getMessage(), e.getMessage().contains(expectedErrorMessage));
        }
    }
}
