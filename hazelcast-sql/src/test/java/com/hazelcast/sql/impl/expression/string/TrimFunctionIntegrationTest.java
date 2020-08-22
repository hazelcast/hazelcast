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
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.SqlExpressionIntegrationTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TrimFunctionIntegrationTest extends SqlExpressionIntegrationTestSupport {
    /**
     * Tests for the core TRIM logic. Operates on literals where possible for simplicity
     */
    @Test
    public void testLogic() {
        put(1);

        // NULL input
        // TODO:
        checkValueInternal("SELECT TRIM(LEADING 'a' FROM null) FROM map", SqlColumnType.NULL, null);
        checkValueInternal("SELECT TRIM(TRAILING 'a' FROM null) FROM map", SqlColumnType.NULL, null);
        checkValueInternal("SELECT TRIM(BOTH 'a' FROM null) FROM map", SqlColumnType.NULL, null);

        checkValueInternal("SELECT TRIM(LEADING null FROM 'aabbccddee') FROM map", SqlColumnType.VARCHAR, null);
        checkValueInternal("SELECT TRIM(LEADING 'a' FROM 'aabbccddee') FROM map", SqlColumnType.VARCHAR, "bbccddee");

        // TODO
//        checkValueInternal("SELECT TRIM(TRAILING 'abc' FROM this) FROM map", SqlColumnType.VARCHAR, "abcde");
//        checkValueInternal("SELECT TRIM(BOTH 'abc' FROM this) FROM map", SqlColumnType.VARCHAR, "abcde");
//
//        checkValueInternal("SELECT TRIM(LEADING FROM this) FROM map", SqlColumnType.VARCHAR, "abcde");
//        checkValueInternal("SELECT TRIM(TRAILING FROM this) FROM map", SqlColumnType.VARCHAR, "abcde");
//        checkValueInternal("SELECT TRIM(BOTH FROM this) FROM map", SqlColumnType.VARCHAR, "abcde");
//
//        checkValueInternal("SELECT TRIM(this) FROM map", SqlColumnType.VARCHAR, "abcde");
//
//        checkValueInternal("SELECT LTRIM(this) FROM map", SqlColumnType.VARCHAR, "abcde");
//        checkValueInternal("SELECT RTRIM(this) FROM map", SqlColumnType.VARCHAR, "abcde");
//        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "abcde");
    }

    @Test
    public void test_input() {
        // TODO
    }

    @Test
    public void test_characters() {

    }

    @Test
    public void testSimple_trim() {
        // TODO
    }

    @Test
    public void testSimple_ltrim() {
        // Columns
        put("abc");
        checkValueInternal("SELECT LTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put(" abc");
        checkValueInternal("SELECT LTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("  abc");
        checkValueInternal("SELECT LTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("abc ");
        checkValueInternal("SELECT LTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc ");

        put("abc  ");
        checkValueInternal("SELECT LTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc  ");

        put(" _ abc _ ");
        checkValueInternal("SELECT LTRIM(this) FROM map", SqlColumnType.VARCHAR, "_ abc _ ");

        put(new ExpressionValue.StringVal());
        checkValueInternal("SELECT LTRIM(field1) FROM map", SqlColumnType.VARCHAR, null);

        // Only one test for numeric column because we are going to restrict them anyway
        put(BigDecimal.ONE);
        checkValueInternal("SELECT LTRIM(this) FROM map", SqlColumnType.VARCHAR, "1");

        // Parameters
        put(1);
        checkValueInternal("SELECT LTRIM(?) FROM map", SqlColumnType.VARCHAR, null, new Object[] { null });
        checkValueInternal("SELECT LTRIM(?) FROM map", SqlColumnType.VARCHAR, "abc ", " abc ");
        checkValueInternal("SELECT LTRIM(?) FROM map", SqlColumnType.VARCHAR, "a", 'a');

        // Only one test for numeric parameter because we are going to restrict them anyway
        checkFailureInternal("SELECT LTRIM(?) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 1);

        // Literals
        checkValueInternal("SELECT LTRIM(' abc ') FROM map", SqlColumnType.VARCHAR, "abc ");
        checkValueInternal("SELECT LTRIM(null) FROM map", SqlColumnType.VARCHAR, null);
        checkValueInternal("SELECT LTRIM(true) FROM map", SqlColumnType.VARCHAR, "true");
        checkValueInternal("SELECT LTRIM(1) FROM map", SqlColumnType.VARCHAR, "1");
        checkValueInternal("SELECT LTRIM(1.1) FROM map", SqlColumnType.VARCHAR, "1.1");
        checkValueInternal("SELECT LTRIM(1.1E2) FROM map", SqlColumnType.VARCHAR, "110.0");
    }

    @Test
    public void testSimple_rtrim() {
        // Columns
        put("abc");
        checkValueInternal("SELECT RTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put(" abc");
        checkValueInternal("SELECT RTRIM(this) FROM map", SqlColumnType.VARCHAR, " abc");

        put("  abc");
        checkValueInternal("SELECT RTRIM(this) FROM map", SqlColumnType.VARCHAR, "  abc");

        put("abc ");
        checkValueInternal("SELECT RTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("abc  ");
        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put(" _ abc _ ");
        checkValueInternal("SELECT RTRIM(this) FROM map", SqlColumnType.VARCHAR, " _ abc _");

        put(new ExpressionValue.StringVal());
        checkValueInternal("SELECT RTRIM(field1) FROM map", SqlColumnType.VARCHAR, null);

        // Only one test for numeric column because we are going to restrict them anyway
        put(BigDecimal.ONE);
        checkValueInternal("SELECT RTRIM(this) FROM map", SqlColumnType.VARCHAR, "1");

        // Parameters
        put(1);
        checkValueInternal("SELECT RTRIM(?) FROM map", SqlColumnType.VARCHAR, null, new Object[] { null });
        checkValueInternal("SELECT RTRIM(?) FROM map", SqlColumnType.VARCHAR, " abc", " abc ");
        checkValueInternal("SELECT RTRIM(?) FROM map", SqlColumnType.VARCHAR, "a", 'a');

        // Only one test for numeric parameter because we are going to restrict them anyway
        checkFailureInternal("SELECT RTRIM(?) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 1);

        // Literals
        checkValueInternal("SELECT RTRIM(' abc ') FROM map", SqlColumnType.VARCHAR, " abc");
        checkValueInternal("SELECT RTRIM(null) FROM map", SqlColumnType.VARCHAR, null);
        checkValueInternal("SELECT RTRIM(true) FROM map", SqlColumnType.VARCHAR, "true");
        checkValueInternal("SELECT RTRIM(1) FROM map", SqlColumnType.VARCHAR, "1");
        checkValueInternal("SELECT RTRIM(1.1) FROM map", SqlColumnType.VARCHAR, "1.1");
        checkValueInternal("SELECT RTRIM(1.1E2) FROM map", SqlColumnType.VARCHAR, "110.0");
    }

    @Test
    public void testSimple_btrim() {
        // Columns
        put("abc");
        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put(" abc");
        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("  abc");
        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("abc ");
        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("abc  ");
        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put(" _ abc _ ");
        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "_ abc _");

        put(new ExpressionValue.StringVal());
        checkValueInternal("SELECT BTRIM(field1) FROM map", SqlColumnType.VARCHAR, null);

        // Only one test for numeric column because we are going to restrict them anyway
        put(BigDecimal.ONE);
        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "1");

        // Parameters
        put(1);
        checkValueInternal("SELECT BTRIM(?) FROM map", SqlColumnType.VARCHAR, null, new Object[] { null });
        checkValueInternal("SELECT BTRIM(?) FROM map", SqlColumnType.VARCHAR, "abc", " abc ");
        checkValueInternal("SELECT BTRIM(?) FROM map", SqlColumnType.VARCHAR, "a", 'a');

        // Only one test for numeric parameter because we are going to restrict them anyway
        checkFailureInternal("SELECT BTRIM(?) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 1);

        // Literals
        checkValueInternal("SELECT BTRIM(' abc ') FROM map", SqlColumnType.VARCHAR, "abc");
        checkValueInternal("SELECT BTRIM(null) FROM map", SqlColumnType.VARCHAR, null);
        checkValueInternal("SELECT BTRIM(true) FROM map", SqlColumnType.VARCHAR, "true");
        checkValueInternal("SELECT BTRIM(1) FROM map", SqlColumnType.VARCHAR, "1");
        checkValueInternal("SELECT BTRIM(1.1) FROM map", SqlColumnType.VARCHAR, "1.1");
        checkValueInternal("SELECT BTRIM(1.1E2) FROM map", SqlColumnType.VARCHAR, "110.0");
    }
}
