/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql;

import com.hazelcast.sql.impl.CoreSqlTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlStatementTest extends CoreSqlTestSupport {

    private static final String SQL = "sql";

    @Test
    public void testDefaults() {
        SqlStatement query = create();

        assertEquals(SQL, query.getSql());
        assertEquals(0, query.getParameters().size());
        assertEquals(SqlStatement.DEFAULT_TIMEOUT, query.getTimeoutMillis());
        assertEquals(SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE, query.getCursorBufferSize());
        assertNull(query.getSchema());
    }

    @SuppressWarnings("ConstantConditions")
    @Test(expected = NullPointerException.class)
    public void testSql_null() {
        new SqlStatement(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSql_empty() {
        new SqlStatement("");
    }

    @Test
    public void testParameters() {
        Object param0 = new Object();
        Object param1 = new Object();
        Object param2 = new Object();

        SqlStatement query = create();
        assertEquals(0, query.getParameters().size());

        query.setParameters(Arrays.asList(param0, param1));
        assertEquals(2, query.getParameters().size());
        assertEquals(param0, query.getParameters().get(0));
        assertEquals(param1, query.getParameters().get(1));

        query.addParameter(param2);
        assertEquals(3, query.getParameters().size());
        assertEquals(param0, query.getParameters().get(0));
        assertEquals(param1, query.getParameters().get(1));
        assertEquals(param2, query.getParameters().get(2));

        query.clearParameters();
        assertEquals(0, query.getParameters().size());

        query.addParameter(param0);
        assertEquals(1, query.getParameters().size());
        assertEquals(param0, query.getParameters().get(0));
    }

    @Test
    public void testTimeout() {
        SqlStatement query = create().setTimeoutMillis(1);
        assertEquals(1, query.getTimeoutMillis());

        query.setTimeoutMillis(0);
        assertEquals(0, query.getTimeoutMillis());

        query.setTimeoutMillis(SqlStatement.TIMEOUT_NOT_SET);
        assertEquals(SqlStatement.TIMEOUT_NOT_SET, query.getTimeoutMillis());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTimeout_negative() {
        create().setTimeoutMillis(-2);
    }

    @Test
    public void testCursorBufferSize() {
        SqlStatement query = create().setCursorBufferSize(1);
        assertEquals(1, query.getCursorBufferSize());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCursorBufferSize_zero() {
        create().setCursorBufferSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCursorBufferSize_negative() {
        create().setCursorBufferSize(-1);
    }

    @Test
    public void testCopy() {
        SqlStatement original = create();
        original.setSchema("schema");
        checkEquals(original, original.copy(), true);

        original.setParameters(Arrays.asList(1, 2)).setTimeoutMillis(3L).setCursorBufferSize(4);
        checkEquals(original, original.copy(), true);
    }

    @Test
    public void testEquals() {
        SqlStatement query = create();
        SqlStatement another = query.copy();
        checkEquals(query, another, true);

        another = query.copy().setSql(SQL + "1");
        checkEquals(query, another, false);

        another = query.copy().setParameters(Arrays.asList(1, 2, 3));
        checkEquals(query, another, false);

        another = query.copy().setTimeoutMillis(11L);
        checkEquals(query, another, false);

        another = query.copy().setCursorBufferSize(21);
        checkEquals(query, another, false);

        another = query.copy().setSchema("schema");
        checkEquals(query, another, false);
    }

    private static SqlStatement create() {
        return new SqlStatement(SQL);
    }
}
