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

package com.hazelcast.sql;

import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlQueryTest extends SqlTestSupport {

    private static final String SQL = "sql";

    @Test
    public void testDefaults() {
        SqlQuery query = create();

        assertEquals(SQL, query.getSql());
        assertEquals(0, query.getParameters().size());
        assertEquals(SqlQuery.DEFAULT_TIMEOUT, query.getTimeoutMillis());
        assertEquals(SqlQuery.DEFAULT_CURSOR_BUFFER_SIZE, query.getCursorBufferSize());
    }

    @Test(expected = NullPointerException.class)
    public void testSql_null() {
        new SqlQuery(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSql_empty() {
        new SqlQuery("");
    }

    @Test
    public void testParameters() {
        Object param0 = new Object();
        Object param1 = new Object();
        Object param2 = new Object();

        SqlQuery query = create();
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
        SqlQuery query = create().setTimeoutMillis(1);
        assertEquals(1, query.getTimeoutMillis());

        query.setTimeoutMillis(0);
        assertEquals(0, query.getTimeoutMillis());

        query.setTimeoutMillis(SqlQuery.TIMEOUT_NOT_SET);
        assertEquals(SqlQuery.TIMEOUT_NOT_SET, query.getTimeoutMillis());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTimeout_negative() {
        create().setTimeoutMillis(-2);
    }

    @Test
    public void testCursorBufferSize() {
        SqlQuery query = create().setCursorBufferSize(1);
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
        SqlQuery original = create();
        checkEquals(original, original.copy(), true);

        original.setParameters(Arrays.asList(1, 2)).setTimeoutMillis(3L).setCursorBufferSize(4);
        checkEquals(original, original.copy(), true);
    }

    @Test
    public void testEquals() {
        SqlQuery query = create();
        SqlQuery another = create();
        checkEquals(query, another, true);

        query.setParameters(Arrays.asList(1, 2)).setTimeoutMillis(10L).setCursorBufferSize(20);
        another.setParameters(Arrays.asList(1, 2)).setTimeoutMillis(10L).setCursorBufferSize(20);
        checkEquals(query, another, true);

        another.setSql(SQL + "1").setParameters(Arrays.asList(1, 2)).setTimeoutMillis(10L).setCursorBufferSize(20);
        checkEquals(query, another, false);

        another.setSql(SQL).setParameters(Arrays.asList(1, 2, 3)).setTimeoutMillis(10L).setCursorBufferSize(20);
        checkEquals(query, another, false);

        another.setSql(SQL).setParameters(Arrays.asList(1, 2)).setTimeoutMillis(11L).setCursorBufferSize(20);
        checkEquals(query, another, false);

        another.setSql(SQL).setParameters(Arrays.asList(1, 2)).setTimeoutMillis(10L).setCursorBufferSize(21);
        checkEquals(query, another, false);
    }

    @Test
    public void testToString() {
        SqlQuery query = create().setParameters(Arrays.asList(1, 2)).setTimeoutMillis(3L).setCursorBufferSize(4);

        String string = query.toString();

        assertTrue(string.contains("sql="));
        assertTrue(string.contains("parameters="));
        assertTrue(string.contains("timeout="));
        assertTrue(string.contains("cursorBufferSize="));
    }

    private static SqlQuery create() {
        return new SqlQuery(SQL);
    }
}
