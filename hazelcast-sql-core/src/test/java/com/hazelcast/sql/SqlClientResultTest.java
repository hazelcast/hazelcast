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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlClientResultTest {

    private static final String MAP_NAME = "map";
    private static final String SQL_GOOD = "SELECT * FROM " + MAP_NAME;
    private static final String SQL_BAD = "SELECT * FROM " + MAP_NAME + "_bad";

    private final SqlTestInstanceFactory factory = SqlTestInstanceFactory.create();
    private HazelcastInstance client;

    @Before
    public void before() {
        HazelcastInstance member = factory.newHazelcastInstance();
        client = factory.newHazelcastClient();

        Map<Integer, Integer> map = member.getMap(MAP_NAME);
        map.put(0, 0);
        map.put(1, 1);
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testQuery() {
        try (SqlResult result = execute(SQL_GOOD)) {
            assertEquals(2, result.getRowMetadata().getColumnCount());

            assertEquals(-1, result.updateCount());
            assertTrue(result.isRowSet());

            Iterator<SqlRow> iterator = result.iterator();
            iterator.next();
            iterator.next();
            assertFalse(iterator.hasNext());

            checkIllegalStateException(result::iterator, "Iterator can be requested only once");
        }
    }

    @Test
    public void testBadQuery() {
        try (SqlResult result = execute(SQL_BAD)) {
            checkSqlException(result::iterator, SqlErrorCode.PARSING, "Object 'map_bad' not found");
            checkSqlException(result::getRowMetadata, SqlErrorCode.PARSING, "Object 'map_bad' not found");
            checkSqlException(result::updateCount, SqlErrorCode.PARSING, "Object 'map_bad' not found");
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testCloseBetweenFetches() {
        try (SqlResult result = execute(SQL_GOOD)) {
            assertEquals(2, result.getRowMetadata().getColumnCount());

            assertEquals(-1, result.updateCount());
            assertTrue(result.isRowSet());

            Iterator<SqlRow> iterator = result.iterator();
            iterator.next();

            result.close();

            checkSqlException(iterator::hasNext, SqlErrorCode.CANCELLED_BY_USER, "Query was cancelled by the user");
            checkSqlException(iterator::next, SqlErrorCode.CANCELLED_BY_USER, "Query was cancelled by the user");
        }
    }

    private void checkSqlException(Runnable task, int expectedCode, String expectedMessage) {
        try {
            task.run();

            fail("Must fail");
        } catch (Exception e) {
            assertEquals(HazelcastSqlException.class, e.getClass());
            assertEquals(expectedCode, ((HazelcastSqlException) e).getCode());
            assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
        }
    }

    private void checkIllegalStateException(Runnable task, String expectedMessage) {
        try {
            task.run();

            fail("Must fail");
        } catch (Exception e) {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals(expectedMessage, e.getMessage());
        }
    }

    private SqlResult execute(String sql) {
        return client.getSql().execute(new SqlStatement(sql).setCursorBufferSize(1));
    }
}
