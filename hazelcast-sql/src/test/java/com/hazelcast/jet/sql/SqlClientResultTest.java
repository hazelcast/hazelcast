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

package com.hazelcast.jet.sql;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlClientResultTest extends SqlTestSupport {
    private static final String MAP_NAME = "map";

    @BeforeClass
    public static void setUpClass() {
        initializeWithClient(1, null, null);
    }

    @Before
    public void before() {
        createMapping(MAP_NAME, int.class, int.class);
        Map<Integer, Integer> map = instance().getMap(MAP_NAME);
        map.put(0, 0);
        map.put(1, 1);
    }

    @Test
    public void when_executingValidQuery() {
        try (SqlResult result = execute("SELECT * FROM " + MAP_NAME)) {
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
    public void when_executingInvalidQuery_then_fail() {
        checkSqlException(() -> execute("SELECT * FROM " + MAP_NAME + "_bad"), SqlErrorCode.OBJECT_NOT_FOUND, "Object 'map_bad' not found");
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void when_fetchingElementOnClosedResult_then_fail() {
        try (SqlResult result = execute("SELECT * FROM " + MAP_NAME)) {
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

    @Test
    public void when_checkingHasNextWithTimeout_then_timeoutOccurs() {
        try (SqlResult result = execute("select * from table(generate_stream(1))")) {
            assertTrue(result.isRowSet());
            ResultIterator<SqlRow> iterator = (ResultIterator<SqlRow>) result.iterator();

            int timeoutCount = 0;
            for (int i = 0; i < 2; i++) {
                while (iterator.hasNext(10, TimeUnit.MILLISECONDS) == ResultIterator.HasNextResult.TIMEOUT) {
                    timeoutCount++;
                }
                iterator.next();
            }
            assertNotEquals(0, timeoutCount);
        }
    }

    @Test
    public void when_checkingHasNextWithTimeout_then_timeoutIsLongerThanParam() {
        try (SqlResult result = execute("select * from table(generate_stream(1))")) {
            assertTrue(result.isRowSet());
            ResultIterator<SqlRow> iterator = (ResultIterator<SqlRow>) result.iterator();

            long shortestSleep = Long.MAX_VALUE;
            for (int i = 0; i < 2; i++) {
                long startNanos = System.nanoTime();
                while (iterator.hasNext(10, TimeUnit.MILLISECONDS) == ResultIterator.HasNextResult.TIMEOUT) {
                    shortestSleep = Math.min(shortestSleep, System.nanoTime() - startNanos);
                    startNanos = System.nanoTime();
                }
                iterator.next();
            }
            assertGreaterOrEquals("shortestSleep", shortestSleep, TimeUnit.MILLISECONDS.toNanos(10));
        }
    }

    private void checkSqlException(Runnable task, int expectedCode, String expectedMessage) {
        HazelcastSqlException err = assertThrows(HazelcastSqlException.class, task);

        assertEquals(expectedCode, err.getCode());
        assertTrue(err.getMessage(), err.getMessage().contains(expectedMessage));
    }

    private void checkIllegalStateException(Runnable task, String expectedMessage) {
        IllegalStateException err = assertThrows(IllegalStateException.class, task);

        assertEquals(expectedMessage, err.getMessage());
    }

    private SqlResult execute(String sql) {
        return client().getSql().execute(new SqlStatement(sql).setCursorBufferSize(1));
    }
}
