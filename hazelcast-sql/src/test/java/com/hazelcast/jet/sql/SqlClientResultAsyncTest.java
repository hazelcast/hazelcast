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

import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlClientResultAsyncTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";
    private static final String SQL = "SELECT * FROM " + MAP_NAME;

    @BeforeClass
    public static void setUpClass() {
        initializeWithClient(1, null, null);
    }

    @Before
    public void before() {
        createMapping(MAP_NAME, int.class, int.class);
        Map<Integer, Integer> map = instance().getMap(MAP_NAME);
        for (int i = 0; i < 10_000; i++) {
            map.put(i, i);
        }
    }

    @Test
    // TODO:
    public void testHasNextWithTimeoutFinish() {
        try (SqlResult result = execute(SQL)) {
            assertEquals(2, result.getRowMetadata().getColumnCount());
            assertEquals(-1, result.updateCount());
            assertTrue(result.isRowSet());
            ResultIterator<SqlRow> iterator = (ResultIterator<SqlRow>) result.iterator();

            ResultIterator.HasNextResult hasNextResult;
            int timeoutCount = 0;
            while (true) {
                while ((hasNextResult = iterator.hasNext(100, TimeUnit.NANOSECONDS)) == ResultIterator.HasNextResult.TIMEOUT) {
                    timeoutCount++;
                }
                if (hasNextResult == ResultIterator.HasNextResult.DONE) {
                    break;
                }
                iterator.next();
            }

            checkIllegalStateException(result::iterator, "Iterator can be requested only once");
        }
    }

    private void checkIllegalStateException(Runnable task, String expectedMessage) {
        IllegalStateException err = assertThrows(IllegalStateException.class, task);
        assertEquals(expectedMessage, err.getMessage());
    }

    private SqlResult execute(String sql) {
        return client().getSql().execute(new SqlStatement(sql).setCursorBufferSize(1));
    }
}
