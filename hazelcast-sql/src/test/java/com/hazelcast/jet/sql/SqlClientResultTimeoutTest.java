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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlClientResultTimeoutTest extends SqlTestSupport {

    private static final String SQL = "select * from table(generate_stream(1))";

    @BeforeClass
    public static void setUpClass() {
        initializeWithClient(1, null, null);
    }

    @Test
    public void when_checkingHasNextWithTimeout_then_timeoutOccurs() {
        try (SqlResult result = execute(SQL)) {
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
        try (SqlResult result = execute(SQL)) {
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

    private SqlResult execute(String sql) {
        return client().getSql().execute(new SqlStatement(sql).setCursorBufferSize(1));
    }
}
