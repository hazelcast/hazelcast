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

package com.hazelcast.sql.impl;

import com.hazelcast.internal.json.TestUtil.RunnableEx;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.state.QueryState;
import org.junit.Test;

import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SqlResultImplTest {

    @Test
    public void test_rowsResult() {
        QueryId queryId = new QueryId(1, 2, 3, 4);
        SqlRowMetadata metadata = new SqlRowMetadata(singletonList(new SqlColumnMetadata("n", SqlColumnType.INTEGER)));
        QueryState queryState = QueryState.createInitiatorState(queryId, null, null, 0, null, null, metadata,
                null, System::currentTimeMillis);
        SqlResultImpl r = SqlResultImpl.createRowsResult(queryState);

        assertEquals(-1, r.updateCount());
        assertEquals(metadata, r.getRowMetadata());
        assertEquals(queryId, r.getQueryId());
    }

    @Test
    public void test_updateCountResult() {
        SqlResultImpl r = SqlResultImpl.createUpdateCountResult(10);
        assertEquals(10, r.updateCount());
        assertThrows(IllegalStateException.class, "This result contains only update count", () -> r.iterator());
        assertThrows(IllegalStateException.class, "This result contains only update count", () -> r.getRowMetadata());
        r.close();
    }

    private void assertThrows(Class<? extends Throwable> expectedClass, String expectedMessage, RunnableEx action) {
        try {
            action.run();
            fail("action didn't fail");
        } catch (Throwable e) {
            assertInstanceOf(expectedClass, e);
            assertEquals(expectedMessage, e.getMessage());
        }
    }
}
