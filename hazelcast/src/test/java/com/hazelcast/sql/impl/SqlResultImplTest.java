/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.state.QueryState;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class SqlResultImplTest extends SqlTestSupport {
    @Test
    public void test_rowsResult() {
        QueryId queryId = new QueryId(1, 2, 3, 4);
        SqlRowMetadata metadata = new SqlRowMetadata(singletonList(new SqlColumnMetadata("n", SqlColumnType.INTEGER, true)));
        QueryState queryState = QueryState.createInitiatorState(queryId, null, null, 0, null, null, metadata,
                null, System::currentTimeMillis);
        SqlResultImpl r = SqlResultImpl.createRowsResult(queryState, new DefaultSerializationServiceBuilder().build());

        assertEquals(-1, r.updateCount());
        assertEquals(metadata, r.getRowMetadata());
        assertEquals(queryId, r.getQueryId());
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void test_updateCountResult() {
        SqlResultImpl r = SqlResultImpl.createUpdateCountResult(10);
        assertEquals(10, r.updateCount());

        assertIllegalStateException("This result contains only update count", r::iterator);
        assertIllegalStateException("This result contains only update count", r::getRowMetadata);
        r.close();
    }

    private void assertIllegalStateException(String expectedMessage, Runnable action) {
        IllegalStateException err = assertThrows(IllegalStateException.class, action);
        assertEquals(expectedMessage, err.getMessage());
    }
}
