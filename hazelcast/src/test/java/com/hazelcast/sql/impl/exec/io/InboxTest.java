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

package com.hazelcast.sql.impl.exec.io;

import com.hazelcast.sql.impl.LoggingQueryOperationHandler;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.LoggingFlowControl;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InboxTest {
    @Test
    public void testInbox() {
        UUID localMemberId = UUID.randomUUID();
        QueryId queryId = QueryId.create(UUID.randomUUID());
        int edgeId = 1;
        int rowWidth = 100;
        LoggingQueryOperationHandler operationHandler = new LoggingQueryOperationHandler();
        LoggingFlowControl flowControl = new LoggingFlowControl(queryId, edgeId, localMemberId, operationHandler);

        Inbox inbox = new Inbox(
            operationHandler,
            queryId,
            edgeId,
            rowWidth,
            localMemberId,
            2,
            flowControl
        );

        // Test state.
        assertEquals(queryId, inbox.getQueryId());
        assertEquals(edgeId, inbox.getEdgeId());
        assertEquals(rowWidth, inbox.getRowWidth());
        assertEquals(localMemberId, inbox.getLocalMemberId());

        // Test setup.
        inbox.setup();

        assertTrue(flowControl.isSetupInvoked());

        // Test add/remove.
        assertNull(inbox.poll());

        InboundBatch batch = createBatch(6, false);
        inbox.onBatch(batch, 400L);

        assertEquals(batch.getSenderId(), flowControl.getAddDescriptor().getMemberId());
        assertEquals(batch.getBatch().getRowCount() * rowWidth, flowControl.getAddDescriptor().getSize());
        assertEquals(batch.isLast(), flowControl.getAddDescriptor().isLast());
        assertEquals(400L, flowControl.getAddDescriptor().getRemoteMemory());

        checkBatch(batch, inbox.poll());

        assertEquals(batch.getSenderId(), flowControl.getRemoveDescriptor().getMemberId());
        assertEquals(batch.getBatch().getRowCount() * rowWidth, flowControl.getRemoveDescriptor().getSize());
        assertEquals(batch.isLast(), flowControl.getRemoveDescriptor().isLast());

        assertNull(inbox.poll());

        // Test close.
        assertEquals(2, inbox.getRemainingStreams());

        inbox.onBatch(createBatch(10, true), 100L);
        assertEquals(1, inbox.getRemainingStreams());
        assertFalse(inbox.closed());

        inbox.onBatch(createBatch(10, true), 100L);
        assertEquals(0, inbox.getRemainingStreams());
        assertFalse(inbox.closed());

        inbox.poll();
        assertFalse(inbox.closed());

        inbox.poll();
        assertTrue(inbox.closed());

        // Test fragment completion propagation.
        inbox.onFragmentExecutionCompleted();
        assertTrue(flowControl.isFragmentCallbackInvoked());
    }

    private static void checkBatch(InboundBatch expected, InboundBatch actual) {
        assertEquals(expected.getSenderId(), actual.getSenderId());
        assertSame(expected.getBatch(), actual.getBatch());
        assertEquals(expected.isLast(), actual.isLast());
    }

    private static InboundBatch createBatch(int rowsCount, boolean last) {
        List<Row> rows = new ArrayList<>();

        for (int i = 0; i < rowsCount; i++) {
            rows.add(HeapRow.of(i));
        }

        ListRowBatch batch = new ListRowBatch(rows);

        return new InboundBatch(batch, last, UUID.randomUUID());
    }
}
