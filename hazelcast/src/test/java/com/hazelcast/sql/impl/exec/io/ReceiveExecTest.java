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

import com.hazelcast.sql.impl.LoggingFlowControl;
import com.hazelcast.sql.impl.LoggingQueryOperationHandler;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReceiveExecTest extends SqlTestSupport {
    @Test
    public void testReceive() {
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

        ReceiveExec exec = new ReceiveExec(1, inbox);

        // Test setup.
        exec.setup(emptyFragmentContext());

        assertTrue(flowControl.isSetupInvoked());

        // Advance on empty.
        assertEquals(IterationResult.WAIT, exec.advance());
        checkMonotonicBatch(exec.currentBatch(), 0, 0);

        // Read non-last batch.
        inbox.onBatch(new InboundBatch(createMonotonicBatch(0, 10), false, UUID.randomUUID()), 100L);

        assertEquals(IterationResult.FETCHED, exec.advance());
        checkMonotonicBatch(exec.currentBatch(), 0, 10);

        assertEquals(IterationResult.WAIT, exec.advance());
        checkMonotonicBatch(exec.currentBatch(), 10, 0);

        // Read last batch, with one stream still opened.
        inbox.onBatch(new InboundBatch(createMonotonicBatch(10, 20), true, UUID.randomUUID()), 100L);

        assertEquals(IterationResult.FETCHED, exec.advance());
        checkMonotonicBatch(exec.currentBatch(), 10, 20);

        assertEquals(IterationResult.WAIT, exec.advance());
        checkMonotonicBatch(exec.currentBatch(), 30, 0);

        // One more last batch, now closing the stream.
        inbox.onBatch(new InboundBatch(createMonotonicBatch(30, 30), false, UUID.randomUUID()), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(60, 40), true, UUID.randomUUID()), 100L);

        assertEquals(IterationResult.FETCHED, exec.advance());
        checkMonotonicBatch(exec.currentBatch(), 30, 30);

        assertEquals(IterationResult.FETCHED_DONE, exec.advance());
        checkMonotonicBatch(exec.currentBatch(), 60, 40);

        // Re-check after close.
        assertThrows(IllegalStateException.class, exec::advance);
    }
}
