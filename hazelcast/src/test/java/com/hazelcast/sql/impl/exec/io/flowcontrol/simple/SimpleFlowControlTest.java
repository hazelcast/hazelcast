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

package com.hazelcast.sql.impl.exec.io.flowcontrol.simple;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.FaultyQueryOperationHandler;
import com.hazelcast.sql.impl.LoggingQueryOperationHandler;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.operation.QueryFlowControlExchangeOperation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SimpleFlowControlTest {
    @Test
    public void testFactory() {
        int initialMemory = 100;

        SimpleFlowControl flowControl = (SimpleFlowControl) SimpleFlowControlFactory.INSTANCE.create(initialMemory);

        assertEquals(initialMemory, flowControl.getMaxMemory());
        assertEquals(SimpleFlowControl.THRESHOLD_PERCENTAGE, flowControl.getThresholdPercentage(), 0.0d);
    }

    @Test
    public void testStream() {
        UUID memberId = UUID.randomUUID();

        SimpleFlowControlStream stream = new SimpleFlowControlStream(memberId, 100L, 200L);

        assertEquals(memberId, stream.getMemberId());
        assertEquals(100L, stream.getRemoteMemory());
        assertEquals(200L, stream.getLocalMemory());
        assertFalse(stream.isShouldSend());

        stream.updateMemory(300L, 400L);
        assertEquals(300L, stream.getRemoteMemory());
        assertEquals(400L, stream.getLocalMemory());

        stream.setShouldSend(true);
        assertTrue(stream.isShouldSend());
    }

    @Test
    public void testFlowControl() {
        long memMax = 1_000L;

        SimpleFlowControl flowControl = new SimpleFlowControl(memMax, 0.5d);

        QueryId queryId = QueryId.create(UUID.randomUUID());
        int edgeId = 1;
        LoggingQueryOperationHandler operationHandler = new LoggingQueryOperationHandler();

        flowControl.setup(queryId, edgeId, UUID.randomUUID(), operationHandler);

        // Test above threshold.
        UUID memberId1 = UUID.randomUUID();

        long deltaSmall = 100L;

        flowControl.onBatchAdded(memberId1, deltaSmall, false, memMax - deltaSmall);
        flowControl.onFragmentExecutionCompleted();
        assertNull(operationHandler.tryPollSubmitInfo());

        flowControl.onBatchRemoved(memberId1, deltaSmall, false);
        flowControl.onFragmentExecutionCompleted();
        assertNull(operationHandler.tryPollSubmitInfo());

        // Test below threshold.
        long deltaBig = 800L;

        flowControl.onBatchAdded(memberId1, deltaSmall, false, memMax - deltaSmall);
        flowControl.onBatchAdded(memberId1, deltaBig, false, memMax - deltaSmall - deltaBig);
        flowControl.onFragmentExecutionCompleted();
        assertNull(operationHandler.tryPollSubmitInfo());

        flowControl.onBatchRemoved(memberId1, deltaSmall, false);
        flowControl.onFragmentExecutionCompleted();
        assertNull(operationHandler.tryPollSubmitInfo());

        flowControl.onBatchRemoved(memberId1, deltaBig, false);
        flowControl.onFragmentExecutionCompleted();
        checkOperation(operationHandler, memberId1, queryId, edgeId, memMax);

        flowControl.onBatchAdded(memberId1, deltaSmall, false, memMax - deltaSmall);
        flowControl.onBatchRemoved(memberId1, deltaSmall, false);
        flowControl.onFragmentExecutionCompleted();
        assertNull(operationHandler.tryPollSubmitInfo());

        // Test below threshold with last batch.
        flowControl.onBatchAdded(memberId1, deltaSmall, false, memMax - deltaSmall);
        flowControl.onBatchAdded(memberId1, deltaBig, true, memMax - deltaSmall - deltaBig);
        flowControl.onBatchRemoved(memberId1, deltaSmall, false);
        flowControl.onBatchRemoved(memberId1, deltaBig, true);
        flowControl.onFragmentExecutionCompleted();
        assertNull(operationHandler.tryPollSubmitInfo());

        // Test several members.
        UUID memberId2 = UUID.randomUUID();

        flowControl.onBatchAdded(memberId1, deltaBig, false, memMax - deltaBig);
        flowControl.onBatchAdded(memberId2, deltaBig, false, memMax - deltaBig);
        flowControl.onBatchRemoved(memberId1, deltaBig, false);
        flowControl.onFragmentExecutionCompleted();
        checkOperation(operationHandler, memberId1, queryId, edgeId, memMax);

        flowControl.onBatchRemoved(memberId2, deltaBig, false);
        flowControl.onFragmentExecutionCompleted();
        checkOperation(operationHandler, memberId2, queryId, edgeId, memMax);
    }

    @Test
    public void testCannotSend() {
        SimpleFlowControl flowControl = new SimpleFlowControl(1_000L, 0.5d);
        flowControl.setup(QueryId.create(UUID.randomUUID()), 1, UUID.randomUUID(), FaultyQueryOperationHandler.INSTANCE);

        UUID memberId = UUID.randomUUID();

        flowControl.onBatchAdded(memberId, 800L, false, 200L);
        flowControl.onBatchRemoved(memberId, 800L, false);

        try {
            flowControl.onFragmentExecutionCompleted();
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.MEMBER_CONNECTION, e.getCode());
        }
    }

    private static void checkOperation(
        LoggingQueryOperationHandler operationHandler,
        UUID memberId,
        QueryId queryId,
        int edgeId,
        long memory
    ) {
        LoggingQueryOperationHandler.SubmitInfo submit = operationHandler.tryPollSubmitInfo();
        assertNotNull(submit);
        assertEquals(memberId, submit.getMemberId());

        QueryFlowControlExchangeOperation operation = submit.getOperation();
        assertEquals(queryId, operation.getQueryId());
        assertEquals(edgeId, operation.getEdgeId());
        assertEquals(memory, operation.getRemainingMemory());
    }
}
