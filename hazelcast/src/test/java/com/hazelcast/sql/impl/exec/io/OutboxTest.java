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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.FaultyQueryOperationHandler;
import com.hazelcast.sql.impl.LoggingQueryOperationHandler;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.operation.QueryBatchExchangeOperation;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OutboxTest extends SqlTestSupport {

    private static final QueryId QUERY_ID = QueryId.create(UUID.randomUUID());
    private static final int EDGE_ID = 1;
    private static final UUID LOCAL_MEMBER_ID = UUID.randomUUID();
    private static final UUID TARGET_MEMBER_ID = UUID.randomUUID();

    private static final int ROW_WIDTH = 100;
    private static final int BATCH_SIZE = ROW_WIDTH * 4 + ROW_WIDTH / 2;
    private static final long REMAINING_MEMORY = ROW_WIDTH * 8 + ROW_WIDTH / 2;

    /** Must be greater than (remaining memory / row width). */
    private static final int REPEAT_COUNT = 10;

    /** Must be greater than (remaining memory / row width). */
    private static final int MAX_ROWS_IN_BATCH = 20;

    @Test
    public void testState() {
        Outbox outbox = createOutbox(new LoggingQueryOperationHandler());

        assertEquals(QUERY_ID, outbox.getQueryId());
        assertEquals(EDGE_ID, outbox.getEdgeId());
        assertEquals(ROW_WIDTH, outbox.getRowWidth());
        assertEquals(LOCAL_MEMBER_ID, outbox.getLocalMemberId());
        assertEquals(TARGET_MEMBER_ID, outbox.getTargetMemberId());
        assertEquals(BATCH_SIZE, outbox.getBatchSize());
        assertEquals(REMAINING_MEMORY, outbox.getRemainingMemory());
    }

    @Test
    public void testFlowControl() {
        Outbox outbox = createOutbox(new LoggingQueryOperationHandler());

        long remainingMemory = REMAINING_MEMORY * 2;

        outbox.onFlowControl(remainingMemory);

        assertEquals(remainingMemory, outbox.getRemainingMemory());
    }

    @Test
    public void testQualifier() {
        LoggingQueryOperationHandler operationHandler = new LoggingQueryOperationHandler();

        Outbox outbox = createOutbox(operationHandler);

        outbox.onRowBatch(createMonotonicBatch(0, 4), true, 0, rowIndex -> rowIndex > 1);

        assertEquals(1, operationHandler.getChannel().getSubmitCounter());

        LoggingQueryOperationHandler.SubmitInfo submitInfo = operationHandler.tryPollSubmitInfo();
        assertEquals(LOCAL_MEMBER_ID, submitInfo.getSourceMemberId());
        assertEquals(TARGET_MEMBER_ID, submitInfo.getMemberId());

        QueryBatchExchangeOperation operation = submitInfo.getOperation();

        checkMonotonicBatch(operation.getBatch(), 2, 2);
    }

    @Test
    public void testCannotSend() {
        Outbox outbox = createOutbox(FaultyQueryOperationHandler.INSTANCE);

        try {
            outbox.onRowBatch(EmptyRowBatch.INSTANCE, true, 0, AlwaysTrueOutboxSendQualifier.INSTANCE);

            fail();
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.MEMBER_CONNECTION, e.getCode());
        }
    }

    @Test
    public void testSend() {
        for (int batchSize = 0; batchSize <= MAX_ROWS_IN_BATCH; batchSize++) {
            checkSend(batchSize);
        }
    }

    private void checkSend(int batchSize) {
        LoggingQueryOperationHandler operationHandler = new LoggingQueryOperationHandler();

        Outbox outbox = createOutbox(operationHandler);

        // Send values to the inbox.
        AtomicLong memoryIncrease = new AtomicLong();
        int value = 0;

        for (int i = 1; i <= REPEAT_COUNT; i++) {
            boolean last = i == REPEAT_COUNT;

            if (batchSize > 0 || last) {
                send(outbox, value, batchSize, last, memoryIncrease);

                value += batchSize;
            }
        }

        // Validate values.
        int operationCount = 0;
        boolean last = false;

        List<Row> rows = new ArrayList<>();

        QueryBatchExchangeOperation previousOperation = null;

        while (true) {
            LoggingQueryOperationHandler.SubmitInfo submitInfo = operationHandler.tryPollSubmitInfo();

            if (submitInfo == null) {
                break;
            }

            assertEquals(LOCAL_MEMBER_ID, submitInfo.getSourceMemberId());
            assertEquals(TARGET_MEMBER_ID, submitInfo.getMemberId());

            QueryBatchExchangeOperation operation = submitInfo.getOperation();

            RowBatch batch = operation.getBatch();

            for (int i = 0; i < batch.getRowCount(); i++) {
                rows.add(batch.getRow(i));
            }

            last = operation.isLast();

            operationCount++;

            if (previousOperation != null) {
                // Check valid memory communication.
                if (operation.getRemainingMemory() < previousOperation.getRemainingMemory()) {
                    assertEquals(
                        previousOperation.getRemainingMemory(),
                        operation.getRemainingMemory() + operation.getBatch().getRowCount() * ROW_WIDTH
                    );
                }
            }

            previousOperation = operation;
        }

        assertTrue(last);
        assertTrue(operationCount > 0);
        assertEquals(operationHandler.getChannel().getSubmitCounter(), operationCount);
        checkMonotonicBatch(new ListRowBatch(rows), 0, value);

        // Validate memory: outbox current remaining == initial mem + memory increase from control flows - row memory.
        assertEquals(outbox.getRemainingMemory(), REMAINING_MEMORY + memoryIncrease.get() - rows.size() * ROW_WIDTH);
    }

    private void send(Outbox outbox, int startValue, int size, boolean last, AtomicLong memoryIncrease) {
        ListRowBatch batch = createMonotonicBatch(startValue, size);

        int position = 0;
        boolean sent = false;

        while (position < size) {
            // If there are no memory for a single row, then execute a control flow. Otherwise the test will hang.
            if (outbox.getRemainingMemory() < ROW_WIDTH) {
                memoryIncrease.addAndGet(REMAINING_MEMORY - outbox.getRemainingMemory());

                outbox.onFlowControl(REMAINING_MEMORY);
            }

            position = outbox.onRowBatch(batch, last, position, AlwaysTrueOutboxSendQualifier.INSTANCE);

            sent = true;
        }

        if (last && !sent) {
            outbox.onRowBatch(EmptyRowBatch.INSTANCE, true, 0, AlwaysTrueOutboxSendQualifier.INSTANCE);
        }
    }

    private static Outbox createOutbox(QueryOperationHandler operationHandler) {
        Outbox res = new Outbox(
            operationHandler,
            QUERY_ID,
            EDGE_ID,
            ROW_WIDTH,
            LOCAL_MEMBER_ID,
            TARGET_MEMBER_ID,
            BATCH_SIZE,
            REMAINING_MEMORY
        );

        res.setup();

        return res;
    }
}
