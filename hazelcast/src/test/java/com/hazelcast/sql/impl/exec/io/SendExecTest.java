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
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.UpstreamExec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.operation.QueryBatchExchangeOperation;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SendExecTest extends SqlTestSupport {

    private static final QueryId QUERY_ID = QueryId.create(UUID.randomUUID());
    private static final int EDGE_ID = 1;
    private static final UUID LOCAL_MEMBER_ID = UUID.randomUUID();
    private static final UUID TARGET_MEMBER_ID = UUID.randomUUID();

    private static final int ROW_WIDTH = 100;

    private static final int ROWS_IN_BATCH = 4;
    private static final int ROWS_IN_REMAINING_MEMORY = 8;

    @Test
    public void testSetup() {
        Sender sender = create();

        assertNotNull(sender.getOperationHandler().getChannel());
    }

    @Test
    public void testAdvance() {
        Sender sender = create();
        SendExec sendExec = sender.getSender();

        // Wait on empty state.
        assertEquals(IterationResult.WAIT, sendExec.advance());

        // Push batch that is below the threshold.
        sender.addUpstreamData(ROWS_IN_BATCH / 2, false);
        assertEquals(IterationResult.WAIT, sendExec.advance());
        sender.checkRows(0);

        // Push batch above the send threshold.
        sender.addUpstreamData(ROWS_IN_BATCH / 2 - 1, false);
        sender.addUpstreamData(ROWS_IN_BATCH / 2 + 1, false);
        assertEquals(IterationResult.WAIT, sendExec.advance());
        sender.checkRows(sender.getRowsSubmitted());

        // Push big batch which could not be consumed at once.
        sender.restoreRemainingMemory();
        sender.addUpstreamData(ROWS_IN_REMAINING_MEMORY + 1, false);
        assertEquals(IterationResult.WAIT, sendExec.advance());
        sender.checkRows(sender.getRowsSubmitted() - 1);

        // Enqueue another batch, nothing should happen since we are low on memory.
        sender.addUpstreamData(2, false);
        assertEquals(IterationResult.WAIT, sendExec.advance());
        sender.checkRows(sender.getRowsSubmitted() - 3);

        // Return memory and submit the last batch. Several rows are still pending due to insufficient memory.
        sender.restoreRemainingMemory();
        sender.addUpstreamData(ROWS_IN_REMAINING_MEMORY, true);
        assertEquals(IterationResult.WAIT, sendExec.advance());
        sender.checkRows(sender.getRowsSubmitted() - 3);

        // Return memory and try again. Now all rows should be sent.
        sender.restoreRemainingMemory();
        assertEquals(IterationResult.FETCHED_DONE, sendExec.advance());
        sender.checkRows(sender.getRowsSubmitted());

        // Test isolated "last" batch that is sent in one hop.
        sender = create();
        sendExec = sender.getSender();

        sender.addUpstreamData(2, true);
        assertEquals(IterationResult.FETCHED_DONE, sendExec.advance());
        sender.checkRows(sender.getRowsSubmitted());
    }

    @Test
    public void testCurrentBatch() {
        Sender sender = create();

        assertThrows(UnsupportedOperationException.class, () -> sender.getSender().currentBatch());
    }

    private Sender create() {
        UpstreamExec upstream = new UpstreamExec(1);

        LoggingQueryOperationHandler operationHandler = new LoggingQueryOperationHandler();

        Outbox outbox = new Outbox(
            operationHandler,
            QUERY_ID,
            EDGE_ID,
            ROW_WIDTH,
            LOCAL_MEMBER_ID,
            TARGET_MEMBER_ID,
            ROW_WIDTH * ROWS_IN_BATCH,
            ROW_WIDTH * ROWS_IN_REMAINING_MEMORY
        );

        SendExec sender = new SendExec(2, upstream, outbox);

        sender.setup(emptyFragmentContext());

        return new Sender(sender, upstream, outbox, operationHandler);
    }

    private static class Sender {

        private final SendExec sender;
        private final UpstreamExec upstream;
        private final Outbox outbox;
        private final LoggingQueryOperationHandler operationHandler;

        private final List<Row> rows = new ArrayList<>();

        private int rowsSubmitted;

        private Sender(SendExec sender, UpstreamExec upstream, Outbox outbox, LoggingQueryOperationHandler operationHandler) {
            this.sender = sender;
            this.upstream = upstream;
            this.outbox = outbox;
            this.operationHandler = operationHandler;
        }

        private SendExec getSender() {
            return sender;
        }

        private LoggingQueryOperationHandler getOperationHandler() {
            return operationHandler;
        }

        private int getRowsSubmitted() {
            return rowsSubmitted;
        }

        private void addUpstreamData(int rowCount, boolean last) {
            ListRowBatch batch = createMonotonicBatch(rowsSubmitted, rowCount);

            rowsSubmitted += rowCount;

            upstream.addResult(last ? IterationResult.FETCHED_DONE : IterationResult.FETCHED, batch);
        }

        private void restoreRemainingMemory() {
            outbox.onFlowControl(ROW_WIDTH * ROWS_IN_REMAINING_MEMORY);
        }

        private void checkRows(int maxValue) {
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
            }

            checkMonotonicBatch(new ListRowBatch(rows), 0, maxValue);
        }
    }
}
