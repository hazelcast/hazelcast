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

package com.hazelcast.sql.impl.worker;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.NoLogFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.operation.QueryBatchExchangeOperation;
import com.hazelcast.sql.impl.operation.QueryCancelOperation;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.operation.QueryOperation;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryOperationWorkerPoolTest extends HazelcastTestSupport {

    private static final int THREAD_COUNT = 4;

    private QueryOperationWorkerPool pool;

    @After
    public void after() {
        if (pool != null) {
            pool.stop();

            pool = null;
        }
    }

    @Test
    public void testSubmitLocal() {
        TestQueryOperationHandler operationHandler = new TestQueryOperationHandler();

        pool = createPool(operationHandler);

        // Test specific partition.
        QueryOperation operation = new QueryExecuteOperation();

        int repeatCount = 10;

        for (int i = 0; i < repeatCount; i++) {
            pool.submit(500, QueryOperationExecutable.local(operation));
        }

        assertTrueEventually(() -> {
            List<ExecuteInfo> infos = operationHandler.tryPollExecuteInfos(repeatCount);

            assertNotNull(infos);

            Set<String> threadNames = new HashSet<>();

            for (int i = 1; i < infos.size(); i++) {
                assertSame(operation, infos.get(i).getOperation());

                threadNames.add(infos.get(i).getThreadName());
            }

            assertEquals(1, threadNames.size());
        });

        // Test random partitions.
        for (int i = 0; i < repeatCount; i++) {
            pool.submit(QueryOperation.PARTITION_ANY, QueryOperationExecutable.local(operation));
        }

        assertTrueEventually(() -> {
            List<ExecuteInfo> infos = operationHandler.tryPollExecuteInfos(repeatCount);

            assertNotNull(infos);

            Set<String> threadNames = new HashSet<>();

            for (ExecuteInfo info : infos) {
                assertSame(operation, info.getOperation());

                threadNames.add(info.getThreadName());
            }

            assertTrue(threadNames.size() > 0);
        });
    }

    @Test
    public void testSubmitRemote() {
        TestQueryOperationHandler operationHandler = new TestQueryOperationHandler();

        pool = createPool(operationHandler);

        QueryCancelOperation cancelOperation =
            new QueryCancelOperation(QueryId.create(UUID.randomUUID()), -1, "err", UUID.randomUUID());

        cancelOperation.setCallerId(UUID.randomUUID());

        Packet packet = toPacket(cancelOperation);

        pool.submit(cancelOperation.getPartition(), QueryOperationExecutable.remote(packet));

        assertTrueEventually(() -> {
            List<ExecuteInfo> infos = operationHandler.tryPollExecuteInfos(1);

            assertNotNull(infos);

            QueryCancelOperation operation = (QueryCancelOperation) infos.get(0).getOperation();

            assertEquals(cancelOperation.getCallerId(), operation.getCallerId());
            assertEquals(cancelOperation.getQueryId(), operation.getQueryId());
            assertEquals(cancelOperation.getErrorCode(), operation.getErrorCode());
            assertEquals(cancelOperation.getErrorMessage(), operation.getErrorMessage());
            assertEquals(cancelOperation.getOriginatingMemberId(), operation.getOriginatingMemberId());
        });
    }

    @Test
    public void testSubmitRemoteWithDeserializationError() {
        TestQueryOperationHandler operationHandler = new TestQueryOperationHandler();

        pool = createPool(operationHandler);

        UUID localMemberId = UUID.randomUUID();
        UUID remoteMemberId = UUID.randomUUID();

        pool.init(localMemberId);

        QueryBatchExchangeOperation badOperation = new QueryBatchExchangeOperation(
            QueryId.create(remoteMemberId),
            1,
            new ListRowBatch(Collections.singletonList(new HeapRow(new Object[]{new BadValue()}))),
            false,
            100
        );

        pool.submit(badOperation.getPartition(), QueryOperationExecutable.remote(toPacket(badOperation)));

        assertTrueEventually(() -> {
            SubmitInfo info = operationHandler.tryPollSubmitInfo();

            assertNotNull(info);

            assertEquals(remoteMemberId, info.getMemberId());

            QueryCancelOperation cancelOperation = (QueryCancelOperation) info.getOperation();

            assertEquals(SqlErrorCode.GENERIC, cancelOperation.getErrorCode());
            assertTrue(cancelOperation.getErrorMessage().startsWith("Failed to deserialize"));
            assertEquals(localMemberId, cancelOperation.getOriginatingMemberId());
        });
    }

    @Test
    public void testShutdown() {
        pool = createPool(new TestQueryOperationHandler());

        pool.stop();

        for (int i = 0; i < THREAD_COUNT; i++) {
            QueryOperationWorker worker = pool.getWorker(i);

            assertTrueEventually(() -> assertTrue(worker.isThreadTerminated()));
        }
    }

    private QueryOperationWorkerPool createPool(TestQueryOperationHandler operationHandler) {
        return new QueryOperationWorkerPool(
            "instance",
            THREAD_COUNT,
            operationHandler,
            new DefaultSerializationServiceBuilder().build(),
            new NoLogFactory().getLogger("logger")
        );
    }

    private static Packet toPacket(QueryOperation operation) {
        return new Packet(new DefaultSerializationServiceBuilder().build().toBytes(operation), operation.getPartition());
    }

    private static class TestQueryOperationHandler implements QueryOperationHandler {

        private final LinkedBlockingQueue<SubmitInfo> submitInfos = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<ExecuteInfo> executeInfos = new LinkedBlockingQueue<>();

        @Override
        public boolean submit(UUID memberId, QueryOperation operation) {
            submitInfos.add(new SubmitInfo(memberId, operation));

            return true;
        }

        @Override
        public void execute(QueryOperation operation) {
            executeInfos.add(new ExecuteInfo(operation, Thread.currentThread().getName()));
        }

        public SubmitInfo tryPollSubmitInfo() {
            return submitInfos.poll();
        }

        public List<ExecuteInfo> tryPollExecuteInfos(int count) {
            if (executeInfos.size() >= count) {
                List<ExecuteInfo> res = new ArrayList<>();

                for (int i = 0; i < count; i++) {
                    res.add(executeInfos.poll());
                }

                return res;
            } else {
                return null;
            }
        }
    }

    private static class SubmitInfo {
        private final UUID memberId;
        private final QueryOperation operation;

        private SubmitInfo(UUID memberId, QueryOperation operation) {
            this.memberId = memberId;
            this.operation = operation;
        }

        private UUID getMemberId() {
            return memberId;
        }

        private QueryOperation getOperation() {
            return operation;
        }
    }

    private static class ExecuteInfo {
        private final QueryOperation operation;
        private final String threadName;

        private ExecuteInfo(QueryOperation operation, String threadName) {
            this.operation = operation;
            this.threadName = threadName;
        }

        private QueryOperation getOperation() {
            return operation;
        }

        private String getThreadName() {
            return threadName;
        }
    }

    private static class BadValue implements DataSerializable {
        private BadValue() {
            // No-op.
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(1);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            throw new IOException("Error!");
        }
    }
}
