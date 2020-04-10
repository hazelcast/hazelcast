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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlCustomClass;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.plan.node.MockPlanNode;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.sql.impl.operation.QueryExecuteOperationFragmentMapping.EXPLICIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for operations.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryOperationsTest extends SqlTestSupport {
    @Test
    public void testExecute() {
        QueryId queryId = randomQueryId();

        QueryExecuteOperation original = prepareExecute(queryId);
        assertNotEquals(QueryOperation.PARTITION_ANY, original.getPartition());
        assertEquals(SqlDataSerializerHook.F_ID, original.getFactoryId());
        assertEquals(SqlDataSerializerHook.OPERATION_EXECUTE, original.getClassId());

        QueryExecuteOperation restored = serializeDeserialize(original);
        assertEquals(original.getPartition(), restored.getPartition());
        assertEquals(original.getPartitionMap(), restored.getPartitionMap());
        assertEquals(original.getFragments(), restored.getFragments());
        assertEquals(original.getOutboundEdgeMap(), restored.getOutboundEdgeMap());
        assertEquals(original.getInboundEdgeMap(), restored.getInboundEdgeMap());
        assertEquals(original.getEdgeInitialMemoryMap(), restored.getEdgeInitialMemoryMap());
        assertEquals(original.getArguments(), restored.getArguments());

        assertEquals(original.getPartition(), prepareExecute(queryId).getPartition());
        assertEquals(original.getPartition(), prepareCancel(queryId).getPartition());
    }

    @Test
    public void testCancel() {
        QueryId queryId = randomQueryId();

        QueryCancelOperation original = prepareCancel(queryId);
        assertNotEquals(QueryOperation.PARTITION_ANY, original.getPartition());
        assertEquals(SqlDataSerializerHook.F_ID, original.getFactoryId());
        assertEquals(SqlDataSerializerHook.OPERATION_CANCEL, original.getClassId());

        QueryCancelOperation restored = serializeDeserialize(original);
        assertEquals(original.getPartition(), restored.getPartition());
        assertEquals(original.getQueryId(), restored.getQueryId());
        assertEquals(original.getErrorCode(), restored.getErrorCode());
        assertEquals(original.getErrorMessage(), restored.getErrorMessage());
        assertEquals(original.getOriginatingMemberId(), restored.getOriginatingMemberId());

        assertEquals(original.getPartition(), prepareExecute(queryId).getPartition());
        assertEquals(original.getPartition(), prepareCancel(queryId).getPartition());
    }

    @Test
    public void testBatch() {
        QueryId queryId = randomQueryId();
        int edgeId = randomInt();

        QueryBatchExchangeOperation original = prepareBatch(queryId, edgeId);
        assertNotEquals(QueryOperation.PARTITION_ANY, original.getPartition());
        assertEquals(SqlDataSerializerHook.F_ID, original.getFactoryId());
        assertEquals(SqlDataSerializerHook.OPERATION_BATCH, original.getClassId());

        QueryBatchExchangeOperation restored = serializeDeserialize(original);
        assertEquals(original.getPartition(), restored.getPartition());
        assertEquals(original.getQueryId(), restored.getQueryId());
        checkBatches(original.getBatch(), restored.getBatch());
        assertEquals(original.isLast(), restored.isLast());
        assertEquals(original.getRemainingMemory(), restored.getRemainingMemory());

        assertEquals(original.getPartition(), prepareBatch(queryId, edgeId).getPartition());
        assertEquals(original.getPartition(), prepareFlowControl(queryId, edgeId).getPartition());
    }

    @Test
    public void testFlowControl() {
        QueryId queryId = randomQueryId();
        int edgeId = randomInt();

        QueryFlowControlExchangeOperation original = prepareFlowControl(queryId, edgeId);
        assertNotEquals(QueryOperation.PARTITION_ANY, original.getPartition());
        assertEquals(SqlDataSerializerHook.F_ID, original.getFactoryId());
        assertEquals(SqlDataSerializerHook.OPERATION_FLOW_CONTROL, original.getClassId());

        QueryFlowControlExchangeOperation restored = serializeDeserialize(original);
        assertEquals(original.getPartition(), restored.getPartition());
        assertEquals(original.getQueryId(), restored.getQueryId());
        assertEquals(original.getRemainingMemory(), restored.getRemainingMemory());

        assertEquals(original.getPartition(), prepareBatch(queryId, edgeId).getPartition());
        assertEquals(original.getPartition(), prepareFlowControl(queryId, edgeId).getPartition());
    }

    @Test
    public void testCheck() {
        List<QueryId> queryIds = Arrays.asList(randomQueryId(), randomQueryId());

        QueryCheckOperation original = withCallerId(new QueryCheckOperation(queryIds));
        assertEquals(queryIds, original.getQueryIds());
        assertEquals(QueryOperation.PARTITION_ANY, original.getPartition());
        assertEquals(SqlDataSerializerHook.F_ID, original.getFactoryId());
        assertEquals(SqlDataSerializerHook.OPERATION_CHECK, original.getClassId());

        QueryCheckOperation restored = serializeDeserialize(original);
        assertEquals(original.getQueryIds(), restored.getQueryIds());
    }

    @Test
    public void testCheckResponse() {
        List<QueryId> queryIds = Arrays.asList(randomQueryId(), randomQueryId());

        QueryCheckResponseOperation original = withCallerId(new QueryCheckResponseOperation(queryIds));
        assertEquals(queryIds, original.getQueryIds());
        assertEquals(QueryOperation.PARTITION_ANY, original.getPartition());
        assertEquals(SqlDataSerializerHook.F_ID, original.getFactoryId());
        assertEquals(SqlDataSerializerHook.OPERATION_CHECK_RESPONSE, original.getClassId());

        QueryCheckResponseOperation restored = serializeDeserialize(original);
        assertEquals(original.getQueryIds(), restored.getQueryIds());
    }

    private <T extends QueryOperation> T withCallerId(T operation) {
        UUID callerId = randomUUID();

        operation.setCallerId(callerId);

        assertEquals(callerId, operation.getCallerId());

        return operation;
    }

    private static <T extends QueryOperation> T serializeDeserialize(T original) {
        T restored = serialize(original);

        assertSame(original.getClass(), restored.getClass());

        assertEquals(original.getCallerId(), restored.getCallerId());

        assertEquals(original.getPartition(), restored.getPartition());

        assertEquals(original.getFactoryId(), restored.getFactoryId());
        assertEquals(original.getClassId(), restored.getClassId());

        return restored;
    }

    private QueryExecuteOperation prepareExecute(QueryId queryId) {
        Map<UUID, PartitionIdSet> partitionMapping = new HashMap<>();
        partitionMapping.put(randomUUID(), new PartitionIdSet(10));
        partitionMapping.put(randomUUID(), new PartitionIdSet(10));

        List<QueryExecuteOperationFragment> fragments = new ArrayList<>();
        fragments.add(new QueryExecuteOperationFragment(MockPlanNode.create(1, QueryDataType.INT), EXPLICIT,
            Arrays.asList(randomUUID(), randomUUID())));
        fragments.add(new QueryExecuteOperationFragment(MockPlanNode.create(2, QueryDataType.INT), EXPLICIT,
            Arrays.asList(randomUUID(), randomUUID())));

        Map<Integer, Integer> outboundEdgeMap = new HashMap<>();
        outboundEdgeMap.put(1, 2);
        outboundEdgeMap.put(3, 4);

        Map<Integer, Integer> inboundEdgeMap = new HashMap<>();
        inboundEdgeMap.put(5, 6);
        inboundEdgeMap.put(7, 8);

        Map<Integer, Long> edgeCreditMap = new HashMap<>();
        edgeCreditMap.put(9, 10L);
        edgeCreditMap.put(11, 12L);

        List<Object> arguments = Arrays.asList(randomInt(), randomString(), randomUUID());

        QueryExecuteOperation res = withCallerId(new QueryExecuteOperation(
            queryId, partitionMapping, fragments, outboundEdgeMap, inboundEdgeMap, edgeCreditMap, arguments)
        );

        assertEquals(queryId, res.getQueryId());
        assertEquals(partitionMapping, res.getPartitionMap());
        assertEquals(fragments, res.getFragments());
        assertEquals(outboundEdgeMap, res.getOutboundEdgeMap());
        assertEquals(inboundEdgeMap, res.getInboundEdgeMap());
        assertEquals(edgeCreditMap, res.getEdgeInitialMemoryMap());
        assertEquals(arguments, res.getArguments());

        return res;
    }

    private QueryCancelOperation prepareCancel(QueryId queryId) {
        int errorCode = randomInt();
        String errorMessage = randomString();
        UUID originatingMemberId = randomUUID();

        QueryCancelOperation res = withCallerId(new QueryCancelOperation(queryId, errorCode, errorMessage, originatingMemberId));

        assertEquals(queryId, res.getQueryId());
        assertEquals(errorCode, res.getErrorCode());
        assertEquals(errorMessage, res.getErrorMessage());
        assertEquals(originatingMemberId, res.getOriginatingMemberId());

        return res;
    }

    private QueryBatchExchangeOperation prepareBatch(QueryId queryId, int edgeId) {
        UUID targetMemberId = UUID.randomUUID();

        RowBatch batch = new ListRowBatch(Arrays.asList(
            new HeapRow(new Object[] { new SqlCustomClass(randomInt()), randomUUID()}),
            new HeapRow(new Object[] { new SqlCustomClass(randomInt()), randomUUID()})
        ));

        boolean last = randomBoolean();
        long remainingMemory = randomLong();

        QueryBatchExchangeOperation res = withCallerId(
            new QueryBatchExchangeOperation(queryId, edgeId, targetMemberId, batch, last, remainingMemory)
        );

        assertTrue(res.isInbound());
        assertEquals(queryId, res.getQueryId());
        assertEquals(edgeId, res.getEdgeId());
        assertEquals(targetMemberId, res.getTargetMemberId());
        checkBatches(batch, res.getBatch());
        assertEquals(last, res.isLast());
        assertEquals(remainingMemory, res.getRemainingMemory());

        return res;
    }

    private QueryFlowControlExchangeOperation prepareFlowControl(QueryId queryId, int edgeId) {
        long remainingMemory = randomLong();

        QueryFlowControlExchangeOperation res = withCallerId(
            new QueryFlowControlExchangeOperation(queryId, edgeId, remainingMemory)
        );

        assertFalse(res.isInbound());
        assertEquals(queryId, res.getQueryId());
        assertEquals(edgeId, res.getEdgeId());
        assertEquals(remainingMemory, res.getRemainingMemory());

        return res;
    }

    private static void checkBatches(RowBatch batch1, RowBatch batch2) {
        assertEquals(batch1.getRowCount(), batch1.getRowCount());

        for (int i = 0; i < batch1.getRowCount(); i++) {
            Row row1 = batch1.getRow(i);
            Row row2 = batch2.getRow(i);

            assertEquals(row1.getColumnCount(), row2.getColumnCount());

            for (int j = 0; j < row1.getColumnCount(); j++) {
                Object value1 = row1.get(j);
                Object value2 = row2.get(j);

                assertEquals(value1, value2);
            }
        }
    }

    private static boolean randomBoolean() {
        return ThreadLocalRandom.current().nextBoolean();
    }

    private static int randomInt() {
        return ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);
    }

    private static long randomLong() {
        return ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
    }

    private static UUID randomUUID() {
        return UUID.randomUUID();
    }

    private static QueryId randomQueryId() {
        return QueryId.create(randomUUID());
    }
}

