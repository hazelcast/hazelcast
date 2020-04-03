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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.LoggingQueryOperationHandler;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.exec.io.Inbox;
import com.hazelcast.sql.impl.exec.io.Outbox;
import com.hazelcast.sql.impl.exec.io.ReceiveExec;
import com.hazelcast.sql.impl.exec.io.SendExec;
import com.hazelcast.sql.impl.exec.io.flowcontrol.simple.SimpleFlowControl;
import com.hazelcast.sql.impl.exec.io.flowcontrol.simple.SimpleFlowControlFactory;
import com.hazelcast.sql.impl.exec.root.RootExec;
import com.hazelcast.sql.impl.exec.root.RootResultConsumer;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.operation.QueryExecuteOperationFragment;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.plan.node.RootPlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.plan.node.io.RootSendPlanNode;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.sql.impl.operation.QueryExecuteOperationFragmentMapping.DATA_MEMBERS;
import static com.hazelcast.sql.impl.operation.QueryExecuteOperationFragmentMapping.EXPLICIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CreateExecPlanNodeVisitorTest {

    private static final int ROOT_BATCH_SIZE = 1024;
    private static final int OUTBOX_BATCH_SIZE = 512 * 1024;

    private static final int EDGE_1_ID = 100;
    private static final long EDGE_1_INITIAL_MEMORY = 1000;

    private static final UUID MEMBER_ID_1 = UUID.randomUUID();
    private static final UUID MEMBER_ID_2 = UUID.randomUUID();

    private static final QueryId QUERY_ID = new QueryId();

    private static final int PARTITION_COUNT = 4;
    private static final int[] PARTITIONS_MEMBER_1 = new int[] { 1, 2 };
    private static final int[] PARTITIONS_MEMBER_2 = new int[] { 3, 4 };
    private static final Map<UUID, PartitionIdSet> PARTITION_MAPPING;

    private static final long TIMEOUT = 0;

    private int idGenerator;

    static {
        PARTITION_MAPPING = new HashMap<>();
        PARTITION_MAPPING.put(MEMBER_ID_1, createPartitionIdSet(PARTITION_COUNT, PARTITIONS_MEMBER_1));
        PARTITION_MAPPING.put(MEMBER_ID_2, createPartitionIdSet(PARTITION_COUNT, PARTITIONS_MEMBER_2));
    }

    @Test
    public void testRoot() {
        UpstreamNode upstreamNode = new UpstreamNode(nextNodeId());
        RootPlanNode rootNode = new RootPlanNode(nextNodeId(), upstreamNode);

        QueryExecuteOperationFragment rootFragment = new QueryExecuteOperationFragment(
            rootNode,
            EXPLICIT,
            Collections.singletonList(MEMBER_ID_1)
        );

        QueryExecuteOperation operation = createOperation(
            Collections.singletonList(rootFragment),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        // Visit fragment 0.
        CreateExecPlanNodeVisitor visitor = visit(operation, rootFragment);

        RootExec rootExec = (RootExec) visitor.getExec();
        assertEquals(rootNode.getId(), rootExec.getId());
        assertEquals(operation.getRootConsumer(), rootExec.getConsumer());
        assertEquals(ROOT_BATCH_SIZE, rootExec.getBatchSize());

        UpstreamExec upstreamExec = (UpstreamExec) rootExec.getUpstream();
        assertEquals(upstreamNode.getId(), upstreamExec.getId());
    }

    @Test
    public void testRootSend() {
        UpstreamNode upstreamNode = new UpstreamNode(nextNodeId());
        RootSendPlanNode sendNode = new RootSendPlanNode(nextNodeId(), upstreamNode, EDGE_1_ID);

        QueryExecuteOperationFragment sendFragment = new QueryExecuteOperationFragment(
            sendNode,
            DATA_MEMBERS,
            null
        );

        QueryExecuteOperationFragment receiveFragment = new QueryExecuteOperationFragment(
            null,
            EXPLICIT,
            Collections.singletonList(MEMBER_ID_1)
        );

        QueryExecuteOperation operation = createOperation(
            Arrays.asList(sendFragment, receiveFragment),
            Collections.singletonMap(EDGE_1_ID, 0),
            Collections.singletonMap(EDGE_1_ID, 1),
            Collections.singletonMap(EDGE_1_ID, EDGE_1_INITIAL_MEMORY)
        );

        CreateExecPlanNodeVisitor visitor = visit(operation, sendFragment);

        SendExec sendExec = (SendExec) visitor.getExec();
        Outbox outbox = sendExec.getOutbox();

        assertEquals(sendNode.getId(), sendExec.getId());

        assertEquals(QUERY_ID, outbox.getQueryId());
        assertEquals(EDGE_1_ID, outbox.getEdgeId());
        assertEquals(upstreamNode.getSchema().getEstimatedRowSize(), outbox.getRowWidth());
        assertEquals(MEMBER_ID_1, outbox.getTargetMemberId());
        assertEquals(OUTBOX_BATCH_SIZE, outbox.getBatchSize());
        assertEquals(EDGE_1_INITIAL_MEMORY, outbox.getRemainingMemory());

        UpstreamExec upstreamExec = (UpstreamExec) sendExec.getUpstream();
        assertEquals(upstreamNode.getId(), upstreamExec.getId());

        assertEquals(0, visitor.getInboxes().size());

        assertEquals(1, visitor.getOutboxes().size());
        assertEquals(1, visitor.getOutboxes().get(EDGE_1_ID).size());
        assertSame(outbox, visitor.getOutboxes().get(EDGE_1_ID).get(MEMBER_ID_1));
    }

    @Test
    public void testReceive() {
        ReceivePlanNode receiveNode = new ReceivePlanNode(
            nextNodeId(),
            EDGE_1_ID,
            Arrays.asList(QueryDataType.INT, QueryDataType.VARCHAR)
        );

        DownstreamNode downstreamNode = new DownstreamNode(
            nextNodeId(),
            receiveNode
        );

        QueryExecuteOperationFragment sendFragment = new QueryExecuteOperationFragment(
            null,
            DATA_MEMBERS,
            PARTITION_MAPPING.keySet()
        );

        QueryExecuteOperationFragment receiveFragment = new QueryExecuteOperationFragment(
            downstreamNode,
            EXPLICIT,
            Collections.singletonList(MEMBER_ID_1)
        );

        QueryExecuteOperation operation = createOperation(
            Arrays.asList(sendFragment, receiveFragment),
            Collections.singletonMap(EDGE_1_ID, 0),
            Collections.singletonMap(EDGE_1_ID, 1),
            Collections.singletonMap(EDGE_1_ID, EDGE_1_INITIAL_MEMORY)
        );

        CreateExecPlanNodeVisitor visitor = visit(operation, receiveFragment);

        DownstreamExec downstreamExec = (DownstreamExec) visitor.pop();
        assertEquals(downstreamNode.getId(), downstreamExec.getId());

        ReceiveExec receiveExec = (ReceiveExec) downstreamExec.getUpstream();
        assertEquals(receiveNode.getId(), receiveExec.getId());

        Inbox inbox = receiveExec.getInbox();
        assertEquals(QUERY_ID, inbox.getQueryId());
        assertEquals(EDGE_1_ID, inbox.getEdgeId());
        assertEquals(receiveNode.getSchema().getEstimatedRowSize(), inbox.getRowWidth());
        assertEquals(PARTITION_MAPPING.size(), inbox.getRemainingStreams());
        assertEquals(EDGE_1_INITIAL_MEMORY, ((SimpleFlowControl) inbox.getFlowControl()).getMaxMemory());

        assertEquals(1, visitor.getInboxes().size());
        assertSame(inbox, visitor.getInboxes().get(EDGE_1_ID));

        assertEquals(0, visitor.getOutboxes().size());
    }

    private static CreateExecPlanNodeVisitor visit(QueryExecuteOperation operation, QueryExecuteOperationFragment fragment) {
        CreateExecPlanNodeVisitor res = new CreateExecPlanNodeVisitor(
            new LoggingQueryOperationHandler(),
            operation,
            SimpleFlowControlFactory.INSTANCE,
            OUTBOX_BATCH_SIZE
        );

        fragment.getNode().visit(res);

        return res;
    }

    private static QueryExecuteOperation createOperation(
        List<QueryExecuteOperationFragment> fragments,
        Map<Integer, Integer> outboundEdgeMap,
        Map<Integer, Integer> inboundEdgeMap,
        Map<Integer, Long> edgeInitialMemoryMap
    ) {
        QueryExecuteOperation operation = new QueryExecuteOperation(
            QUERY_ID,
            PARTITION_MAPPING,
            fragments,
            outboundEdgeMap,
            inboundEdgeMap,
            edgeInitialMemoryMap,
            Collections.emptyList(),
            TIMEOUT
        );

        operation.setRootConsumer(new TestRootResultConusmer(), ROOT_BATCH_SIZE);

        return operation;
    }

    private static PartitionIdSet createPartitionIdSet(int size, int... partitions) {
        PartitionIdSet res = new PartitionIdSet(size);

        if (partitions != null) {
            for (int partition : partitions) {
                res.add(partition);
            }
        }

        return res;
    }

    private int nextNodeId() {
        return idGenerator++;
    }

    private static class UpstreamNode implements PlanNode, CreateExecPlanNodeVisitorCallback {

        private final int id;
        private final List<QueryDataType> types;

        private UpstreamNode(int id) {
            this.id = id;

            types = Arrays.asList(QueryDataType.INT, QueryDataType.VARCHAR);
        }

        @Override
        public int getId() {
            return id;
        }

        @Override
        public void visit(PlanNodeVisitor visitor) {
            visitor.onOtherNode(this);
        }

        @Override
        public void onVisit(CreateExecPlanNodeVisitor visitor) {
            visitor.push(new UpstreamExec(getId()));
        }

        @Override
        public PlanNodeSchema getSchema() {
            return new PlanNodeSchema(types);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            // No-op.
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            // No-op.
        }
    }

    private static class DownstreamNode implements PlanNode, CreateExecPlanNodeVisitorCallback {

        private final int id;
        private final PlanNode upstream;

        private DownstreamNode(int id, PlanNode upstream) {
            this.id = id;
            this.upstream = upstream;
        }

        @Override
        public int getId() {
            return id;
        }

        @Override
        public void visit(PlanNodeVisitor visitor) {
            upstream.visit(visitor);

            visitor.onOtherNode(this);
        }

        @Override
        public void onVisit(CreateExecPlanNodeVisitor visitor) {
            visitor.push(new DownstreamExec(id, visitor.pop()));
        }

        @Override
        public PlanNodeSchema getSchema() {
            return upstream.getSchema();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            // No-op.
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            // No-op.
        }
    }

    private static class UpstreamExec extends AbstractExec {
        private UpstreamExec(int id) {
            super(id);
        }

        @Override
        protected IterationResult advance0() {
            return null;
        }

        @Override
        protected RowBatch currentBatch0() {
            return null;
        }
    }

    private static class DownstreamExec extends AbstractUpstreamAwareExec {
        private DownstreamExec(int id, Exec upstream) {
            super(id, upstream);
        }

        @Override
        protected IterationResult advance0() {
            return null;
        }

        @Override
        protected RowBatch currentBatch0() {
            return null;
        }
    }

    private static class TestRootResultConusmer implements RootResultConsumer {
        @Override
        public void setup(QueryFragmentContext context) {
            // No-op.
        }

        @Override
        public boolean consume(List<Row> batch, boolean last) {
            return false;
        }

        @Override
        public Iterator<Row> iterator() {
            return null;
        }

        @Override
        public void onError(HazelcastSqlException error) {
            // No-op.
        }
    }
}
