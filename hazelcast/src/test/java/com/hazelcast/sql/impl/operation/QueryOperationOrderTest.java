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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.NodeServiceProviderImpl;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlInternalService;
import com.hazelcast.sql.impl.SqlServiceProxy;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.exec.AbstractExec;
import com.hazelcast.sql.impl.exec.AbstractUpstreamAwareExec;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitor;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitorCallback;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.exec.root.BlockingRootResultConsumer;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.plan.node.UniInputPlanNode;
import com.hazelcast.sql.impl.plan.node.ZeroInputPlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.sql.impl.SqlServiceProxy.OUTBOX_BATCH_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A set of integration tests for query message processing on a single member.
 * <p>
 * Abbreviations:
 * <ul>
 *     <li>E - execute</li>
 *     <li>Bx - batch request with x ordinal</li>
 *     <li>C - cancel</li>
 * </ul>
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryOperationOrderTest extends SqlTestSupport {

    private static final int EDGE_ID = 1;
    private static final int BATCH_SIZE = 100;

    private static final long STATE_CHECK_FREQUENCY_SMALL = 100L;
    private static final long STATE_CHECK_FREQUENCY_BIG = Long.MAX_VALUE;

    private static volatile State testState;

    private TestHazelcastInstanceFactory factory;

    private HazelcastInstanceProxy initiator;
    private HazelcastInstanceProxy participant;

    private SqlServiceProxy initiatorService;
    private SqlServiceProxy participantService;

    private Map<UUID, PartitionIdSet> partitionMap;

    private QueryExecuteOperation participantExecuteOperation;
    private QueryBatchExchangeOperation participantBatch1Operation;
    private QueryBatchExchangeOperation participantBatch2Operation;
    private QueryCancelOperation participantCancelOperation;

    private QueryOperationChannel channel;

    @Before
    public void before() {
        factory = new TestHazelcastInstanceFactory(2);

        initiator = (HazelcastInstanceProxy) factory.newHazelcastInstance();
        participant = (HazelcastInstanceProxy) factory.newHazelcastInstance();

        initiatorService = getService(initiator);
        participantService = getService(participant);

        setInternalService(initiator, STATE_CHECK_FREQUENCY_BIG);
        setInternalService(participant, STATE_CHECK_FREQUENCY_SMALL);

        UUID initiatorId = initiator.getLocalEndpoint().getUuid();
        UUID participantId = participant.getLocalEndpoint().getUuid();

        partitionMap = new HashMap<>();
        partitionMap.put(initiatorId, new PartitionIdSet(2, Collections.singletonList(1)));
        partitionMap.put(participantId, new PartitionIdSet(1, Collections.singletonList(2)));

        testState = startInitiator();

        // Prepare operations that will be used throughout the test.
        PlanNode participantNode = new ParticipantNode(
            2,
            new ReceivePlanNode(3, EDGE_ID, Collections.singletonList(QueryDataType.INT))
        );

        QueryExecuteOperationFragment participantInitiatorFragment = new QueryExecuteOperationFragment(
            null,
            QueryExecuteOperationFragmentMapping.EXPLICIT,
            Collections.singletonList(initiatorId)
        );

        QueryExecuteOperationFragment participantFragment = new QueryExecuteOperationFragment(
            participantNode,
            QueryExecuteOperationFragmentMapping.EXPLICIT,
            Collections.singletonList(participantId)
        );

        participantExecuteOperation = new QueryExecuteOperation(
            testState.getQueryId(),
            partitionMap,
            Arrays.asList(participantInitiatorFragment, participantFragment),
            Collections.singletonMap(EDGE_ID, 0),
            Collections.singletonMap(EDGE_ID, 1),
            Collections.singletonMap(EDGE_ID, Long.MAX_VALUE),
            Collections.emptyList()
        );

        participantBatch1Operation = new QueryBatchExchangeOperation(
            testState.getQueryId(),
            EDGE_ID,
            participantId,
            createMonotonicBatch(0, BATCH_SIZE),
            false,
            Long.MAX_VALUE
        );

        participantBatch2Operation = new QueryBatchExchangeOperation(
            testState.getQueryId(),
            EDGE_ID,
            participantId,
            createMonotonicBatch(BATCH_SIZE, BATCH_SIZE),
            true,
            Long.MAX_VALUE
        );

        participantCancelOperation = new QueryCancelOperation(
            testState.getQueryId(),
            SqlErrorCode.GENERIC,
            "Error",
            initiatorId
        );

        channel = initiatorService.getInternalService().getOperationHandler().createChannel(initiatorId, participantId);
    }

    private State startInitiator() {
        UUID initiatorId = initiator.getLocalEndpoint().getUuid();

        Plan plan = new Plan(
            partitionMap,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        QueryId queryId = initiatorService.getInternalService().getStateRegistry().onInitiatorQueryStarted(
            initiatorId,
            Long.MAX_VALUE,
            plan,
            new BlockingRootResultConsumer(),
            initiatorService.getInternalService().getOperationHandler(),
            true
        ).getQueryId();

        testState = new State(queryId);

        // TODO: Remove?
//        PlanNode initiatorNode = new InitiatorNode(1);
//
//        QueryExecuteOperationFragment initiatorFragment = new QueryExecuteOperationFragment(
//            initiatorNode,
//            QueryExecuteOperationFragmentMapping.EXPLICIT,
//            Collections.singletonList(initiatorId)
//        );
//
//        QueryExecuteOperation initiatorExecuteOperation = new QueryExecuteOperation(
//            testState.getQueryId(),
//            partitionMap,
//            Collections.singletonList(initiatorFragment),
//            Collections.emptyMap(),
//            Collections.emptyMap(),
//            Collections.emptyMap(),
//            Collections.emptyList()
//        );
//
//        getService(initiator).getInternalService().getOperationHandler().submitLocal(initiatorId, initiatorExecuteOperation);

        return testState;
    }

    private void stopInitiator() {
        initiatorService.getInternalService().getStateRegistry().onQueryCompleted(testState.getQueryId());
    }

    @After
    public void after() {
        if (factory != null) {
            factory.shutdownAll();
        }
    }

    private void send(QueryOperation operation) {
        if (operation instanceof QueryBatchExchangeOperation) {
            channel.submit(operation);
        } else {
            initiatorService.getInternalService().getOperationHandler().submit(
                initiator.getLocalEndpoint().getUuid(),
                participant.getLocalEndpoint().getUuid(),
                operation
            );
        }

        try {
            Thread.sleep(50L);
        } catch (InterruptedException e) {
            // No-op.
        }
    }

    private void checkQueryOnParticipant() {
        assertTrueEventually(() -> {
            assertNotNull(participantService.getInternalService().getStateRegistry().getState(testState.getQueryId()));
        });
    }

    private void checkNoQueryOnParticipant() {
        assertTrueEventually(() -> {
            assertNull(participantService.getInternalService().getStateRegistry().getState(testState.getQueryId()));
        });
    }

    @Test
    public void test_E_B1_B2_C() {
        // EXECUTE
        send(participantExecuteOperation);
        testState.assertStartedEventually();

        // BATCH 1
        send(participantBatch1Operation);
        ListRowBatch batch = testState.assertRowsArrived(BATCH_SIZE);
        checkMonotonicBatch(batch, 0, BATCH_SIZE);

        // BATCH 2
        send(participantBatch2Operation);
        batch = testState.assertRowsArrived(BATCH_SIZE);
        checkMonotonicBatch(batch, BATCH_SIZE, BATCH_SIZE);
        testState.assertCompletedEventually();
        checkNoQueryOnParticipant();

        // CANCEL
        send(participantCancelOperation);
        checkNoQueryOnParticipant();
    }

    @Test
    public void test_E_B1_C_B2() {
        // EXECUTE
        send(participantExecuteOperation);
        testState.assertStartedEventually();

        // BATCH 1
        send(participantBatch1Operation);
        ListRowBatch batch = testState.assertRowsArrived(BATCH_SIZE);
        checkMonotonicBatch(batch, 0, BATCH_SIZE);

        // CANCEL
        send(participantCancelOperation);
        checkNoQueryOnParticipant();

        // BATCH 2
        send(participantBatch2Operation);
        checkQueryOnParticipant();
        stopInitiator();
        checkNoQueryOnParticipant();
        testState.assertNoRows();
    }

    @Test
    public void test_E_C_B1_B2() {
        // EXECUTE
        send(participantExecuteOperation);
        testState.assertStartedEventually();

        // CANCEL
        send(participantCancelOperation);
        checkNoQueryOnParticipant();

        // BATCH 1
        send(participantBatch1Operation);
        checkQueryOnParticipant();

        // BATCH 2
        send(participantBatch2Operation);
        checkQueryOnParticipant();
        stopInitiator();
        checkNoQueryOnParticipant();
        testState.assertNoRows();
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_C_E_B1_B2() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Test
    public void test_B1_E_B2_C() {
        // BATCH 1
        send(participantBatch1Operation);
        checkQueryOnParticipant();

        // EXECUTE
        send(participantExecuteOperation);
        testState.assertStartedEventually();

        // BATCH 2
        send(participantBatch2Operation);
        RowBatch batch = testState.assertRowsArrived(BATCH_SIZE * 2);
        checkMonotonicBatch(batch, 0, BATCH_SIZE * 2);
        testState.assertCompletedEventually();
        checkNoQueryOnParticipant();

        // CANCEL
        send(participantCancelOperation);
        checkNoQueryOnParticipant();
    }

    @Test
    public void test_B1_E_C_B2() {
        // BATCH 1
        send(participantBatch1Operation);
        checkQueryOnParticipant();

        // EXECUTE
        send(participantExecuteOperation);
        testState.assertStartedEventually();
        RowBatch batch = testState.assertRowsArrived(BATCH_SIZE);
        checkMonotonicBatch(batch, 0, BATCH_SIZE);

        // CANCEL
        send(participantCancelOperation);
        checkNoQueryOnParticipant();

        // BATCH 2
        send(participantBatch2Operation);
        checkQueryOnParticipant();
        stopInitiator();
        checkNoQueryOnParticipant();
        testState.assertNoRows();
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_B1_C_E_B2() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_C_B1_E_B2() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Test
    public void test_B1_B2_E_C() {
        // BATCH 1
        send(participantBatch1Operation);
        checkQueryOnParticipant();

        // BATCH 2
        send(participantBatch2Operation);
        checkQueryOnParticipant();

        // EXECUTE
        send(participantExecuteOperation);
        testState.assertStartedEventually();

        // BATCH 2
        RowBatch batch = testState.assertRowsArrived(BATCH_SIZE * 2);
        checkMonotonicBatch(batch, 0, BATCH_SIZE * 2);
        testState.assertCompletedEventually();
        checkNoQueryOnParticipant();

        // CANCEL
        send(participantCancelOperation);
        checkNoQueryOnParticipant();
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_B1_B2_C_E() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_B1_C_B2_E() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_C_B1_B2_E() {
        fail("Cannot handle reordered cancel -> execute");
    }

    private static SqlServiceProxy getService(HazelcastInstance instance) {
        return ((HazelcastInstanceProxy) instance).getOriginal().node.nodeEngine.getSqlService();
    }

    private static void setInternalService(HazelcastInstanceProxy member, long stateCheckFrequency) {
        // Stop the old service.
        getService(member).getInternalService().shutdown();

        // Start the new one.
        NodeEngineImpl nodeEngine = member.getOriginal().node.nodeEngine;

        NodeServiceProviderImpl nodeServiceProvider = new NodeServiceProviderImpl(nodeEngine);

        String instanceName = nodeEngine.getHazelcastInstance().getName();
        InternalSerializationService serializationService = (InternalSerializationService) nodeEngine.getSerializationService();

        SqlInternalService internalService = new SqlInternalService(
            instanceName,
            nodeServiceProvider,
            serializationService,
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors(),
            OUTBOX_BATCH_SIZE,
            stateCheckFrequency
        );

        internalService.start();

        getService(member).setInternalService(internalService);
    }

    private static class InitiatorNode extends ZeroInputPlanNode implements CreateExecPlanNodeVisitorCallback {
        private InitiatorNode() {
            // No-op.
        }

        private InitiatorNode(int id) {
            super(id);
        }

        @Override
        public void visit(PlanNodeVisitor visitor) {
            visitor.onOtherNode(this);
        }

        @Override
        public void onVisit(CreateExecPlanNodeVisitor visitor) {
            visitor.setExec(new InitiatorExec(id));
        }

        @Override
        protected PlanNodeSchema getSchema0() {
            return new PlanNodeSchema(Collections.emptyList());
        }
    }

    private static class ParticipantNode extends UniInputPlanNode implements CreateExecPlanNodeVisitorCallback {
        private ParticipantNode() {
            // No-op.
        }

        private ParticipantNode(int id, PlanNode upstream) {
            super(id, upstream);
        }

        @Override
        protected void visit0(PlanNodeVisitor visitor) {
            visitor.onOtherNode(this);
        }

        @Override
        public void onVisit(CreateExecPlanNodeVisitor visitor) {
            visitor.setExec(new ParticipantExec(id, visitor.pop()));
        }
    }

    private static class InitiatorExec extends AbstractExec {

        public InitiatorExec(int id) {
            super(id);
        }

        @Override
        protected IterationResult advance0() {
            return testState.isKeepInitiator() ? IterationResult.WAIT : IterationResult.FETCHED_DONE;
        }

        @Override
        protected RowBatch currentBatch0() {
            return EmptyRowBatch.INSTANCE;
        }
    }

    private static class ParticipantExec extends AbstractUpstreamAwareExec {

        public ParticipantExec(int id, Exec upstream) {
            super(id, upstream);
        }

        @Override
        protected IterationResult advance0() {
            testState.onAdvance();

            while (true) {
                if (!state.advance()) {
                    return IterationResult.WAIT;
                }

                testState.pushRows(state.consumeBatch());

                if (state.isDone()) {
                    testState.onCompleted();

                    return IterationResult.FETCHED_DONE;
                }
            }
        }

        @Override
        protected RowBatch currentBatch0() {
            return EmptyRowBatch.INSTANCE;
        }
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private static class State {

        private final QueryId queryId;

        private volatile boolean started;
        private volatile int advanceCount;
        private volatile boolean completed;

        private final BlockingQueue<Row> rows = new LinkedBlockingQueue<>();

        private volatile boolean keepInitiator;

        public State(QueryId queryId) {
            this.queryId = queryId;
        }

        public QueryId getQueryId() {
            return queryId;
        }

        private void assertStartedEventually() {
            assertTrueEventually(() -> assertTrue(started));
        }

        private void assertNotStarted() {
            assertFalse(started);
        }

        private void onAdvance() {
            started = true;

            this.advanceCount++;
        }

        private void assertAdvanceCount(int expected) {
            assertEquals(expected, advanceCount);
        }

        private void onCompleted() {
            this.completed = true;
        }

        private void assertCompletedEventually() {
            assertTrueEventually(() -> assertTrue(completed));
        }

        private void assertNotCompleted() {
            assertFalse(completed);
        }

        private void pushRows(RowBatch batch) {
            for (int i = 0; i < batch.getRowCount(); i++) {
                this.rows.add(batch.getRow(i));
            }
        }

        private ListRowBatch assertRowsArrived(int count) {
            assertTrueEventually(() -> assertTrue(rows.size() >= count));

            List<Row> res = new ArrayList<>(count);

            for (int i = 0; i < count; i++) {
                res.add(rows.poll());
            }

            return new ListRowBatch(res);
        }

        private void assertNoRows() {
            assertTrue(rows.isEmpty());
        }

        private boolean isKeepInitiator() {
            return keepInitiator;
        }

        private void doNotKeepInitiator() {
            keepInitiator = false;
        }
    }
}
