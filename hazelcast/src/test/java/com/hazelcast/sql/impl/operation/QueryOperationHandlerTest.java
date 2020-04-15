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
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.exec.AbstractUpstreamAwareExec;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitor;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitorCallback;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.exec.root.BlockingRootResultConsumer;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.plan.node.UniInputPlanNode;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
 *     <li>L - leave of the other member</li>
 * </ul>
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryOperationHandlerTest extends SqlTestSupport {

    private static final int EDGE_ID = 1;
    private static final int BATCH_SIZE = 100;

    private static final long STATE_CHECK_FREQUENCY = 100L;

    private static volatile State testState;

    private TestHazelcastInstanceFactory factory;

    private HazelcastInstanceProxy initiator;
    private HazelcastInstanceProxy participant;

    private UUID initiatorId;
    private UUID participantId;

    private SqlInternalService initiatorService;
    private SqlInternalService participantService;

    private Map<UUID, PartitionIdSet> partitionMap;

    private QueryExecuteOperation initiatorExecuteOperation;
    private QueryBatchExchangeOperation initiatorBatch1Operation;
    private QueryBatchExchangeOperation initiatorBatch2Operation;
    private QueryCancelOperation initiatorCancelOperation;

    private QueryExecuteOperation participantExecuteOperation;
    private QueryBatchExchangeOperation participantBatch1Operation;
    private QueryBatchExchangeOperation participantBatch2Operation;
    private QueryCancelOperation participantCancelOperation;

    private QueryOperationChannel toInitiatorChannel;
    private QueryOperationChannel toParticipantChannel;

    @Before
    public void before() {
        factory = new TestHazelcastInstanceFactory(2);

        initiator = (HazelcastInstanceProxy) factory.newHazelcastInstance();
        participant = (HazelcastInstanceProxy) factory.newHazelcastInstance();

        initiatorId = initiator.getLocalEndpoint().getUuid();
        participantId = participant.getLocalEndpoint().getUuid();

        initiatorService = setInternalService(initiator, STATE_CHECK_FREQUENCY);
        participantService = setInternalService(participant, STATE_CHECK_FREQUENCY);

        partitionMap = new HashMap<>();
        partitionMap.put(initiatorId, new PartitionIdSet(2, Collections.singletonList(1)));
        partitionMap.put(participantId, new PartitionIdSet(1, Collections.singletonList(2)));

        // Start the query with maximum timeout by default.
        prepare(Long.MAX_VALUE);
    }

    private void prepare(long timeout) {
        testState = startQueryOnInitiator(timeout);

        initiatorExecuteOperation = createExecuteOperation(initiatorId);
        participantExecuteOperation = createExecuteOperation(participantId);

        initiatorBatch1Operation = createBatch1Operation(initiatorId);
        participantBatch1Operation = createBatch1Operation(participantId);

        initiatorBatch2Operation = createBatch2Operation(initiatorId);
        participantBatch2Operation = createBatch2Operation(participantId);

        initiatorCancelOperation = createCancelOperation(participantId);
        participantCancelOperation = createCancelOperation(initiatorId);

        toInitiatorChannel = participantService.getOperationHandler().createChannel(participantId, initiatorId);
        toParticipantChannel = initiatorService.getOperationHandler().createChannel(initiatorId, participantId);
    }

    @After
    public void after() {
        if (factory != null) {
            factory.shutdownAll();
        }
    }

    @Test
    public void test_initiator_timeout() {
        stopQueryOnInitiator();
        prepare(50L);

        sendToInitiator(initiatorExecuteOperation);
        checkNoQueryOnInitiator();
    }

    @Test
    public void test_initiator_E_B1_B2_C() {
        // EXECUTE
        sendToInitiator(initiatorExecuteOperation);
        testState.assertStartedEventually();

        // BATCH 1
        sendToInitiator(initiatorBatch1Operation);
        ListRowBatch batch = testState.assertRowsArrived(BATCH_SIZE);
        checkMonotonicBatch(batch, 0, BATCH_SIZE);

        // BATCH 2
        sendToInitiator(initiatorBatch2Operation);
        batch = testState.assertRowsArrived(BATCH_SIZE);
        checkMonotonicBatch(batch, BATCH_SIZE, BATCH_SIZE);
        testState.assertCompletedEventually();
        checkNoQueryOnInitiator();

        // CANCEL
        sendToInitiator(initiatorCancelOperation);
        checkNoQueryOnInitiator();
    }

    @Test
    public void test_initiator_E_B1_C_B2() {
        // EXECUTE
        sendToInitiator(initiatorExecuteOperation);
        testState.assertStartedEventually();

        // BATCH 1
        sendToInitiator(initiatorBatch1Operation);
        ListRowBatch batch = testState.assertRowsArrived(BATCH_SIZE);
        checkMonotonicBatch(batch, 0, BATCH_SIZE);

        // CANCEL
        sendToInitiator(initiatorCancelOperation);
        checkNoQueryOnInitiator();

        // BATCH 2
        sendToInitiator(initiatorBatch2Operation);
        checkNoQueryOnInitiator();
        testState.assertNoRows();
    }

    @Test
    public void test_initiator_E_C_B1_B2() {
        // EXECUTE
        sendToInitiator(initiatorExecuteOperation);
        testState.assertStartedEventually();

        // CANCEL
        sendToInitiator(initiatorCancelOperation);
        checkNoQueryOnInitiator();

        // BATCH 1
        sendToInitiator(initiatorBatch1Operation);
        checkNoQueryOnInitiator();

        // BATCH 2
        sendToInitiator(initiatorBatch2Operation);
        checkNoQueryOnInitiator();
        testState.assertNoRows();
    }

    @Test
    public void test_initiator_E_L_B() {
        // EXECUTE
        sendToInitiator(initiatorExecuteOperation);
        testState.assertStartedEventually();

        // LEAVE
        participant.shutdown();
        checkNoQueryOnInitiator();

        // BATCH
        sendToInitiator(initiatorBatch1Operation);
        checkNoQueryOnInitiator();
    }

    @Test
    public void test_participant_E_B1_B2_C() {
        // EXECUTE
        sendToParticipant(participantExecuteOperation);
        testState.assertStartedEventually();

        // BATCH 1
        sendToParticipant(participantBatch1Operation);
        ListRowBatch batch = testState.assertRowsArrived(BATCH_SIZE);
        checkMonotonicBatch(batch, 0, BATCH_SIZE);

        // BATCH 2
        sendToParticipant(participantBatch2Operation);
        batch = testState.assertRowsArrived(BATCH_SIZE);
        checkMonotonicBatch(batch, BATCH_SIZE, BATCH_SIZE);
        testState.assertCompletedEventually();
        checkNoQueryOnParticipant();

        // CANCEL
        sendToParticipant(participantCancelOperation);
        checkNoQueryOnParticipant();
    }

    @Test
    public void test_participant_E_B1_C_B2() {
        // EXECUTE
        sendToParticipant(participantExecuteOperation);
        testState.assertStartedEventually();

        // BATCH 1
        sendToParticipant(participantBatch1Operation);
        ListRowBatch batch = testState.assertRowsArrived(BATCH_SIZE);
        checkMonotonicBatch(batch, 0, BATCH_SIZE);

        // CANCEL
        sendToParticipant(participantCancelOperation);
        checkNoQueryOnParticipant();

        // BATCH 2
        sendToParticipant(participantBatch2Operation);
        checkQueryOnParticipant();
        stopQueryOnInitiator();
        checkNoQueryOnParticipant();
        testState.assertNoRows();
    }

    @Test
    public void test_participant_E_C_B1_B2() {
        // EXECUTE
        sendToParticipant(participantExecuteOperation);
        testState.assertStartedEventually();

        // CANCEL
        sendToParticipant(participantCancelOperation);
        checkNoQueryOnParticipant();

        // BATCH 1
        sendToParticipant(participantBatch1Operation);
        checkQueryOnParticipant();

        // BATCH 2
        sendToParticipant(participantBatch2Operation);
        checkQueryOnParticipant();
        stopQueryOnInitiator();
        checkNoQueryOnParticipant();
        testState.assertNoRows();
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_participant_C_E_B1_B2() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Test
    public void test_participant_B1_E_B2_C() {
        // BATCH 1
        sendToParticipant(participantBatch1Operation);
        checkQueryOnParticipant();

        // EXECUTE
        sendToParticipant(participantExecuteOperation);
        testState.assertStartedEventually();

        // BATCH 2
        sendToParticipant(participantBatch2Operation);
        RowBatch batch = testState.assertRowsArrived(BATCH_SIZE * 2);
        checkMonotonicBatch(batch, 0, BATCH_SIZE * 2);
        testState.assertCompletedEventually();
        checkNoQueryOnParticipant();

        // CANCEL
        sendToParticipant(participantCancelOperation);
        checkNoQueryOnParticipant();
    }

    @Test
    public void test_participant_B1_E_C_B2() {
        // BATCH 1
        sendToParticipant(participantBatch1Operation);
        checkQueryOnParticipant();

        // EXECUTE
        sendToParticipant(participantExecuteOperation);
        testState.assertStartedEventually();
        RowBatch batch = testState.assertRowsArrived(BATCH_SIZE);
        checkMonotonicBatch(batch, 0, BATCH_SIZE);

        // CANCEL
        sendToParticipant(participantCancelOperation);
        checkNoQueryOnParticipant();

        // BATCH 2
        sendToParticipant(participantBatch2Operation);
        checkQueryOnParticipant();
        stopQueryOnInitiator();
        checkNoQueryOnParticipant();
        testState.assertNoRows();
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_participant_B1_C_E_B2() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_participant_C_B1_E_B2() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Test
    public void test_participant_B1_B2_E_C() {
        // BATCH 1
        sendToParticipant(participantBatch1Operation);
        checkQueryOnParticipant();

        // BATCH 2
        sendToParticipant(participantBatch2Operation);
        checkQueryOnParticipant();

        // EXECUTE
        sendToParticipant(participantExecuteOperation);
        testState.assertStartedEventually();

        // BATCH 2
        RowBatch batch = testState.assertRowsArrived(BATCH_SIZE * 2);
        checkMonotonicBatch(batch, 0, BATCH_SIZE * 2);
        testState.assertCompletedEventually();
        checkNoQueryOnParticipant();

        // CANCEL
        sendToParticipant(participantCancelOperation);
        checkNoQueryOnParticipant();
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_participant_B1_B2_C_E() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_participant_B1_C_B2_E() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_participant_C_B1_B2_E() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Test
    public void test_participant_E_L_B() {
        // EXECUTE
        sendToParticipant(participantExecuteOperation);
        testState.assertStartedEventually();

        // LEAVE
        initiator.shutdown();
        checkNoQueryOnParticipant();

        // BATCH
        sendToParticipant(participantBatch1Operation);
        checkNoQueryOnParticipant();
    }

    private QueryExecuteOperation createExecuteOperation(UUID toMemberId) {
        PlanNode node = new ParticipantNode(
            1, new ReceivePlanNode(2, EDGE_ID, Collections.singletonList(QueryDataType.INT))
        );

        QueryExecuteOperationFragment fragment = new QueryExecuteOperationFragment(
            node,
            QueryExecuteOperationFragmentMapping.EXPLICIT,
            Collections.singletonList(toMemberId)
        );

        return new QueryExecuteOperation(
            testState.getQueryId(),
            partitionMap,
            Collections.singletonList(fragment),
            Collections.singletonMap(EDGE_ID, 0),
            Collections.singletonMap(EDGE_ID, 0),
            Collections.singletonMap(EDGE_ID, Long.MAX_VALUE),
            Collections.emptyList()
        );
    }

    private QueryBatchExchangeOperation createBatch1Operation(UUID toMemberId) {
        return new QueryBatchExchangeOperation(
            testState.getQueryId(),
            EDGE_ID,
            toMemberId,
            createMonotonicBatch(0, BATCH_SIZE),
            false,
            Long.MAX_VALUE
        );
    }

    private QueryBatchExchangeOperation createBatch2Operation(UUID toMemberId) {
        return new QueryBatchExchangeOperation(
            testState.getQueryId(),
            EDGE_ID,
            toMemberId,
            createMonotonicBatch(BATCH_SIZE, BATCH_SIZE),
            true,
            Long.MAX_VALUE
        );
    }

    private QueryCancelOperation createCancelOperation(UUID fromMemberId) {
        return new QueryCancelOperation(
            testState.getQueryId(),
            SqlErrorCode.GENERIC,
            "Error",
            fromMemberId
        );
    }

    private State startQueryOnInitiator(long timeout) {
        Plan plan = new Plan(
            partitionMap,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        QueryId queryId = initiatorService.getStateRegistry().onInitiatorQueryStarted(
            initiatorId,
            timeout,
            plan,
            new BlockingRootResultConsumer(),
            initiatorService.getOperationHandler(),
            true
        ).getQueryId();

        testState = new State(queryId);

        return testState;
    }

    private void stopQueryOnInitiator() {
        initiatorService.getStateRegistry().onQueryCompleted(testState.getQueryId());
    }

    private void sendToInitiator(QueryOperation operation) {
        send(participantId, initiatorId, operation);
    }

    private void sendToParticipant(QueryOperation operation) {
        send(initiatorId, participantId, operation);
    }

    private void send(UUID fromId, UUID toId, QueryOperation operation) {
        SqlInternalService fromService = fromId.equals(initiatorId) ? initiatorService : participantService;
        QueryOperationChannel toChannel = toId.equals(initiatorId) ? toInitiatorChannel : toParticipantChannel;

        if (operation instanceof QueryBatchExchangeOperation) {
            toChannel.submit(operation);
        } else {
            fromService.getOperationHandler().submit(
                fromId,
                toId,
                operation
            );
        }

        try {
            Thread.sleep(50L);
        } catch (InterruptedException e) {
            // No-op.
        }
    }

    private void checkNoQueryOnInitiator() {
        checkNoQuery(initiatorService);
    }

    private void checkQueryOnParticipant() {
        checkQuery(participantService);
    }

    private void checkNoQueryOnParticipant() {
        checkNoQuery(participantService);
    }

    private void checkQuery(SqlInternalService service) {
        assertTrueEventually(() -> assertNotNull(service.getStateRegistry().getState(testState.getQueryId())));
    }

    private void checkNoQuery(SqlInternalService service) {
        assertTrueEventually(() -> assertNull(service.getStateRegistry().getState(testState.getQueryId())));
    }

    private static SqlServiceImpl getService(HazelcastInstance instance) {
        return ((HazelcastInstanceProxy) instance).getOriginal().node.nodeEngine.getSqlService();
    }

    private static SqlInternalService setInternalService(HazelcastInstanceProxy member, long stateCheckFrequency) {
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
            1000,
            stateCheckFrequency
        );

        internalService.start();

        getService(member).setInternalService(internalService);

        return internalService;
    }

    @SuppressWarnings("unused")
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

    private static class ParticipantExec extends AbstractUpstreamAwareExec {

        private ParticipantExec(int id, Exec upstream) {
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

    private static class State {

        private final QueryId queryId;

        private volatile boolean started;
        private volatile boolean completed;

        private final BlockingQueue<Row> rows = new LinkedBlockingQueue<>();

        private State(QueryId queryId) {
            this.queryId = queryId;
        }

        public QueryId getQueryId() {
            return queryId;
        }

        private void assertStartedEventually() {
            assertTrueEventually(() -> assertTrue(started));
        }

        private void onAdvance() {
            started = true;
        }

        private void onCompleted() {
            this.completed = true;
        }

        private void assertCompletedEventually() {
            assertTrueEventually(() -> assertTrue(completed));
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
    }
}
