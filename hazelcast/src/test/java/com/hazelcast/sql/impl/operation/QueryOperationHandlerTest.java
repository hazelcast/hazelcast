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
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlServiceProxy;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.exec.AbstractExec;
import com.hazelcast.sql.impl.exec.AbstractUpstreamAwareExec;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitor;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitorCallback;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.plan.node.UniInputPlanNode;
import com.hazelcast.sql.impl.plan.node.ZeroInputPlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryOperationHandlerTest extends SqlTestSupport {

    private static final int EDGE_ID = 1;
    private static final int BATCH_SIZE = 100;

    private static TestHazelcastInstanceFactory factory;

    private static HazelcastInstanceProxy initiator;
    private static HazelcastInstanceProxy participant;

    private static SqlServiceProxy initiatorService;
    private static SqlServiceProxy participantService;

    private static State testState;

    private QueryExecuteOperation participantExecuteOperation;
    private QueryBatchExchangeOperation participantBatch1Operation;
    private QueryBatchExchangeOperation participantBatch2Operation;
    private QueryCancelOperation participantCancelOperation;

    @BeforeClass
    public static void beforeClass() {
        factory = new TestHazelcastInstanceFactory(2);

        initiator = (HazelcastInstanceProxy) factory.newHazelcastInstance();
        participant = (HazelcastInstanceProxy) factory.newHazelcastInstance();

        initiatorService = getService(initiator);
        participantService = getService(participant);
    }

    @AfterClass
    public static void afterClass() {
        if (factory != null) {
            factory.shutdownAll();
        }
    }

    @Before
    public void before() {
        UUID initiatorId = initiator.getLocalEndpoint().getUuid();
        UUID participantId = participant.getLocalEndpoint().getUuid();

        testState = new State(initiatorId);

        // Start query on initiator.
        Map<UUID, PartitionIdSet> partitionMap = new HashMap<>();
        partitionMap.put(initiatorId, new PartitionIdSet(2, Collections.singletonList(1)));
        partitionMap.put(initiatorId, new PartitionIdSet(1, Collections.singletonList(2)));

        Plan plan = new Plan(
            partitionMap,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        PlanNode initiatorNode = new InitiatorNode(1);

        QueryExecuteOperationFragment initiatorFragment = new QueryExecuteOperationFragment(
            initiatorNode,
            QueryExecuteOperationFragmentMapping.EXPLICIT,
            Collections.singletonList(initiatorId)
        );

        QueryExecuteOperation initiatorExecuteOperation = new QueryExecuteOperation(
            testState.getQueryId(),
            partitionMap,
            Collections.singletonList(initiatorFragment),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyList()
        );

        getService(initiator).getInternalService().getOperationHandler().submitLocal(initiatorId, initiatorExecuteOperation);

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
            createMonotonicBatch(BATCH_SIZE, BATCH_SIZE * 2),
            true,
            Long.MAX_VALUE
        );

        participantCancelOperation = new QueryCancelOperation(
            testState.getQueryId(),
            SqlErrorCode.GENERIC,
            "Error",
            initiatorId
        );
    }

    @After
    public void after() {
        triggerNoKeepAlive();
        awaitClean();
    }

    @Test
    public void test_E_B1_B2_C() {
        triggerKeepAlive();
        triggerExecute();
        testState.assertStartedEventually();

        sendBatch(BATCH_SIZE, false);
        awaitRows(BATCH_SIZE);

        sendBatch(BATCH_SIZE, true);
        awaitRows(BATCH_SIZE);
        awaitCompletion();

        sendCancel();
        awaitClean();



        // TODO
    }

    @Test
    public void test_E_B1_C_B2() {
        // TODO
    }

    @Test
    public void test_E_C_B1_B2() {
        // TODO
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_C_E_B1_B2() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Test
    public void test_B1_E_B2_C() {
        // TODO
    }

    @Test
    public void test_B1_E_C_B2() {
        // TODO
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
        // TODO
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

    private static class InitiatorNode extends ZeroInputPlanNode implements CreateExecPlanNodeVisitorCallback {

        public InitiatorNode(int id) {
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

        public ParticipantNode(int id, PlanNode upstream) {
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

        public State(UUID initiatorMemberId) {
            this.queryId = QueryId.create(initiatorMemberId);
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

        private List<Row> assertRowsArrived(int count) {
            assertTrueEventually(() -> assertTrue(rows.size() >= count));

            List<Row> res = new ArrayList<>(count);

            for (int i = 0; i < count; i++) {
                res.add(rows.poll());
            }

            return res;
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
