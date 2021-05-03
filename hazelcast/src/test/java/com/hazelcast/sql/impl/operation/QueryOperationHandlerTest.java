/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.SqlInternalService;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.exec.AbstractUpstreamAwareExec;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitor;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitorCallback;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.plan.node.UniInputPlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.state.QueryState;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.worker.QueryFragmentExecutable;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for different combinations of events
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryOperationHandlerTest extends SqlTestSupport {

    private static final int EDGE_ID = 1;

    private static final int VALUE_0 = 0;
    private static final int VALUE_1 = 1;

    private static final Duration ASSERT_FALSE_TIMEOUT = Duration.ofMillis(1000L);

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);

    private UUID initiatorId;
    private UUID participantId;

    private SqlInternalService initiatorService;
    private SqlInternalService participantService;

    private Map<UUID, PartitionIdSet> partitionMap;

    private QueryId queryId;

    @Parameterized.Parameter
    public boolean targetIsNotInitiator;

    @Parameterized.Parameters(name = "targetIsNotInitiator:{0}")
    public static Object[] parameters() {
        return new Object[]{true, false};
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Before
    public void before() {
        HazelcastInstanceProxy initiator = (HazelcastInstanceProxy) factory.newHazelcastInstance();
        HazelcastInstanceProxy participant = (HazelcastInstanceProxy) factory.newHazelcastInstance();

        initiatorId = initiator.getLocalEndpoint().getUuid();
        participantId = participant.getLocalEndpoint().getUuid();

        initiatorService = sqlInternalService(initiator);
        participantService = sqlInternalService(participant);

        setStateCheckFrequency(Long.MAX_VALUE);

        partitionMap = new HashMap<>();
        partitionMap.put(initiatorId, new PartitionIdSet(2, Collections.singletonList(1)));
        partitionMap.put(participantId, new PartitionIdSet(2, Collections.singletonList(2)));

        queryId = QueryId.create(initiatorId);
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void test_E() {
        sendExecute(false);
        assertQueryRegisteredEventually(queryId);

        if (targetIsNotInitiator) {
            setOrphanedQueryStateCheckFrequency(100L);
            setStateCheckFrequency(100L);
            assertQueryNotRegisteredEventually(queryId);
        }
    }

    @Test
    public void test_E_B1_B2_ordered() {
        check_E_B1_B2(true);
    }

    @Test
    public void test_E_B1_B2_unordered() {
        check_E_B1_B2(false);
    }

    private void check_E_B1_B2(boolean ordered) {
        sendExecute(ordered);
        QueryState state = assertQueryRegisteredEventually(queryId);

        TestExec exec = assertExecCreatedEventually(state);
        assertFalse(exec.consumed0);
        assertFalse(exec.consumed1);

        sendBatch(VALUE_0);
        assertConsumedEventually(exec, VALUE_0);

        sendBatch(VALUE_1);
        assertConsumedEventually(exec, VALUE_1);

        assertQueryNotRegisteredEventually(queryId);
    }

    @Test
    public void test_E_B2_B1_ordered() {
        check_E_B2_B1(true);
    }

    @Test
    public void test_E_B2_B1_unordered() {
        check_E_B2_B1(false);
    }

    public void check_E_B2_B1(boolean ordered) {
        sendExecute(ordered);

        QueryState state = assertQueryRegisteredEventually(queryId);

        TestExec exec = assertExecCreatedEventually(state);
        assertFalse(exec.consumed0);
        assertFalse(exec.consumed1);

        // Send the second batch, only unordered exec should process it
        sendBatch(VALUE_1);

        if (ordered) {
            assertNotConsumedWithDelay(exec, VALUE_1);
        } else {
            assertConsumedEventually(exec, VALUE_1);
        }

        // Send the first batch, processing should be finished in both modes
        sendBatch(VALUE_0);

        assertConsumedEventually(exec, VALUE_0);

        if (ordered) {
            assertConsumedEventually(exec, VALUE_1);
            assertFalse(exec.reordered);
        } else {
            assertTrue(exec.reordered);
        }

        assertQueryNotRegisteredEventually(queryId);
    }

    @Test
    public void test_E_B_C() {
        sendExecute(false);

        QueryState state = assertQueryRegisteredEventually(queryId);

        TestExec exec = assertExecCreatedEventually(state);
        assertFalse(exec.consumed0);
        assertFalse(exec.consumed1);

        sendBatch(VALUE_0);
        assertConsumedEventually(exec, VALUE_0);

        sendCancel();
        assertQueryNotRegisteredEventually(queryId);
    }

    @Test
    public void test_E_C_B() {
        sendExecute(false);

        QueryState state = assertQueryRegisteredEventually(queryId);

        TestExec exec = assertExecCreatedEventually(state);
        assertFalse(exec.consumed0);
        assertFalse(exec.consumed1);

        sendCancel();
        assertQueryNotRegisteredEventually(queryId);

        sendBatch(VALUE_0);

        if (targetIsNotInitiator) {
            assertQueryRegisteredEventually(queryId);

            setStateCheckFrequency(100L);
        }

        assertQueryNotRegisteredEventually(queryId);
    }

    @Test
    public void test_B() {
        sendBatch(VALUE_0);

        if (targetIsNotInitiator) {
            QueryState state = assertQueryRegisteredEventually(queryId);
            assertExecNotCreatedWithDelay(state);

            setStateCheckFrequency(100L);
        }

        assertQueryNotRegisteredEventually(queryId);
    }

    @Test
    public void test_B_C() {
        sendBatch(VALUE_0);

        if (targetIsNotInitiator) {
            QueryState state = assertQueryRegisteredEventually(queryId);
            assertExecNotCreatedWithDelay(state);

            sendCancel();
        }

        assertQueryNotRegisteredEventually(queryId);
    }

    @Test
    public void test_B1_E_B2_ordered() {
        check_B1_E_B2(true);
    }

    @Test
    public void test_B1_E_B2_unordered() {
        check_B1_E_B2(false);
    }

    private void check_B1_E_B2(boolean ordered) {
        if (targetIsNotInitiator) {
            sendBatch(VALUE_0);
            QueryState state = assertQueryRegisteredEventually(queryId);
            assertExecNotCreatedWithDelay(state);

            sendExecute(ordered);
            TestExec exec = assertExecCreatedEventually(state);
            assertConsumedEventually(exec, VALUE_0);

            sendBatch(VALUE_1);
            assertConsumedEventually(exec, VALUE_1);

            assertQueryNotRegisteredEventually(queryId);
        }
    }

    @Test
    public void test_B2_E_B1_ordered() {
        check_B2_E_B1(true);
    }

    @Test
    public void test_B2_E_B1_unordered() {
        check_B2_E_B1(false);
    }

    private void check_B2_E_B1(boolean ordered) {
        if (targetIsNotInitiator) {
            sendBatch(VALUE_1);
            QueryState state = assertQueryRegisteredEventually(queryId);
            assertExecNotCreatedWithDelay(state);

            sendExecute(ordered);
            TestExec exec = assertExecCreatedEventually(state);

            if (ordered) {
                assertNotConsumedWithDelay(exec, VALUE_1);
            } else {
                assertConsumedEventually(exec, VALUE_1);
            }

            sendBatch(VALUE_0);
            assertConsumedEventually(exec, VALUE_0);

            if (ordered) {
                assertConsumedEventually(exec, VALUE_1);
                assertFalse(exec.reordered);
            } else {
                assertTrue(exec.reordered);
            }

            assertQueryNotRegisteredEventually(queryId);
        }
    }

    @Test
    public void test_B1_B2_E_ordered() {
        check_B1_B2_E(true);
    }

    @Test
    public void test_B1_B2_E_unordered() {
        check_B1_B2_E(false);
    }

    private void check_B1_B2_E(boolean ordered) {
        if (targetIsNotInitiator) {
            sendBatch(VALUE_0);
            QueryState state = assertQueryRegisteredEventually(queryId);
            assertExecNotCreatedWithDelay(state);

            sendBatch(VALUE_1);
            assertExecNotCreatedWithDelay(state);

            sendExecute(ordered);
            TestExec exec = assertExecCreatedEventually(state);
            assertConsumedEventually(exec, VALUE_0);
            assertConsumedEventually(exec, VALUE_1);
            assertFalse(exec.reordered);

            assertQueryNotRegisteredEventually(queryId);
        }
    }

    @Test
    public void test_B2_B1_E_ordered() {
        check_B2_B1_E(true);
    }

    @Test
    public void test_B2_B1_E_unordered() {
        check_B2_B1_E(false);
    }

    private void check_B2_B1_E(boolean ordered) {
        if (targetIsNotInitiator) {
            sendBatch(VALUE_1);
            QueryState state = assertQueryRegisteredEventually(queryId);
            assertExecNotCreatedWithDelay(state);

            sendBatch(VALUE_0);
            assertExecNotCreatedWithDelay(state);

            sendExecute(ordered);
            TestExec exec = assertExecCreatedEventually(state);
            assertConsumedEventually(exec, VALUE_0);
            assertConsumedEventually(exec, VALUE_1);

            if (ordered) {
                assertFalse(exec.reordered);
            } else {
                assertTrue(exec.reordered);
            }

            assertQueryNotRegisteredEventually(queryId);
        }
    }

    @Test
    public void test_C() {
        sendCancel();

        if (targetIsNotInitiator) {
            QueryState state = assertQueryRegisteredEventually(queryId);
            assertTrue(state.isCancelled());

            setStateCheckFrequency(100L);
        }

        assertQueryNotRegisteredEventually(queryId);
    }

    @Test
    public void test_C_E() {
        sendCancel();

        if (targetIsNotInitiator) {
            QueryState state = assertQueryRegisteredEventually(queryId);
            assertTrue(state.isCancelled());

            sendExecute(false);
        }

        assertQueryNotRegisteredEventually(queryId);
    }

    @Test
    public void test_C_B_E() {
        sendCancel();

        if (targetIsNotInitiator) {
            QueryState state = assertQueryRegisteredEventually(queryId);
            assertTrue(state.isCancelled());

            sendBatch(VALUE_0);
            assertExecNotCreatedWithDelay(state);

            sendExecute(false);
        }

        assertQueryNotRegisteredEventually(queryId);
    }

    private void sendExecute(boolean ordered) {
        if (!targetIsNotInitiator) {
            // Initiator must register the state in advance.
            Plan plan = new Plan(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                null,
                QueryParameterMetadata.EMPTY,
                null,
                Collections.emptySet(),
                Collections.emptyList()
            );

            QueryState state = initiatorService.getStateRegistry().onInitiatorQueryStarted(
                QueryId.create(initiatorId),
                initiatorId,
                Long.MAX_VALUE,
                plan,
                null,
                null,
                TestQueryResultProducer.INSTANCE,
                initiatorService.getOperationHandler()
            );

            this.queryId = state.getQueryId();
        }

        send(initiatorId, targetId(), createExecuteOperation(targetId(), ordered));
    }

    private void sendBatch(int value) {
        UUID sourceId = targetId() == initiatorId ? participantId : initiatorId;

        send(sourceId, targetId(), createBatchOperation(targetId(), value));
    }

    private void sendCancel() {
        send(initiatorId, targetId(), createCancelOperation(initiatorId));
    }

    private void send(UUID sourceMemberId, UUID targetMemberId, QueryOperation operation) {
        SqlInternalService sourceService = sourceMemberId.equals(initiatorId) ? initiatorService : participantService;

        sourceService.getOperationHandler().submit(
            sourceMemberId,
            targetMemberId,
            operation
        );
    }

    private QueryExecuteOperation createExecuteOperation(UUID targetMemberId, boolean ordered) {
        TestNode node = new TestNode(
            1, new ReceivePlanNode(2, EDGE_ID, ordered, Collections.singletonList(QueryDataType.INT))
        );

        QueryExecuteOperationFragment fragment = new QueryExecuteOperationFragment(
            node,
            QueryExecuteOperationFragmentMapping.EXPLICIT,
            Collections.singletonList(targetMemberId)
        );

        return new QueryExecuteOperation(
            queryId,
            partitionMap,
            Collections.singletonList(fragment),
            Collections.singletonMap(EDGE_ID, 0),
            Collections.singletonMap(EDGE_ID, 0),
            Collections.singletonMap(EDGE_ID, Long.MAX_VALUE),
            Collections.emptyList()
        );
    }

    private QueryBatchExchangeOperation createBatchOperation(UUID targetMemberId, int value) {
        long ordinal = value;
        boolean last = value == 1;
        ListRowBatch rows = createMonotonicBatch(value, 1);

        return new QueryBatchExchangeOperation(
            queryId,
            EDGE_ID,
            targetMemberId,
            rows,
            ordinal,
            last,
            Long.MAX_VALUE
        );
    }

    private QueryCancelOperation createCancelOperation(UUID sourceMemberId) {
        return new QueryCancelOperation(
            queryId,
            SqlErrorCode.GENERIC,
            "Error",
            sourceMemberId
        );
    }

    private UUID targetId() {
        return targetIsNotInitiator ? participantId : initiatorId;
    }

    private SqlInternalService targetService() {
        return targetIsNotInitiator ? participantService : initiatorService;
    }

    private void setOrphanedQueryStateCheckFrequency(long frequency) {
        initiatorService.getStateRegistryUpdater().setOrphanedQueryStateCheckFrequency(frequency);
        participantService.getStateRegistryUpdater().setOrphanedQueryStateCheckFrequency(frequency);
    }

    private void setStateCheckFrequency(long frequency) {
        initiatorService.getStateRegistryUpdater().setStateCheckFrequency(frequency);
        participantService.getStateRegistryUpdater().setStateCheckFrequency(frequency);
    }

    private TestExec assertExecCreatedEventually(QueryState state) {
        return assertTrueEventually(() -> {
            Set<QueryFragmentExecutable> fragments = state.getDistributedState().getFragments();

            assertFalse(fragments.isEmpty());

            return (TestExec) fragments.iterator().next().getExec();
        });
    }

    private void assertExecNotCreatedWithDelay(QueryState state) {
        assertTrueDelayed((int) ASSERT_FALSE_TIMEOUT.getSeconds(), () -> {
            Set<QueryFragmentExecutable> fragments = state.getDistributedState().getFragments();

            assertTrue(fragments.isEmpty());
        });
    }

    private QueryState assertQueryRegisteredEventually(QueryId queryId) {
        return assertTrueEventually(() -> {
            QueryState state0 = targetService().getStateRegistry().getState(queryId);

            assertNotNull(state0);

            return state0;
        });
    }

    private void assertQueryNotRegisteredEventually(QueryId queryId) {
        assertTrueEventually(() -> {
            QueryState state0 = targetService().getStateRegistry().getState(queryId);

            assertNull(state0);
        }, ASSERT_FALSE_TIMEOUT.toMillis());
    }

    private void assertConsumedEventually(TestExec exec, int value) {
        assert value == VALUE_0 || value == VALUE_1;

        if (value == VALUE_0) {
            assertTrueEventually(() -> assertTrue(exec.consumed0));
        } else {
            assertTrueEventually(() -> assertTrue(exec.consumed1));
        }
    }

    private void assertNotConsumedWithDelay(TestExec exec, int value) {
        assert value == VALUE_0 || value == VALUE_1;

        if (value == VALUE_0) {
            assertTrueDelayed((int) ASSERT_FALSE_TIMEOUT.getSeconds(), () -> assertFalse(exec.consumed0));
        } else {
            assertTrueDelayed((int) ASSERT_FALSE_TIMEOUT.getSeconds(), () -> assertFalse(exec.consumed1));
        }
    }

    private static <T> T assertTrueEventually(Supplier<T> task) {
        AtomicReference<T> resRef = new AtomicReference<>();

        assertTrueEventually(() -> {
            T res = task.get();

            resRef.set(res);
        });

        return resRef.get();
    }

    private static class TestNode extends UniInputPlanNode implements CreateExecPlanNodeVisitorCallback {
        @SuppressWarnings("unused")
        private TestNode() {
            // No-op.
        }

        private TestNode(int id, PlanNode upstream) {
            super(id, upstream);
        }

        @Override
        protected void visit0(PlanNodeVisitor visitor) {
            visitor.onOtherNode(this);
        }

        @Override
        public void onVisit(CreateExecPlanNodeVisitor visitor) {
            visitor.setExec(new TestExec(id, visitor.pop()));
        }
    }

    private static class TestExec extends AbstractUpstreamAwareExec {

        private boolean consumed0;
        private boolean consumed1;

        private boolean reordered;

        private TestExec(int id, Exec upstream) {
            super(id, upstream);
        }

        @Override
        protected IterationResult advance0() {
            while (true) {
                if (!state.advance()) {
                    return IterationResult.WAIT;
                }

                RowBatch batch = state.consumeBatch();

                for (int i = 0; i < batch.getRowCount(); i++) {
                    Integer value = batch.getRow(i).get(0, false);

                    if (value == VALUE_0) {
                        consumed0 = true;
                    } else {
                        assert value == VALUE_1;

                        consumed1 = true;

                        if (!consumed0) {
                            reordered = true;
                        }
                    }
                }

                if (state.isDone()) {
                    return IterationResult.FETCHED_DONE;
                }
            }
        }

        @Override
        protected RowBatch currentBatch0() {
            return EmptyRowBatch.INSTANCE;
        }
    }
}
