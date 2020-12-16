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
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlInternalService;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.exec.AbstractUpstreamAwareExec;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitor;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitorCallback;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.plan.node.UniInputPlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Tests for different combinations of events
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryOperationHandlerTest2 extends SqlTestSupport {

    private static final int EDGE_ID = 1;
    private static final long STATE_CHECK_FREQUENCY = 100L;

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);

    private HazelcastInstanceProxy initiator;
    private HazelcastInstanceProxy participant;

    private UUID initiatorId;
    private UUID participantId;

    private SqlInternalService initiatorService;
    private SqlInternalService participantService;

    private Map<UUID, PartitionIdSet> partitionMap;

    private QueryId queryId;

    @Before
    public void before() {
        initiator = (HazelcastInstanceProxy) factory.newHazelcastInstance();
        participant = (HazelcastInstanceProxy) factory.newHazelcastInstance();

        initiatorId = initiator.getLocalEndpoint().getUuid();
        participantId = participant.getLocalEndpoint().getUuid();

        initiatorService = sqlInternalService(initiator);
        participantService = sqlInternalService(participant);

        initiatorService.setStateCheckFrequency(STATE_CHECK_FREQUENCY);
        participantService.setStateCheckFrequency(STATE_CHECK_FREQUENCY);

        partitionMap = new HashMap<>();
        partitionMap.put(initiatorId, new PartitionIdSet(2, Collections.singletonList(1)));
        partitionMap.put(participantId, new PartitionIdSet(1, Collections.singletonList(2)));

        queryId = QueryId.create(initiatorId);
    }

    @Test
    public void test() {
        send(initiatorId, participantId, createExecuteOperation(participantId));
    }

    private void send(UUID sourceMemberId, UUID targetMemberId, QueryOperation operation) {
        SqlInternalService sourceService = sourceMemberId.equals(initiatorId) ? initiatorService : participantService;

        sourceService.getOperationHandler().submit(
            sourceMemberId,
            targetMemberId,
            operation
        );
    }

    private QueryExecuteOperation createExecuteOperation(UUID targetMemberId) {
        TestNode node = new TestNode(
            1, new ReceivePlanNode(2, EDGE_ID, Collections.singletonList(QueryDataType.INT))
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

    private static class TestNode extends UniInputPlanNode implements CreateExecPlanNodeVisitorCallback {
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

        private boolean consumed1;
        private boolean consumed2;

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
                    Integer value = batch.getRow(i).get(0);

                    if (value == 1) {
                        consumed1 = true;
                    } else {
                        assert value == 2;

                        consumed2 = true;
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
