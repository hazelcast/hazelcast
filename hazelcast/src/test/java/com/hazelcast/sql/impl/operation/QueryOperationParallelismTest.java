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
import com.hazelcast.sql.impl.SqlInternalService;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.exec.AbstractExec;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitor;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitorCallback;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.PlanFragmentMapping;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.plan.node.ZeroInputPlanNode;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test to ensure that query operation parallelism works as expected, properly distributing tasks between fragment and system
 * pools.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(ParallelJVMTest.class)
public class QueryOperationParallelismTest extends SqlTestSupport {

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);

    private UUID memberId;
    private SqlInternalService memberService;


    @Before
    public void before() {
        Config config = smallInstanceConfig();

        config.getSqlConfig().setExecutorPoolSize(2);

        HazelcastInstanceProxy member = (HazelcastInstanceProxy) factory.newHazelcastInstance();

        memberId = member.getLocalEndpoint().getUuid();
        memberService = sqlInternalService(member);
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testVerticalParallelism() throws Exception {
        TestNode node1 = new TestNode(1);
        TestNode node2 = new TestNode(2);

        try {
            List<PlanNode> nodes = Arrays.asList(node1, node2);

            startQuery(nodes);

            assert node1.startLatch.await(1, TimeUnit.SECONDS);
            assert node2.startLatch.await(1, TimeUnit.SECONDS);
        } finally {
            node1.stopLatch.countDown();
            node2.stopLatch.countDown();
        }
    }

    private void startQuery(List<PlanNode> nodes) {
        assert !nodes.isEmpty();

        List<PlanFragmentMapping> mappings = new ArrayList<>(nodes.size());

        for (int i = 0; i < nodes.size(); i++) {
            mappings.add(new PlanFragmentMapping(Collections.singleton(memberId), true));
        }

        Plan plan = new Plan(
            Collections.singletonMap(memberId, new PartitionIdSet(1, 1)),
            nodes,
            mappings,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null,
            QueryParameterMetadata.EMPTY,
            null,
            Collections.emptySet(),
            Collections.emptyList()
        );

        memberService.execute(
            QueryId.create(UUID.randomUUID()),
            plan,
            Collections.emptyList(),
            Long.MAX_VALUE,
            Integer.MAX_VALUE,
            plan1 -> { }
        );
    }

    private static final class TestNode extends ZeroInputPlanNode implements CreateExecPlanNodeVisitorCallback {

        private final CountDownLatch startLatch;
        private final CountDownLatch stopLatch;

        private TestNode(int id) {
            super(id);

            this.startLatch = new CountDownLatch(1);
            this.stopLatch = new CountDownLatch(1);
        }

        @Override
        public void visit(PlanNodeVisitor visitor) {
            visitor.onOtherNode(this);
        }

        @Override
        public void onVisit(CreateExecPlanNodeVisitor visitor) {
            visitor.setExec(new TestExec(id, startLatch, stopLatch));
        }

        @Override
        protected PlanNodeSchema getSchema0() {
            return new PlanNodeSchema(Collections.singletonList(QueryDataType.INT));
        }
    }

    private static final class TestExec extends AbstractExec {

        private final CountDownLatch startLatch;
        private final CountDownLatch stopLatch;

        private TestExec(int id,  CountDownLatch startLatch, CountDownLatch stopLatch) {
            super(id);

            this.startLatch = startLatch;
            this.stopLatch = stopLatch;
        }

        @Override
        protected IterationResult advance0() {
            startLatch.countDown();

            try {
                while (!stopLatch.await(100, TimeUnit.MILLISECONDS)) {
                    checkCancelled();
                }

                return IterationResult.FETCHED_DONE;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new RuntimeException("Interrupted", e);
            }
        }

        @Override
        protected RowBatch currentBatch0() {
            return EmptyRowBatch.INSTANCE;
        }
    }
}
