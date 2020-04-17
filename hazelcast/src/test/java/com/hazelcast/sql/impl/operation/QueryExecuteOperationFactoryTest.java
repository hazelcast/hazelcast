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
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.PlanFragmentMapping;
import com.hazelcast.sql.impl.plan.node.MockPlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.sql.impl.operation.QueryExecuteOperationFragmentMapping.DATA_MEMBERS;
import static com.hazelcast.sql.impl.operation.QueryExecuteOperationFragmentMapping.EXPLICIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryExecuteOperationFactoryTest {
    @Test
    public void testExecuteOperationFactory() {
        UUID member1 = UUID.randomUUID();
        UUID member2 = UUID.randomUUID();

        Map<UUID, PartitionIdSet> partitionMap = new HashMap<>();
        partitionMap.put(member1, new PartitionIdSet(2, Collections.singletonList(1)));
        partitionMap.put(member2, new PartitionIdSet(2, Collections.singletonList(2)));

        PlanNode node1 = MockPlanNode.create(1);
        PlanNode node2 = MockPlanNode.create(2);
        List<PlanNode> fragments = Arrays.asList(node1, node2);

        PlanFragmentMapping mapping1 = new PlanFragmentMapping(null, true);
        PlanFragmentMapping mapping2 = new PlanFragmentMapping(Collections.singletonList(member2), false);
        List<PlanFragmentMapping> fragmentMappings = Arrays.asList(mapping1, mapping2);

        Map<Integer, Integer> outboundEdgeMap = Collections.singletonMap(1, 0);
        Map<Integer, Integer> inboundEdgeMap = Collections.singletonMap(1, 1);
        Map<Integer, Integer> inboundEdgeMemberCountMap = Collections.singletonMap(1, 2);

        Plan plan = new Plan(
            partitionMap,
            fragments,
            fragmentMappings,
            outboundEdgeMap,
            inboundEdgeMap,
            inboundEdgeMemberCountMap
        );

        QueryId queryId = QueryId.create(UUID.randomUUID());
        List<Object> args = Collections.singletonList(1);
        Map<Integer, Long> edgeInitialMemoryMap = Collections.singletonMap(1, 1000L);

        QueryExecuteOperationFactory factory = new QueryExecuteOperationFactory(plan, args, edgeInitialMemoryMap);

        QueryExecuteOperation operation1 = factory.create(queryId, member1);
        QueryExecuteOperation operation2 = factory.create(queryId, member2);

        // Check common port.
        for (QueryExecuteOperation operation : Arrays.asList(operation1, operation2)) {
            assertEquals(queryId, operation.getQueryId());
            assertSame(partitionMap, operation.getPartitionMap());
            assertEquals(2, operation.getFragments().size());
            assertSame(outboundEdgeMap, operation.getOutboundEdgeMap());
            assertSame(inboundEdgeMap, operation.getInboundEdgeMap());
            assertSame(edgeInitialMemoryMap, operation.getEdgeInitialMemoryMap());
            assertSame(args, operation.getArguments());
        }

        // Check fragments.
        assertEquals(operation1.getFragments().get(0), new QueryExecuteOperationFragment(node1, DATA_MEMBERS, null));
        assertEquals(operation2.getFragments().get(0), new QueryExecuteOperationFragment(node1, DATA_MEMBERS, null));

        Collection<UUID> expectedMemberIds = Collections.singletonList(member2);
        assertEquals(operation1.getFragments().get(1), new QueryExecuteOperationFragment(null, EXPLICIT, expectedMemberIds));
        assertEquals(operation2.getFragments().get(1), new QueryExecuteOperationFragment(node2, EXPLICIT, expectedMemberIds));
    }
}
