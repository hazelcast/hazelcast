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

package com.hazelcast.sql.impl.plan;

import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.plan.node.MockPlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PlanTest extends SqlTestSupport {
    @Test
    public void testPlan() {
        Map<UUID, PartitionIdSet> partitionMap = Collections.singletonMap(UUID.randomUUID(), new PartitionIdSet(1));
        List<PlanNode> fragments = Collections.singletonList(new MockPlanNode());
        List<PlanFragmentMapping> fragmentMappings = Collections.singletonList(new PlanFragmentMapping(Collections.emptyList(), true));
        Map<Integer, Integer> outboundEdgeMap = Collections.singletonMap(1, 1);
        Map<Integer, Integer> inboundEdgeMap = Collections.singletonMap(2, 2);
        Map<Integer, Integer> inboundEdgeMemberCountMap = Collections.singletonMap(3, 3);

        Plan plan = new Plan(
            partitionMap,
            fragments,
            fragmentMappings,
            outboundEdgeMap,
            inboundEdgeMap,
            inboundEdgeMemberCountMap
        );

        assertSame(partitionMap, plan.getPartitionMap());

        assertEquals(1, plan.getFragmentCount());
        assertSame(fragments.get(0), plan.getFragment(0));
        assertSame(fragmentMappings.get(0), plan.getFragmentMapping(0));

        assertSame(outboundEdgeMap, plan.getOutboundEdgeMap());
        assertSame(inboundEdgeMap, plan.getInboundEdgeMap());
        assertSame(inboundEdgeMemberCountMap, plan.getInboundEdgeMemberCountMap());
    }
}
