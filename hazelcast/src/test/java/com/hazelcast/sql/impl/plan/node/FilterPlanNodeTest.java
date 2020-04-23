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

package com.hazelcast.sql.impl.plan.node;

import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.expression.ConstantPredicateExpression;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FilterPlanNodeTest extends SqlTestSupport {
    @Test
    public void testState() {
        MockPlanNode upstream = MockPlanNode.create(1, QueryDataType.INT);
        ConstantPredicateExpression filter = new ConstantPredicateExpression(true);

        FilterPlanNode node = new FilterPlanNode(2, upstream, filter);

        assertEquals(2, node.getId());
        assertEquals(upstream, node.getUpstream());
        assertEquals(upstream.getSchema(), node.getSchema());
        assertEquals(filter, node.getFilter());
    }

    @Test
    public void testEquality() {
        int id1 = 1;
        int id2 = 2;

        MockPlanNode upstream1 = MockPlanNode.create(3, QueryDataType.INT);
        MockPlanNode upstream2 = MockPlanNode.create(3, QueryDataType.BIGINT);

        ConstantPredicateExpression filter1 = new ConstantPredicateExpression(true);
        ConstantPredicateExpression filter2 = new ConstantPredicateExpression(false);

        checkEquals(new FilterPlanNode(id1, upstream1, filter1), new FilterPlanNode(id1, upstream1, filter1), true);
        checkEquals(new FilterPlanNode(id1, upstream1, filter1), new FilterPlanNode(id2, upstream1, filter1), false);
        checkEquals(new FilterPlanNode(id1, upstream1, filter1), new FilterPlanNode(id1, upstream2, filter1), false);
        checkEquals(new FilterPlanNode(id1, upstream1, filter1), new FilterPlanNode(id1, upstream1, filter2), false);
    }

    @Test
    public void testSerialization() {
        MockPlanNode upstream = MockPlanNode.create(1, QueryDataType.INT);
        ConstantPredicateExpression filter = new ConstantPredicateExpression(true);

        FilterPlanNode original = new FilterPlanNode(1, upstream, filter);
        FilterPlanNode restored = serializeAndCheck(original, SqlDataSerializerHook.NODE_FILTER);

        checkEquals(original, restored, true);
    }
}
