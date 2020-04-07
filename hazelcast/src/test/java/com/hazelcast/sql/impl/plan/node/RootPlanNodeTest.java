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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RootPlanNodeTest extends SqlTestSupport {
    @Test
    public void testState() {
        int id = 1;
        MockPlanNode upstream = MockPlanNode.create(2);

        RootPlanNode node = new RootPlanNode(id, upstream);

        assertEquals(id, node.getId());
        assertSame(upstream, node.getUpstream());
        assertEquals(upstream.getSchema(), node.getSchema());
    }

    @Test
    public void testEquality() {
        MockPlanNode upstream1 = MockPlanNode.create(1);
        MockPlanNode upstream2 = MockPlanNode.create(2);

        int id1 = 3;
        int id2 = 4;

        checkEquals(new RootPlanNode(id1, upstream1), new RootPlanNode(id1, upstream1), true);
        checkEquals(new RootPlanNode(id1, upstream1), new RootPlanNode(id2, upstream1), false);
        checkEquals(new RootPlanNode(id1, upstream1), new RootPlanNode(id1, upstream2), false);
    }

    @Test
    public void testSerialization() {
        RootPlanNode original = new RootPlanNode(1, MockPlanNode.create(2));
        RootPlanNode restored = serializeAndCheck(original, SqlDataSerializerHook.NODE_ROOT);

        checkEquals(original, restored, true);
    }
}
