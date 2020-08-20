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
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EmptyPlanNodeTest extends SqlTestSupport {
    @Test
    public void testState() {
        List<QueryDataType> fieldTypes = Collections.singletonList(QueryDataType.INT);
        EmptyPlanNode node = new EmptyPlanNode(1, fieldTypes);

        assertEquals(1, node.getId());
        assertEquals(new PlanNodeSchema(fieldTypes), node.getSchema());
    }

    @Test
    public void testEquality() {
        int id1 = 1;
        int id2 = 2;

        List<QueryDataType> fieldTypes1 = Collections.singletonList(QueryDataType.INT);
        List<QueryDataType> fieldTypes2 = Collections.singletonList(QueryDataType.BIGINT);

        checkEquals(new EmptyPlanNode(id1, fieldTypes1), new EmptyPlanNode(id1, fieldTypes1), true);
        checkEquals(new EmptyPlanNode(id1, fieldTypes1), new EmptyPlanNode(id2, fieldTypes1), false);
        checkEquals(new EmptyPlanNode(id1, fieldTypes1), new EmptyPlanNode(id1, fieldTypes2), false);
    }

    @Test
    public void testSerialization() {
        EmptyPlanNode original = new EmptyPlanNode(1, Collections.singletonList(QueryDataType.INT));
        EmptyPlanNode restored = serializeAndCheck(original, SqlDataSerializerHook.NODE_EMPTY);

        checkEquals(original, restored, true);
    }
}
