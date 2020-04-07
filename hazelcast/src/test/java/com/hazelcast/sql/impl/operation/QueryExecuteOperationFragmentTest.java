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

import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.plan.node.MockPlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.sql.impl.operation.QueryExecuteOperationFragmentMapping.EXPLICIT;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryExecuteOperationFragmentTest extends SqlTestSupport {
    @Test
    public void testFragment() {
        PlanNode node = MockPlanNode.create(1, QueryDataType.INT);
        List<UUID> memberIds = Arrays.asList(UUID.randomUUID(), UUID.randomUUID());

        QueryExecuteOperationFragment fragment = new QueryExecuteOperationFragment(node, EXPLICIT, memberIds);

        assertEquals(node, fragment.getNode());
        assertEquals(memberIds, fragment.getMemberIds());
    }

    @Test
    public void testSerialization() {
        QueryExecuteOperationFragment original = new QueryExecuteOperationFragment(
            MockPlanNode.create(1, QueryDataType.INT),
            EXPLICIT,
            Arrays.asList(UUID.randomUUID(), UUID.randomUUID())
        );

        QueryExecuteOperationFragment restored = serializeAndCheck(original, SqlDataSerializerHook.OPERATION_EXECUTE_FRAGMENT);

        assertEquals(original.getNode(), restored.getNode());
        assertEquals(original.getMapping(), restored.getMapping());
        assertEquals(original.getMemberIds(), restored.getMemberIds());
    }
}
