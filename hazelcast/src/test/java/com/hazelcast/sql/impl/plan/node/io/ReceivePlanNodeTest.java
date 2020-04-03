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

package com.hazelcast.sql.impl.plan.node.io;

import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReceivePlanNodeTest extends SqlTestSupport {
    @Test
    public void testState() {
        int id = 1;
        int edgeId = 2;
        List<QueryDataType> types = Arrays.asList(QueryDataType.INT, QueryDataType.VARCHAR);

        ReceivePlanNode node = new ReceivePlanNode(id, edgeId, types);

        assertEquals(id, node.getId());
        assertEquals(edgeId, node.getEdgeId());
        assertEquals(new PlanNodeSchema(types), node.getSchema());
    }

    @Test
    public void testEquality() {
        int id1 = 1;
        int id2 = 2;

        int edgeId1 = 3;
        int edgeId2 = 4;

        List<QueryDataType> types1 = Arrays.asList(QueryDataType.INT, QueryDataType.VARCHAR);
        List<QueryDataType> types2 = Arrays.asList(QueryDataType.DECIMAL, QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME);

        checkEquals(new ReceivePlanNode(id1, edgeId1, types1), new ReceivePlanNode(id1, edgeId1, types1), true);
        checkEquals(new ReceivePlanNode(id1, edgeId1, types1), new ReceivePlanNode(id2, edgeId1, types1), false);
        checkEquals(new ReceivePlanNode(id1, edgeId1, types1), new ReceivePlanNode(id1, edgeId2, types1), false);
        checkEquals(new ReceivePlanNode(id1, edgeId1, types1), new ReceivePlanNode(id1, edgeId1, types2), false);
    }

    @Test
    public void testSerialization() {
        ReceivePlanNode original = new ReceivePlanNode(1, 2, Arrays.asList(QueryDataType.INT, QueryDataType.VARCHAR));
        ReceivePlanNode restored = serializeAndCheck(original, SqlDataSerializerHook.NODE_RECEIVE);

        checkEquals(original, restored, true);
    }
}
