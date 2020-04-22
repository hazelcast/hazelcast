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
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("rawtypes")
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProjectPlanNodeTest extends SqlTestSupport {
    @Test
    public void testState() {
        MockPlanNode upstream = MockPlanNode.create(1, QueryDataType.INT, QueryDataType.BIGINT, QueryDataType.DOUBLE);
        List<Expression> projects =
            Arrays.asList(ColumnExpression.create(1, QueryDataType.BIGINT), ColumnExpression.create(2, QueryDataType.DOUBLE));

        ProjectPlanNode node = new ProjectPlanNode(2, upstream, projects);

        PlanNodeSchema expectedSchema = new PlanNodeSchema(Arrays.asList(QueryDataType.BIGINT, QueryDataType.DOUBLE));

        assertEquals(2, node.getId());
        assertEquals(upstream, node.getUpstream());
        assertEquals(projects, node.getProjects());
        assertEquals(expectedSchema, node.getSchema());
    }

    @Test
    public void testEquality() {
        int id1 = 1;
        int id2 = 2;

        MockPlanNode upstream1 = MockPlanNode.create(3, QueryDataType.INT, QueryDataType.BIGINT);
        MockPlanNode upstream2 = MockPlanNode.create(3, QueryDataType.INT, QueryDataType.DOUBLE);

        List<Expression> projects1 = Collections.singletonList(ColumnExpression.create(0, QueryDataType.INT));
        List<Expression> projects2 = Collections.singletonList(ColumnExpression.create(1, QueryDataType.BIGINT));

        checkEquals(new ProjectPlanNode(id1, upstream1, projects1), new ProjectPlanNode(id1, upstream1, projects1), true);
        checkEquals(new ProjectPlanNode(id1, upstream1, projects1), new ProjectPlanNode(id2, upstream1, projects1), false);
        checkEquals(new ProjectPlanNode(id1, upstream1, projects1), new ProjectPlanNode(id1, upstream2, projects1), false);
        checkEquals(new ProjectPlanNode(id1, upstream1, projects1), new ProjectPlanNode(id1, upstream1, projects2), false);
    }

    @Test
    public void testSerialization() {
        MockPlanNode upstream = MockPlanNode.create(1, QueryDataType.INT);
        List<Expression> projects = Collections.singletonList(ColumnExpression.create(0, QueryDataType.INT));

        ProjectPlanNode original = new ProjectPlanNode(2, upstream, projects);
        ProjectPlanNode restored = serializeAndCheck(original, SqlDataSerializerHook.NODE_PROJECT);

        checkEquals(original, restored, true);
    }
}
