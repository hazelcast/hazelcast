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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.expression.ConstantPredicateExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapScanPlanNodeTest extends SqlTestSupport {
    @Test
    public void testState() {
        int id = 1;
        String mapName = "map";
        List<QueryPath> fieldPaths = Collections.singletonList(valuePath("field"));
        List<QueryDataType> fieldTypes = Collections.singletonList(QueryDataType.INT);
        List<Integer> projects = Collections.singletonList(0);
        Expression<Boolean> filter = new ConstantPredicateExpression(true);

        MapScanPlanNode node = new MapScanPlanNode(
            id,
            mapName,
            GenericQueryTargetDescriptor.INSTANCE,
            GenericQueryTargetDescriptor.INSTANCE,
            fieldPaths,
            fieldTypes,
            projects,
            filter
        );

        assertEquals(id, node.getId());
        assertEquals(mapName, node.getMapName());
        assertEquals(GenericQueryTargetDescriptor.INSTANCE, node.getKeyDescriptor());
        assertEquals(GenericQueryTargetDescriptor.INSTANCE, node.getValueDescriptor());
        assertEquals(fieldPaths, node.getFieldPaths());
        assertEquals(fieldTypes, node.getFieldTypes());
        assertEquals(projects, node.getProjects());
        assertEquals(filter, node.getFilter());
    }

    @Test
    public void testEquality() {
        int id1 = 1;
        int id2 = 2;

        String mapName1 = "map1";
        String mapName2 = "map2";

        List<QueryPath> fieldPaths1 = Collections.singletonList(valuePath("field1"));
        List<QueryPath> fieldPaths2 = Collections.singletonList(valuePath("field2"));

        List<QueryDataType> fieldTypes1 = Collections.singletonList(QueryDataType.INT);
        List<QueryDataType> fieldTypes2 = Collections.singletonList(QueryDataType.BIGINT);

        List<Integer> projects1 = Collections.singletonList(0);
        List<Integer> projects2 = Collections.singletonList(1);

        Expression<Boolean> filter1 = new ConstantPredicateExpression(true);
        Expression<Boolean> filter2 = new ConstantPredicateExpression(false);

        MapScanPlanNode node = new MapScanPlanNode(
            id1,
            mapName1,
            GenericQueryTargetDescriptor.INSTANCE,
            GenericQueryTargetDescriptor.INSTANCE,
            fieldPaths1,
            fieldTypes1,
            projects1,
            filter1
        );

        checkEquals(
            node,
            new MapScanPlanNode(
                id1,
                mapName1,
                GenericQueryTargetDescriptor.INSTANCE,
                GenericQueryTargetDescriptor.INSTANCE,
                fieldPaths1,
                fieldTypes1,
                projects1,
                filter1
            ),
            true
        );

        checkEquals(
            node,
            new MapScanPlanNode(
                id2,
                mapName1,
                GenericQueryTargetDescriptor.INSTANCE,
                GenericQueryTargetDescriptor.INSTANCE,
                fieldPaths1,
                fieldTypes1,
                projects1,
                filter1
            ),
            false
        );

        checkEquals(
            node,
            new MapScanPlanNode(
                id1,
                mapName2,
                GenericQueryTargetDescriptor.INSTANCE,
                GenericQueryTargetDescriptor.INSTANCE,
                fieldPaths1,
                fieldTypes1,
                projects1,
                filter1
            ),
            false
        );

        checkEquals(
            node,
            new MapScanPlanNode(
                id1,
                mapName1,
                new TestTargetDescriptor(),
                GenericQueryTargetDescriptor.INSTANCE,
                fieldPaths1,
                fieldTypes1,
                projects1,
                filter1
            ),
            false
        );

        checkEquals(
            node,
            new MapScanPlanNode(
                id1,
                mapName1,
                GenericQueryTargetDescriptor.INSTANCE,
                new TestTargetDescriptor(),
                fieldPaths1,
                fieldTypes1,
                projects1,
                filter1
            ),
            false
        );

        checkEquals(
            node,
            new MapScanPlanNode(
                id1,
                mapName1,
                GenericQueryTargetDescriptor.INSTANCE,
                GenericQueryTargetDescriptor.INSTANCE,
                fieldPaths2,
                fieldTypes1,
                projects1,
                filter1
            ),
            false
        );

        checkEquals(
            node,
            new MapScanPlanNode(
                id1,
                mapName1,
                GenericQueryTargetDescriptor.INSTANCE,
                GenericQueryTargetDescriptor.INSTANCE,
                fieldPaths1,
                fieldTypes2,
                projects1,
                filter1
            ),
            false
        );

        checkEquals(
            node,
            new MapScanPlanNode(
                id1,
                mapName1,
                GenericQueryTargetDescriptor.INSTANCE,
                GenericQueryTargetDescriptor.INSTANCE,
                fieldPaths1,
                fieldTypes2,
                projects2,
                filter1
            ),
            false
        );

        checkEquals(
            node,
            new MapScanPlanNode(
                id1,
                mapName1,
                GenericQueryTargetDescriptor.INSTANCE,
                GenericQueryTargetDescriptor.INSTANCE,
                fieldPaths1,
                fieldTypes2,
                projects1,
                filter2
            ),
            false
        );
    }

    @Test
    public void testSerialization() {
        MapScanPlanNode original = new MapScanPlanNode(
            1,
            "map",
            GenericQueryTargetDescriptor.INSTANCE,
            GenericQueryTargetDescriptor.INSTANCE,
            Collections.singletonList(valuePath("field")),
            Collections.singletonList(QueryDataType.INT),
            Collections.singletonList(0),
            new ConstantPredicateExpression(true)
        );

        MapScanPlanNode restored = serializeAndCheck(original, SqlDataSerializerHook.NODE_MAP_SCAN);

        checkEquals(original, restored, true);
    }

    private static class TestTargetDescriptor implements QueryTargetDescriptor {
        @Override
        public QueryTarget create(InternalSerializationService serializationService, Extractors extractors, boolean isKey) {
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            // No-op.
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            // No-op.
        }
    }
}
