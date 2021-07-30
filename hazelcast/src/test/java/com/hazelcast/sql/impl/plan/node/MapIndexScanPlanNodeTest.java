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

package com.hazelcast.sql.impl.plan.node;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexInFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapIndexScanPlanNodeTest extends SqlTestSupport {
    @Test
    public void testState() {
        int id = 1;
        String mapName = "map";
        List<QueryPath> fieldPaths = Collections.singletonList(valuePath("field"));
        List<QueryDataType> fieldTypes = Collections.singletonList(QueryDataType.INT);
        List<Integer> projects = Collections.singletonList(0);
        String indexName = "index";
        int indexComponentCount = 1;
        IndexFilter indexFilter = new IndexRangeFilter();
        List<QueryDataType> converterTypes = Collections.singletonList(QueryDataType.INT);
        Expression<Boolean> remainderFilter = new ConstantPredicateExpression(true);

        MapIndexScanPlanNode node = new MapIndexScanPlanNode(
            id,
            mapName,
            GenericQueryTargetDescriptor.DEFAULT,
            GenericQueryTargetDescriptor.DEFAULT,
            fieldPaths,
            fieldTypes,
            projects,
            indexName,
            indexComponentCount,
            indexFilter,
            converterTypes,
            remainderFilter,
            Collections.singletonList(false)
        );

        assertEquals(id, node.getId());
        assertEquals(mapName, node.getMapName());
        assertEquals(GenericQueryTargetDescriptor.DEFAULT, node.getKeyDescriptor());
        assertEquals(GenericQueryTargetDescriptor.DEFAULT, node.getValueDescriptor());
        assertEquals(fieldPaths, node.getFieldPaths());
        assertEquals(fieldTypes, node.getFieldTypes());
        assertEquals(projects, node.getProjects());
        assertEquals(indexName, node.getIndexName());
        assertEquals(indexComponentCount, node.getIndexComponentCount());
        assertEquals(indexFilter, node.getIndexFilter());
        assertEquals(converterTypes, node.getConverterTypes());
        assertEquals(remainderFilter, node.getFilter());
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

        String indexName1 = "index1";
        String indexName2 = "index2";

        int indexComponentCount1 = 1;
        int indexComponentCount2 = 2;
        List<Boolean> ascs1 = Arrays.asList(true);
        List<Boolean> ascs2 = Arrays.asList(true, true);

        IndexFilter indexFilter1 = new IndexRangeFilter();
        IndexFilter indexFilter2 = new IndexInFilter();

        List<QueryDataType> converterTypes1 = Collections.singletonList(QueryDataType.INT);
        List<QueryDataType> converterTypes2 = Collections.singletonList(QueryDataType.BIGINT);

        Expression<Boolean> remainderFilter1 = new ConstantPredicateExpression(true);
        Expression<Boolean> remainderFilter2 = new ConstantPredicateExpression(false);

        MapIndexScanPlanNode node = new MapIndexScanPlanNode(
            id1,
            mapName1,
            GenericQueryTargetDescriptor.DEFAULT,
            GenericQueryTargetDescriptor.DEFAULT,
            fieldPaths1,
            fieldTypes1,
            projects1,
            indexName1,
            indexComponentCount1,
            indexFilter1,
            converterTypes1,
            remainderFilter1,
            ascs1
        );

        checkEquals(
            node,
            new MapIndexScanPlanNode(
                id1,
                mapName1,
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                fieldPaths1,
                fieldTypes1,
                projects1,
                indexName1,
                indexComponentCount1,
                indexFilter1,
                converterTypes1,
                remainderFilter1,
                ascs1
            ),
            true
        );

        checkEquals(
            node,
            new MapIndexScanPlanNode(
                id2,
                mapName1,
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                fieldPaths1,
                fieldTypes1,
                projects1,
                indexName1,
                indexComponentCount1,
                indexFilter1,
                converterTypes1,
                remainderFilter1,
                ascs1
            ),
            false
        );

        checkEquals(
            node,
            new MapIndexScanPlanNode(
                id1,
                mapName2,
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                fieldPaths1,
                fieldTypes1,
                projects1,
                indexName1,
                indexComponentCount1,
                indexFilter1,
                converterTypes1,
                remainderFilter1,
                ascs1
            ),
            false
        );

        checkEquals(
            node,
            new MapIndexScanPlanNode(
                id1,
                mapName1,
                new TestTargetDescriptor(),
                GenericQueryTargetDescriptor.DEFAULT,
                fieldPaths1,
                fieldTypes1,
                projects1,
                indexName1,
                indexComponentCount1,
                indexFilter1,
                converterTypes1,
                remainderFilter1,
                ascs1
            ),
            false
        );

        checkEquals(
            node,
            new MapIndexScanPlanNode(
                id1,
                mapName1,
                GenericQueryTargetDescriptor.DEFAULT,
                new TestTargetDescriptor(),
                fieldPaths1,
                fieldTypes1,
                projects1,
                indexName1,
                indexComponentCount1,
                indexFilter1,
                converterTypes1,
                remainderFilter1,
                ascs1
            ),
            false
        );

        checkEquals(
            node,
            new MapIndexScanPlanNode(
                id1,
                mapName1,
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                fieldPaths2,
                fieldTypes1,
                projects1,
                indexName1,
                indexComponentCount1,
                indexFilter1,
                converterTypes1,
                remainderFilter1,
                ascs1
            ),
            false
        );

        checkEquals(
            node,
            new MapIndexScanPlanNode(
                id1,
                mapName1,
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                fieldPaths1,
                fieldTypes2,
                projects1,
                indexName1,
                indexComponentCount1,
                indexFilter1,
                converterTypes1,
                remainderFilter1,
                ascs1
            ),
            false
        );

        checkEquals(
            node,
            new MapIndexScanPlanNode(
                id1,
                mapName1,
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                fieldPaths1,
                fieldTypes1,
                projects2,
                indexName1,
                indexComponentCount1,
                indexFilter1,
                converterTypes1,
                remainderFilter1,
                ascs1
            ),
            false
        );

        checkEquals(
            node,
            new MapIndexScanPlanNode(
                id1,
                mapName1,
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                fieldPaths1,
                fieldTypes1,
                projects1,
                indexName2,
                indexComponentCount1,
                indexFilter1,
                converterTypes1,
                remainderFilter1,
                ascs1
            ),
            false
        );

        checkEquals(
            node,
            new MapIndexScanPlanNode(
                id1,
                mapName1,
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                fieldPaths1,
                fieldTypes1,
                projects1,
                indexName1,
                indexComponentCount2,
                indexFilter1,
                converterTypes1,
                remainderFilter1,
                ascs2
            ),
            false
        );

        checkEquals(
            node,
            new MapIndexScanPlanNode(
                id1,
                mapName1,
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                fieldPaths1,
                fieldTypes1,
                projects1,
                indexName1,
                indexComponentCount1,
                indexFilter2,
                converterTypes1,
                remainderFilter1,
                ascs1
            ),
            false
        );

        checkEquals(
            node,
            new MapIndexScanPlanNode(
                id1,
                mapName1,
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                fieldPaths1,
                fieldTypes1,
                projects1,
                indexName1,
                indexComponentCount1,
                indexFilter1,
                converterTypes2,
                remainderFilter1,
                ascs1
            ),
            false
        );

        checkEquals(
            node,
            new MapIndexScanPlanNode(
                id1,
                mapName1,
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                fieldPaths1,
                fieldTypes1,
                projects1,
                indexName1,
                indexComponentCount1,
                indexFilter1,
                converterTypes1,
                remainderFilter2,
                ascs1
            ),
            false
        );
    }

    @Test
    public void testSerialization() {
        MapIndexScanPlanNode original = new MapIndexScanPlanNode(
            1,
            "map",
            GenericQueryTargetDescriptor.DEFAULT,
            GenericQueryTargetDescriptor.DEFAULT,
            Collections.singletonList(valuePath("field")),
            Collections.singletonList(QueryDataType.INT),
            Collections.singletonList(0),
            "index",
            1,
            new IndexRangeFilter(),
            Collections.singletonList(QueryDataType.INT),
            new ConstantPredicateExpression(true),
            Collections.singletonList(false)
        );

        MapIndexScanPlanNode restored = serializeAndCheck(original, SqlDataSerializerHook.NODE_MAP_INDEX_SCAN);

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
