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

package com.hazelcast.sql.index;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.impl.plan.node.MapIndexScanPlanNode;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.map.AbstractMapTable;
import com.hazelcast.sql.impl.schema.map.JetMapMetadataResolver;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTableResolver;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.support.expressions.ExpressionBiValue;
import com.hazelcast.sql.support.expressions.ExpressionType;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlIndexResolutionTest extends SqlIndexTestSupport {

    private static final AtomicInteger MAP_NAME_GEN = new AtomicInteger();
    private static final String INDEX_NAME = "index";

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
    private HazelcastInstance member;

    @Parameterized.Parameter
    public IndexType indexType;

    @Parameterized.Parameter(1)
    public boolean composite;

    @Parameterized.Parameters(name = "indexType:{0}, composite:{1}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (IndexType indexType : Arrays.asList(IndexType.SORTED, IndexType.HASH)) {
            for (boolean composite : Arrays.asList(true, false)) {
                res.add(new Object[]{indexType, composite});
            }
        }

        return res;
    }

    @Before
    public void before() {
        member = factory.newHazelcastInstance();
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testIndexResolution() {
        for (ExpressionType<?> f1 : allTypes()) {
            for (ExpressionType<?> f2 : allTypes()) {
                checkIndexResolution(f1, f2);
            }
        }
    }

    public void checkIndexResolution(ExpressionType<?> f1, ExpressionType<?> f2) {
        Class<? extends ExpressionBiValue> valueClass = ExpressionBiValue.createBiClass(f1, f2);

        // Check empty map
        IMap<Integer, ExpressionBiValue> map = nextMap();
        map.put(1, ExpressionBiValue.createBiValue(valueClass, 1, null, null));
        checkIndex(map);
        checkIndexUsage(map, f1, f2, false, false);
        map.destroy();

        // Check first component with known type
        map = nextMap();
        map.put(1, ExpressionBiValue.createBiValue(valueClass, 1, f1.valueFrom(), null));
        checkIndex(map, f1.getFieldConverterType());
        checkIndexUsage(map, f1, f2, true, false);
        map.destroy();

        if (composite) {
            // Check second component with known type
            map = nextMap();
            map.put(1, ExpressionBiValue.createBiValue(valueClass, 1, null, f2.valueFrom()));
            checkIndex(map);
            checkIndexUsage(map, f1, f2, false, true);
            map.destroy();

            // Check both components known
            map = nextMap();
            map.put(1, ExpressionBiValue.createBiValue(valueClass, 1, f1.valueFrom(), f2.valueFrom()));
            checkIndex(map, f1.getFieldConverterType(), f2.getFieldConverterType());
            checkIndexUsage(map, f1, f2, true, true);
            map.destroy();
        }
    }

    private IMap<Integer, ExpressionBiValue> nextMap() {
        String mapName = "map" + MAP_NAME_GEN.incrementAndGet();

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.addIndexConfig(getIndexConfig());

        member.getConfig().addMapConfig(mapConfig);

        return member.getMap(mapName);
    }

    protected IndexConfig getIndexConfig() {
        IndexConfig config = new IndexConfig().setName(INDEX_NAME).setType(indexType);

        config.addAttribute("field1");

        if (composite) {
            config.addAttribute("field2");
        }

        return config;
    }

    private void checkIndex(IMap<?, ?> map, QueryDataType... expectedFieldConverterTypes) {
        String mapName = map.getName();

        PartitionedMapTableResolver resolver = new PartitionedMapTableResolver(nodeEngine(member), JetMapMetadataResolver.NO_OP);

        for (Table table : resolver.getTables()) {
            if (((AbstractMapTable) table).getMapName().equals(mapName)) {
                PartitionedMapTable table0 = (PartitionedMapTable) table;

                int field1Ordinal = findFieldOrdinal(table0, "field1");
                int field2Ordinal = findFieldOrdinal(table0, "field2");

                assertEquals(1, table0.getIndexes().size());

                MapTableIndex index = table0.getIndexes().get(0);

                assertEquals(INDEX_NAME, index.getName());
                assertEquals(indexType, index.getType());

                // Components count depends on the index attribute count
                if (composite) {
                    assertEquals(2, index.getComponentsCount());
                } else {
                    assertEquals(1, index.getComponentsCount());
                }

                // Check resolved field converter types. We do not test more than two components.
                List<QueryDataType> expectedFieldConverterTypes0 = expectedFieldConverterTypes == null
                        ? Collections.emptyList() : Arrays.asList(expectedFieldConverterTypes);

                assertTrue(expectedFieldConverterTypes0.size() <= 2);

                assertEquals(expectedFieldConverterTypes0, index.getFieldConverterTypes());

                // Resolved field ordinals depend on the number of resolved converter types
                if (expectedFieldConverterTypes0.isEmpty()) {
                    assertTrue(index.getFieldOrdinals().isEmpty());
                } else if (expectedFieldConverterTypes0.size() == 1) {
                    assertEquals(Collections.singletonList(field1Ordinal), index.getFieldOrdinals());
                } else {
                    assertEquals(Arrays.asList(field1Ordinal, field2Ordinal), index.getFieldOrdinals());
                }

                return;
            }
        }

        fail("Cannot find table for map: " + mapName);
    }

    private static int findFieldOrdinal(PartitionedMapTable table, String fieldName) {
        for (int i = 0; i < table.getFieldCount(); i++) {
            MapTableField field = table.getField(i);

            if (field.getName().equals(fieldName)) {
                return i;
            }
        }

        fail("Failed to find the field \"" + fieldName + "\"");

        return -1;
    }

    private void checkIndexUsage(
            IMap<?, ?> map,
            ExpressionType<?> f1,
            ExpressionType<?> f2,
            boolean firstResolved,
            boolean secondResolved
    ) {
        String field1Literal = toLiteral(f1, f1.valueFrom());
        String field2Literal = toLiteral(f2, f2.valueFrom());

        // The second component could be used if both components are resolved.
        Usage expectedUsage;

        if (firstResolved) {
            if (secondResolved) {
                // Both components were resolved
                expectedUsage = Usage.BOTH;
            } else {
                // Only one component is resolved
                if (composite && indexType == IndexType.HASH) {
                    // Composite HASH index with the first component only cannot be used, because it requires range requests
                    expectedUsage = Usage.NONE;
                } else {
                    // Otherwise, used the first component
                    expectedUsage = Usage.ONE;
                }
            }
        } else {
            // No components were resolved
            expectedUsage = Usage.NONE;
        }

        checkIndexUsage(
                "SELECT * FROM " + map.getName() + " WHERE field1=" + field1Literal + " AND field2=" + field2Literal,
                expectedUsage
        );
    }

    private void checkIndexUsage(String sql, Usage expectedUsage) {
        try (SqlResult result = member.getSql().execute(sql)) {
            MapIndexScanPlanNode indexNode = findFirstIndexNode(result);

            switch (expectedUsage) {
                case NONE:
                    assertNull(indexNode);

                    break;

                case ONE:
                    assertNotNull(indexNode);
                    assertNotNull(indexNode.getFilter());

                    break;

                default:
                    assertNotNull(indexNode);
                    assertNull(indexNode.getFilter());
            }
        }
    }

    private enum Usage {
        /** Index is not used */
        NONE,

        /** Only one component is used */
        ONE,

        /** Both components are used */
        BOTH
    }
}
