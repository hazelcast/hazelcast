/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.map.index;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
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
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
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

import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.sql.impl.SqlTestSupport.getMapContainer;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class SqlMapIndexResolutionTest extends JetSqlIndexTestSupport {

    @BeforeClass
    public static void setUp() {
        initialize(2, smallInstanceConfig());
    }

    private static final AtomicInteger mapName_GEN = new AtomicInteger();
    private String mapName;
    private static final String INDEX_NAME = "index";

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
        mapName = SqlTestSupport.randomName();
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
        mapName = "map" + mapName_GEN.incrementAndGet();

        instance().getMap(mapName).addIndex(getIndexConfig());
        return instance().getMap(mapName);
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

        PartitionedMapTableResolver resolver = new PartitionedMapTableResolver(getNodeEngine(instance()), JetMapMetadataResolver.NO_OP);

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

        SqlStatement sql = new SqlStatement("SELECT * FROM " + map.getName() + " WHERE field1=" + field1Literal + " AND field2=" + field2Literal);

        checkIndexUsage(sql, f1, f2, expectedUsage);
    }

    private void checkIndexUsage(SqlStatement statement, ExpressionType<?> f1, ExpressionType<?> f2, Usage expectedIndexUsage) {
        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, QueryDataType.OBJECT, QueryDataType.INT);
        List<TableField> mapTableFields = asList(
                new MapTableField("__key", QueryDataType.INT, false, QueryPath.KEY_PATH),
                new MapTableField("field1", f1.getFieldConverterType(), false, new QueryPath("field1", false)),
                new MapTableField("field2", f2.getFieldConverterType(), false, new QueryPath("field2", false))
        );
        HazelcastTable table = partitionedTable(
                mapName,
                mapTableFields,
                getPartitionedMapIndexes(getMapContainer(instance().getMap(mapName)), mapTableFields),
                1 // we can place random number, doesn't matter in current case.
        );
        OptimizerTestSupport.Result optimizationResult = optimizePhysical(statement.getSql(), parameterTypes, table);

        assertPlan(
                optimizationResult.getLogical(),
                plan(planRow(0, FullScanLogicalRel.class))
        );
        switch (expectedIndexUsage) {
            case BOTH:
                assertPlan(
                        optimizationResult.getPhysical(),
                        plan(planRow(0, IndexScanMapPhysicalRel.class))
                );
                assertNull(((IndexScanMapPhysicalRel) optimizationResult.getPhysical()).getRemainderExp());
                break;
            case ONE:
                assertPlan(
                        optimizationResult.getPhysical(),
                        plan(planRow(0, IndexScanMapPhysicalRel.class))
                );
                assertNotNull(((IndexScanMapPhysicalRel) optimizationResult.getPhysical()).getRemainderExp());
                break;
            case NONE:
                assertPlan(
                        optimizationResult.getPhysical(),
                        plan(planRow(0, FullScanPhysicalRel.class))
                );
                break;
        }
    }
}
