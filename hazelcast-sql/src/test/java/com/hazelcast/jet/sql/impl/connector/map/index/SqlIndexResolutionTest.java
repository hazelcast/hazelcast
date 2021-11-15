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
import com.hazelcast.config.MapConfig;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.schema.TableResolverImpl;
import com.hazelcast.jet.sql.impl.schema.TablesStorage;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionType;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.map.AbstractMapTable;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
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

import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlIndexResolutionTest extends SqlIndexTestSupport {

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

    @BeforeClass
    public static void beforeClass() {
        // TODO: https://github.com/hazelcast/hazelcast/issues/19285
        initialize(1, null);
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
        ExpressionBiValue value = ExpressionBiValue.createBiValue(valueClass, 1, null, null);
        createMapping(map.getName(), int.class, value.getClass());
        map.put(1, value);
        checkIndex(map);
        checkIndexUsage(map, f1, f2, false, false);
        map.destroy();

        // Check first component with known type
        map = nextMap();
        value = ExpressionBiValue.createBiValue(valueClass, 1, f1.valueFrom(), null);
        createMapping(map.getName(), int.class, value.getClass());
        map.put(1, value);
        checkIndex(map, f1.getFieldConverterType());
        checkIndexUsage(map, f1, f2, true, false);
        map.destroy();

        if (composite) {
            // Check second component with known type
            map = nextMap();
            value = ExpressionBiValue.createBiValue(valueClass, 1, null, f2.valueFrom());
            createMapping(map.getName(), int.class, value.getClass());
            map.put(1, value);
            checkIndex(map);
            checkIndexUsage(map, f1, f2, false, true);
            map.destroy();

            // Check both components known
            map = nextMap();
            value = ExpressionBiValue.createBiValue(valueClass, 1, f1.valueFrom(), f2.valueFrom());
            createMapping(map.getName(), int.class, value.getClass());
            map.put(1, value);
            checkIndex(map, f1.getFieldConverterType(), f2.getFieldConverterType());
            checkIndexUsage(map, f1, f2, true, true);
            map.destroy();
        }
    }

    private IMap<Integer, ExpressionBiValue> nextMap() {
        String mapName = SqlTestSupport.randomName();

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.addIndexConfig(getIndexConfig());

        instance().getConfig().addMapConfig(mapConfig);

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

        NodeEngine nodeEngine = getNodeEngine(instance());
        TablesStorage tablesStorage = new TablesStorage(nodeEngine);
        SqlConnectorCache connectorCache = new SqlConnectorCache(nodeEngine);
        TableResolver resolver = new TableResolverImpl(nodeEngine, tablesStorage, connectorCache);

        for (Table table : resolver.getTables()) {
            if (table instanceof AbstractMapTable && ((AbstractMapTable) table).getMapName().equals(mapName)) {
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

        checkIndexUsage(sql, f1, f2, map.getName(), expectedUsage);
    }

    private void checkIndexUsage(SqlStatement statement, ExpressionType<?> f1, ExpressionType<?> f2, String mapName, Usage expectedIndexUsage) {
        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, QueryDataType.OBJECT, QueryDataType.INT);
        List<TableField> mapTableFields = asList(
                new MapTableField("__key", QueryDataType.INT, false, QueryPath.KEY_PATH),
                new MapTableField("field1", f1.getFieldConverterType(), false, new QueryPath("field1", false)),
                new MapTableField("field2", f2.getFieldConverterType(), false, new QueryPath("field2", false))
        );
        HazelcastTable table = partitionedTable(
                mapName,
                mapTableFields,
                getPartitionedMapIndexes(mapContainer(instance().getMap(mapName)), mapTableFields),
                1 // we can place random number, doesn't matter in current case.
        );
        OptimizerTestSupport.Result optimizationResult = optimizePhysical(statement.getSql(), parameterTypes, table);

        assertPlan(
                optimizationResult.getLogical(),
                plan(planRow(0, FullScanLogicalRel.class))
        );
        switch (expectedIndexUsage) {
            case NONE:
                assertPlan(
                        optimizationResult.getPhysical(),
                        plan(planRow(0, FullScanPhysicalRel.class))
                );
                break;
            case ONE:
                assertPlan(
                        optimizationResult.getPhysical(),
                        plan(planRow(0, IndexScanMapPhysicalRel.class))
                );
                assertNotNull(((IndexScanMapPhysicalRel) optimizationResult.getPhysical()).getRemainderExp());
                break;
            case BOTH:
                assertPlan(
                        optimizationResult.getPhysical(),
                        plan(planRow(0, IndexScanMapPhysicalRel.class))
                );
                assertNull(((IndexScanMapPhysicalRel) optimizationResult.getPhysical()).getRemainderExp());
                break;
        }
    }

    private enum Usage {
        /**
         * Index is not used
         */
        NONE,

        /**
         * Only one component is used
         */
        ONE,

        /**
         * Both components are used
         */
        BOTH
    }
}
