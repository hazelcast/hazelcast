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

import com.hazelcast.config.IndexType;
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
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
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
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.createBiClass;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.createBiValue;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@SuppressWarnings("FieldCanBeLocal")
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlIndexResolutionTest extends SqlIndexTestSupport {
    private NodeEngine nodeEngine;
    private TableResolver resolver;

    private String mapName;
    private String indexName;

    private IMap<Integer, ? super ExpressionBiValue> map;
    private Class<? extends ExpressionBiValue> valueClass;
    private ExpressionBiValue value;

    @Parameterized.Parameter
    public IndexType indexType;

    @Parameterized.Parameter(1)
    public boolean composite;

    @Parameterized.Parameter(2)
    public ExpressionType<?> type1;

    @Parameterized.Parameter(3)
    public ExpressionType<?> type2;

    @Parameterized.Parameters(name = "indexType:{0}, composite:{1}, type1:{2}, type2:{3}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();
        for (IndexType indexType : Arrays.asList(IndexType.SORTED, IndexType.HASH)) {
            for (boolean composite : Arrays.asList(true, false)) {
                for (ExpressionType<?> t1 : baseTypes()) {
                    for (ExpressionType<?> t2 : baseTypes()) {
                        res.add(new Object[]{indexType, composite, t1, t2});
                    }
                }
            }
        }

        return res;
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(3, null);
    }

    @Before
    public void before() throws Exception {
        nodeEngine = getNodeEngine(instance());
        resolver = new TableResolverImpl(nodeEngine, new TablesStorage(nodeEngine), new SqlConnectorCache(nodeEngine));

        mapName = randomName();
        indexName = randomName();
        String[] indexAttributes = composite ? new String[]{"field1", "field2"} : new String[]{"field1"};

        valueClass = createBiClass(type1, type2);
        value = createBiValue(valueClass, 1, type1.valueFrom(), composite ? type2.valueFrom() : null);

        map = instance().getMap(mapName);
        createMapping(mapName, int.class, value.getClass());
        createIndex(indexName, mapName, indexType, indexAttributes);
    }

    @Test
    public void testIndexResolution() {
        putValues();

        checkIndex(map, composite
                ? asList(type1.getFieldConverterType(), type2.getFieldConverterType())
                : singletonList(type1.getFieldConverterType())
        );

        checkIndexUsage(map, true, composite);
    }

    private void checkIndex(IMap<?, ?> map, List<QueryDataType> expectedFieldConverterTypes) {
        String mapName = map.getName();

        List<PartitionedMapTable> tables = resolver.getTables()
                .stream()
                .filter(t -> t instanceof PartitionedMapTable)
                .map(t -> (PartitionedMapTable) t)
                .filter(t -> t.getMapName().equals(mapName))
                .collect(Collectors.toList());
        assertEquals(1, tables.size());

        PartitionedMapTable table = tables.get(0);
        assertEquals(1, table.getIndexes().size());

        MapTableIndex index = table.getIndexes().get(0);
        assertEquals(indexName, index.getName());
        assertEquals(indexType, index.getType());

        // Components count depends on the index attribute count
        assertEquals(composite ? 2 : 1, index.getComponentsCount());

        int field1Ordinal = findFieldOrdinal(table, "field1");
        int field2Ordinal = findFieldOrdinal(table, "field2");

        // Check resolved field converter types. We do not test more than two components.
        assertTrue(expectedFieldConverterTypes.size() <= 2);
        assertEquals(expectedFieldConverterTypes, index.getFieldConverterTypes());

        // Resolved field ordinals depend on the number of resolved converter types
        if (expectedFieldConverterTypes.isEmpty()) {
            assertTrue(index.getFieldOrdinals().isEmpty());
        } else if (expectedFieldConverterTypes.size() == 1) {
            assertEquals(singletonList(field1Ordinal), index.getFieldOrdinals());
        } else {
            assertEquals(Arrays.asList(field1Ordinal, field2Ordinal), index.getFieldOrdinals());
        }
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

    private void checkIndexUsage(IMap<?, ?> map, boolean firstResolved, boolean secondResolved) {
        String field1Literal = toLiteral(type1, type1.valueFrom());
        String field2Literal = toLiteral(type2, type2.valueFrom());

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

        SqlStatement sql = new SqlStatement(
                "SELECT * FROM " + map.getName() + " WHERE field1=" + field1Literal + " AND field2=" + field2Literal
        );
        checkIndexUsage(sql, map.getName(), expectedUsage);
    }

    private void checkIndexUsage(SqlStatement statement, String mapName, Usage expectedIndexUsage) {
        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, QueryDataType.OBJECT, QueryDataType.INT);
        List<TableField> mapTableFields = asList(
                new MapTableField("__key", QueryDataType.INT, false, QueryPath.KEY_PATH),
                new MapTableField("field1", type1.getFieldConverterType(), false, new QueryPath("field1", false)),
                new MapTableField("field2", type2.getFieldConverterType(), false, new QueryPath("field2", false))
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

    private void putValues() {
        for (int i = 1; i <= 100; ++i) {
            map.put(i, value);
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
