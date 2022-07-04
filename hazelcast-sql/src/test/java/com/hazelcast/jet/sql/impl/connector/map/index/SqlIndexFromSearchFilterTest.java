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
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.SortLogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SortPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.createBiClass;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.createBiValue;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.INTEGER;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlIndexFromSearchFilterTest extends SqlIndexTestSupport {
    public static final int MAP_SIZE = 1000;
    public static final String F_1_INDEX = "f1_index";
    public static final String F_2_INDEX = "f2_index";

    private String mapName;

    @BeforeClass
    public static void beforeClass() {
        initialize(3, null);
    }

    @Before
    public void before() throws Exception {
        mapName = randomName();

        Class<? extends ExpressionBiValue> valueClass = createBiClass(INTEGER, INTEGER);
        IMap<Integer, ? super ExpressionBiValue> map = instance().getMap(mapName);

        createMapping(mapName, int.class, valueClass);
        createIndex(F_1_INDEX, mapName, IndexType.SORTED, "field1");
        createIndex(F_2_INDEX, mapName, IndexType.SORTED, "field2");

        for (int i = 1; i <= MAP_SIZE; ++i) {
            map.put(i, createBiValue(valueClass, i, i, i));
        }
    }

    @Test
    public void when_selectWithRange_then_properPlanAndIndex() {
        String sql = "SELECT * FROM  \n" + mapName +
                " WHERE field1 >= 100\n" +
                " AND field1 <= 10000 \n";
        Result optimizePhysical = optimizePhysical(sql, parameterTypes(), table());

        assertPlan(
                optimizePhysical.getLogical(),
                plan(
                        planRow(0, FullScanLogicalRel.class)
                )
        );
        assertPlan(
                optimizePhysical.getPhysical(),
                plan(
                        planRow(0, IndexScanMapPhysicalRel.class)
                )
        );

        IndexScanMapPhysicalRel rel = (IndexScanMapPhysicalRel) optimizePhysical.getPhysical();
        assertEquals(F_1_INDEX, rel.getIndex().getName());
    }

    @Test
    public void when_selectWithTwoRanges_then_properPlanAndIndex() {
        String sql = "SELECT * FROM  \n" + mapName +
                " WHERE (field1 >= 100 AND field1 <= 10000) \n" +
                " OR (field1 >= 10100 AND field1 <= 20000) \n";
        Result optimizePhysical = optimizePhysical(sql, parameterTypes(), table());

        assertPlan(
                optimizePhysical.getLogical(),
                plan(
                        planRow(0, FullScanLogicalRel.class)
                )
        );
        assertPlan(
                optimizePhysical.getPhysical(),
                plan(
                        planRow(0, IndexScanMapPhysicalRel.class)
                )
        );

        IndexScanMapPhysicalRel rel = (IndexScanMapPhysicalRel) optimizePhysical.getPhysical();
        assertEquals(F_1_INDEX, rel.getIndex().getName());
    }

    @Test
    public void when_selectWithRangeAndOrderBy_then_properPlanAndIndex() {
        String sql = "SELECT * FROM  \n" + mapName +
                " WHERE field1 >= 100\n" +
                " AND field1 <= 10000 \n" +
                " ORDER BY field2 ASC LIMIT 20 OFFSET 0";
        Result optimizePhysical = optimizePhysical(sql, parameterTypes(), table());

        assertPlan(
                optimizePhysical.getLogical(),
                plan(
                        planRow(0, SortLogicalRel.class),
                        planRow(1, FullScanLogicalRel.class)
                )
        );
        assertPlan(
                optimizePhysical.getPhysical(),
                plan(
                        planRow(0, SortPhysicalRel.class),
                        planRow(1, IndexScanMapPhysicalRel.class)
                )
        );

        IndexScanMapPhysicalRel rel = (IndexScanMapPhysicalRel) optimizePhysical.getPhysical().getInput(0);
        assertEquals(F_2_INDEX, rel.getIndex().getName());
    }

    @Test
    public void when_selectWithMultipleEquals_then_properPlan() {
        String sql = "SELECT * FROM  \n" + mapName +
                " WHERE field1 = -1\n" +
                " OR field1 = 1 \n" +
                " OR field1 = 3 \n";
        Result optimizePhysical = optimizePhysical(sql, parameterTypes(), table());

        assertPlan(
                optimizePhysical.getLogical(),
                plan(
                        planRow(0, FullScanLogicalRel.class)
                )
        );
        assertPlan(
                optimizePhysical.getPhysical(),
                plan(
                        planRow(0, IndexScanMapPhysicalRel.class)
                )
        );
    }

    private List<QueryDataType> parameterTypes() {
        return asList(QueryDataType.INT, QueryDataType.OBJECT, QueryDataType.INT);
    }

    private HazelcastTable table() {
        List<TableField> mapTableFields = asList(
                new MapTableField("__key", QueryDataType.INT, false, QueryPath.KEY_PATH),
                new MapTableField("field1", INTEGER.getFieldConverterType(), false, new QueryPath("field1", false)),
                new MapTableField("field2", INTEGER.getFieldConverterType(), false, new QueryPath("field2", false))
        );
        HazelcastTable table = partitionedTable(
                mapName,
                mapTableFields,
                getPartitionedMapIndexes(mapContainer(instance().getMap(mapName)), mapTableFields),
                MAP_SIZE
        );
        return table;
    }
}
