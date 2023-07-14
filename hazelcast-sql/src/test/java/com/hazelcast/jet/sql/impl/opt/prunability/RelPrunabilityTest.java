/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.opt.prunability;

import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.jet.sql.impl.HazelcastRexBuilder;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMetadataQuery;
import com.hazelcast.jet.sql.impl.opt.physical.AggregateAccumulateByKeyPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.AggregateCombineByKeyPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.CalcPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class RelPrunabilityTest extends OptimizerTestSupport {
    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Test
    public void test_fullScan() {
        HazelcastTable table = partitionedTable(
                "m",
                asList(
                        mapField(KEY, BIGINT, QueryPath.KEY_PATH),
                        mapField(VALUE, VARCHAR, QueryPath.VALUE_PATH)),
                10);
        PhysicalRel root = optimizePhysical("SELECT __key FROM m WHERE __key = 10 AND this IS NOT NULL", asList(BIGINT, VARCHAR), table).getPhysical();

        assertPlan(root, plan(planRow(0, FullScanPhysicalRel.class)));

        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(RelMetadataQuery.instance());
        Map<String, List<Map<String, RexNode>>> prunability = query.extractPrunability(root);
        RelDataType bigintType = HazelcastTypeUtils.createType(
                HazelcastTypeFactory.INSTANCE,
                SqlTypeName.BIGINT,
                true);
        final RexLiteral expectedLiteral = HazelcastRexBuilder.INSTANCE.makeLiteral(10, bigintType);

        assertEquals(Map.of("m", singletonList(Map.of("__key", expectedLiteral))), prunability);
    }

    @Test
    public void test_indexScan() {
        String mapName = randomName();
        String indexName = randomName();
        IMap<Integer, String> map = instance().getMap(mapName);
        instance().getConfig().addMapConfig(new MapConfig(mapName).setPartitioningAttributeConfigs(List.of(
                new PartitioningAttributeConfig("this"))));

        createMapping(mapName, Integer.class, String.class);
        createIndex(indexName, mapName, IndexType.HASH, "this");
        for (int i = 0; i < 100; ++i) {
            map.put(i, "" + i);
        }

        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, VARCHAR);
        List<TableField> mapTableFields = asList(
                new MapTableField("__key", QueryDataType.INT, false, QueryPath.KEY_PATH),
                new MapTableField("this", VARCHAR, false, QueryPath.VALUE_PATH));
        HazelcastTable table = partitionedTable(
                mapName,
                mapTableFields,
                getPartitionedMapIndexes(mapContainer(map), mapTableFields),
                1 // we can place random number, doesn't matter in current case.
        );

        PhysicalRel root = optimizePhysical(
                "SELECT __key FROM " + mapName + " WHERE this = '10'", asList(BIGINT, VARCHAR), table
        ).getPhysical();

        assertPlan(root, plan(planRow(0, IndexScanMapPhysicalRel.class)));

        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(RelMetadataQuery.instance());
        Map<String, List<Map<String, RexNode>>> prunability = query.extractPrunability(root);
        RelDataType bigintType = HazelcastTypeUtils.createType(
                HazelcastTypeFactory.INSTANCE,
                SqlTypeName.BIGINT,
                true);
        final RexLiteral expectedLiteral = HazelcastRexBuilder.INSTANCE.makeLiteral(10, bigintType);

        assertEquals(Map.of("m", singletonList(Map.of("this", expectedLiteral))), prunability);
    }


    @Test
    public void test_calc() {
        HazelcastTable table = partitionedTable(
                "m",
                asList(
                        mapField(KEY, BIGINT, QueryPath.KEY_PATH),
                        mapField(VALUE, VARCHAR, QueryPath.VALUE_PATH)),
                10);
        PhysicalRel root = optimizePhysical("SELECT this, SUM(__key) FROM m WHERE __key = ? AND this = '10' GROUP BY __key, this", asList(BIGINT, VARCHAR), table).getPhysical();

        assertPlan(root, plan(
                planRow(0, CalcPhysicalRel.class),
                planRow(1, AggregateCombineByKeyPhysicalRel.class),
                planRow(2, AggregateAccumulateByKeyPhysicalRel.class),
                planRow(3, FullScanPhysicalRel.class)
        ));

        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(RelMetadataQuery.instance());
        Map<String, List<Map<String, RexNode>>> prunability = query.extractPrunability(root);
        RelDataType bigintType = HazelcastTypeUtils.createType(
                HazelcastTypeFactory.INSTANCE,
                SqlTypeName.BIGINT,
                true);
        final RexDynamicParam param = HazelcastRexBuilder.INSTANCE.makeDynamicParam(bigintType, 0);
        assertEquals(Map.of("m", singletonList(Map.of("__key", param))), prunability);
    }
}
