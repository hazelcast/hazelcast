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
import com.hazelcast.jet.sql.impl.HazelcastRexBuilder;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMetadataQuery;
import com.hazelcast.jet.sql.impl.opt.physical.AggregateAccumulateByKeyPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.AggregateCombineByKeyPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.CalcPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.JoinNestedLoopPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SortPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.UnionPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class RelPrunabilityTest extends OptimizerTestSupport {
    private static final RelDataType REL_TYPE_BIGINT = HazelcastTypeUtils.createType(
            HazelcastTypeFactory.INSTANCE,
            SqlTypeName.BIGINT,
            true);

    private static final String MAP_NAME = "m";

    private HazelcastTable table;
    private HazelcastRelMetadataQuery query;
    List<TableField> mapTableFields;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Before
    public void before() throws Exception {
        this.mapTableFields = asList(
                mapField(KEY, OBJECT, QueryPath.KEY_PATH),
                mapField("comp0", BIGINT, QueryPath.create(QueryPath.KEY_PREFIX + "comp0")),
                mapField("comp1", BIGINT, QueryPath.create(QueryPath.KEY_PREFIX + "comp1")),
                mapField(VALUE, BIGINT, QueryPath.VALUE_PATH));

        this.table = partitionedTable(
                MAP_NAME,
                mapTableFields,
                emptyList(),
                10,
                singletonList("comp1"), true);
    }

    @Test
    public void test_fullScanWithDefaultKey() {
        this.mapTableFields = asList(
                mapField(KEY, BIGINT, QueryPath.KEY_PATH),
                mapField(VALUE, BIGINT, QueryPath.VALUE_PATH));

        this.table = partitionedTable(
                MAP_NAME,
                mapTableFields,
                emptyList(),
                10,
                emptyList(), true);

        PhysicalRel root = optimizePhysical("SELECT * FROM m WHERE __key = 10 AND this IS NOT NULL",
                asList(BIGINT, BIGINT), table)
                .getPhysical();
        assertPlan(root, plan(planRow(0, FullScanPhysicalRel.class)));

        query = HazelcastRelMetadataQuery.reuseOrCreate(RelMetadataQuery.instance());
        Map<String, List<Map<String, RexNode>>> prunability = query.extractPrunability(root);
        final RexLiteral expectedLiteral = HazelcastRexBuilder.INSTANCE.makeLiteral(10, REL_TYPE_BIGINT);
        assertEquals(Map.of(MAP_NAME, singletonList(Map.of("__key", expectedLiteral))), prunability);
    }

    @Test
    public void test_fullScanWithStrategy() {
        PhysicalRel root = optimizePhysical("SELECT * FROM m WHERE comp1 = 10", asList(BIGINT, BIGINT), table)
                .getPhysical();
        assertPlan(root, plan(planRow(0, FullScanPhysicalRel.class)));

        query = HazelcastRelMetadataQuery.reuseOrCreate(RelMetadataQuery.instance());
        Map<String, List<Map<String, RexNode>>> prunability = query.extractPrunability(root);
        final RexLiteral expectedLiteral = HazelcastRexBuilder.INSTANCE.makeLiteral(10, REL_TYPE_BIGINT);
        assertEquals(Map.of(MAP_NAME, singletonList(Map.of("comp1", expectedLiteral))), prunability);
    }

    @Test
    public void test_sort() {
        PhysicalRel root = optimizePhysical("SELECT * FROM m WHERE comp1 = 10 ORDER BY comp0",
                asList(BIGINT, BIGINT), table)
                .getPhysical();

        assertPlan(root, plan(
                planRow(0, SortPhysicalRel.class),
                planRow(1, FullScanPhysicalRel.class)
        ));

        query = HazelcastRelMetadataQuery.reuseOrCreate(RelMetadataQuery.instance());
        Map<String, List<Map<String, RexNode>>> prunability = query.extractPrunability(root);
        final RexLiteral expectedLiteral = HazelcastRexBuilder.INSTANCE.makeLiteral(10, REL_TYPE_BIGINT);
        assertEquals(Map.of(MAP_NAME, singletonList(Map.of("comp1", expectedLiteral))), prunability);
    }

    @Test
    @Ignore("Resolve later : There is no suitable accessor for 'comp1' on class 'java.lang.String'")
    public void test_indexScan_isNotSupported() {
        String mapName = randomName();
        String indexName = randomName();
        IMap<CompoundKey, String> map = instance().getMap(mapName);
        createMapping(mapName, CompoundKey.class, String.class);
        createIndex(indexName, mapName, IndexType.HASH, "comp1");
        for (int i = 0; i < 100; ++i) {
            map.put(new CompoundKey(i, i), "" + i);
        }

        table = partitionedTable(
                mapName,
                mapTableFields,
                getPartitionedMapIndexes(mapContainer(map), mapTableFields),
                1, // we can place random number, doesn't matter in current case.
                singletonList("comp1"),
                true);

        PhysicalRel root = optimizePhysical(
                "SELECT * FROM " + mapName + " WHERE comp1 = 10",
                asList(OBJECT, QueryDataType.INT, QueryDataType.INT, VARCHAR),
                table
        ).getPhysical();
        assertPlan(root, plan(planRow(0, IndexScanMapPhysicalRel.class)));

        query = HazelcastRelMetadataQuery.reuseOrCreate(RelMetadataQuery.instance());
        Map<String, List<Map<String, RexNode>>> prunability = query.extractPrunability(root);
        final RexLiteral expectedLiteral = HazelcastRexBuilder.INSTANCE.makeLiteral(10, REL_TYPE_BIGINT);

        assertEquals(emptyMap(), prunability);
        // TODO: uncomment when IndexScan prunability is implemented
        // assertEquals(Map.of(MAP_NAME, singletonList(Map.of("comp1", expectedLiteral))), prunability);
    }


    @Test
    public void test_aggAndCalc_areNotSupported() {
        PhysicalRel root = optimizePhysical(
                "SELECT this, COUNT(__key) FROM m WHERE comp1 = ? AND comp0 = 10 " +
                        "GROUP BY comp0, comp1, this",
                asList(BIGINT, VARCHAR),
                table
        ).getPhysical();

        assertPlan(root, plan(
                planRow(0, CalcPhysicalRel.class),
                planRow(1, AggregateCombineByKeyPhysicalRel.class),
                planRow(2, AggregateAccumulateByKeyPhysicalRel.class),
                planRow(3, FullScanPhysicalRel.class)
        ));

        query = HazelcastRelMetadataQuery.reuseOrCreate(RelMetadataQuery.instance());
        Map<String, List<Map<String, RexNode>>> prunability = query.extractPrunability(root);
        assertEquals(emptyMap(), prunability);
        // TODO: uncomment when Aggregation prunability is implemented
        // final RexDynamicParam param = HazelcastRexBuilder.INSTANCE.makeDynamicParam(REL_TYPE_BIGINT, 0);
        // assertEquals(Map.of(MAP_NAME, singletonList(Map.of("comp1", param))), prunability);
    }

    @Test
    public void test_joinAndCalc_areNotSupported() {
        PhysicalRel root = optimizePhysical(
                "SELECT * FROM m AS m1 "
                        + "JOIN m AS m2 "
                        + "ON m1.comp1 = m2.comp1 "
                        + "WHERE m1.comp1 = 10 AND m1.this IS NOT NULL " +
                        "    AND m2.comp1 = 10 AND m2.this IS NOT NULL",
                asList(BIGINT, VARCHAR),
                table
        ).getPhysical();

        assertPlan(root, plan(
                planRow(0, JoinNestedLoopPhysicalRel.class),
                planRow(1, FullScanPhysicalRel.class),
                planRow(1, FullScanPhysicalRel.class)
        ));

        query = HazelcastRelMetadataQuery.reuseOrCreate(RelMetadataQuery.instance());
        Map<String, List<Map<String, RexNode>>> prunability = query.extractPrunability(root);
        assertEquals(emptyMap(), prunability);
        // TODO: uncomment when Aggregation prunability is implemented
//        final RexLiteral l = HazelcastRexBuilder.INSTANCE.makeLiteral(10, REL_TYPE_BIGINT);
//        assertEquals(Map.of(
//                        "m1", singletonList(Map.of("comp1", l)),
//                        "m2", singletonList(Map.of("comp1", l))),
//                prunability);
    }

    @Test
    public void test_union() {
        PhysicalRel root = optimizePhysical(
                "(SELECT * FROM m WHERE comp1 = 10 AND this IS NOT NULL)" +
                        " UNION ALL " +
                        "(SELECT * FROM m WHERE comp1 = 10 AND this IS NULL)",
                asList(BIGINT, VARCHAR),
                table
        ).getPhysical();

        assertPlan(root, plan(
                planRow(0, UnionPhysicalRel.class),
                planRow(1, FullScanPhysicalRel.class),
                planRow(1, FullScanPhysicalRel.class)
        ));

        query = HazelcastRelMetadataQuery.reuseOrCreate(RelMetadataQuery.instance());
        Map<String, List<Map<String, RexNode>>> prunability = query.extractPrunability(root);
        final RexLiteral l = HazelcastRexBuilder.INSTANCE.makeLiteral(10, REL_TYPE_BIGINT);
        assertEquals(
                Map.of(MAP_NAME, asList(Map.of("comp1", l), Map.of("comp1", l))),
                prunability);
    }

    @Test
    public void shouldNotForwardPrunability_whenOneBranchIsNotPrunable() {
        PhysicalRel root = optimizePhysical(
                "(SELECT * FROM m WHERE comp1 = 10 AND this IS NOT NULL)" +
                        " UNION ALL " +
                        "(SELECT * FROM m WHERE comp0 = 10 AND this IS NULL)",
                asList(BIGINT, VARCHAR),
                table
        ).getPhysical();

        assertPlan(root, plan(
                planRow(0, UnionPhysicalRel.class),
                planRow(1, FullScanPhysicalRel.class),
                planRow(1, FullScanPhysicalRel.class)
        ));

        query = HazelcastRelMetadataQuery.reuseOrCreate(RelMetadataQuery.instance());
        Map<String, List<Map<String, RexNode>>> prunability = query.extractPrunability(root);
        assertEquals(Map.of(), prunability);
    }

    static class CompoundKey {
        public Integer comp0;
        public Integer comp1;

        CompoundKey(Integer comp0, Integer comp1) {
            this.comp0 = comp0;
            this.comp1 = comp1;
        }
    }
}
