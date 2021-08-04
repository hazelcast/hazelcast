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

package com.hazelcast.sql.impl.calcite.opt.physical.index;

import com.hazelcast.config.IndexType;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.calcite.HazelcastSqlBackend;
import com.hazelcast.sql.impl.calcite.OptimizerContext;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTraitDef;
import com.hazelcast.sql.impl.calcite.opt.logical.MapScanLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastRelOptTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchemaUtils;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.validate.operators.predicate.HazelcastComparisonPredicate;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;
import static org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(ParallelJVMTest.class)
public class PhysicalIndexScanTest extends IndexOptimizerTestSupport {

    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        HazelcastTable pTable = partitionedTable(
                "p",
                fields("ret", INT, "f1", INT, "f2", INT, "f3", INT),
                Arrays.asList(
                        new MapTableIndex("sorted_f1", IndexType.SORTED, 1, singletonList(1), singletonList(INT)),
                        new MapTableIndex("sorted_f1_f3", IndexType.SORTED, 2, asList(1, 3), asList(INT, INT)),
                        new MapTableIndex("sorted_f1_f2", IndexType.HASH, 2, asList(1, 2), asList(INT, INT))
                ),
                100,
                false
        );

        tableMap.put("p", pTable);

        return new HazelcastSchema(tableMap);
    }

    protected HazelcastSchema createSchema(List<MapTableIndex> indexes) {
        Map<String, Table> tableMap = new HashMap<>();

        HazelcastTable pTable = partitionedTable(
                "p",
                fields("ret", INT, "f1", INT, "f2", INT, "f3", INT),
                indexes,
                100,
                false
        );

        tableMap.put("p", pTable);

        return new HazelcastSchema(tableMap);
    }

    @Test
    public void testNoIndexes() {
        Collection<RelNode> rels = createIndexScans(Collections.emptyList(), makeFiler(1));
        assertEquals(0, rels.size());
    }

    @Test
    public void testFullScanOneIndex() {
        Collection<RelNode> rels = createIndexScans(
                Arrays.asList(
                        new MapTableIndex("sorted_f1", IndexType.SORTED, 1, singletonList(1), singletonList(INT))
                ),
                null);

        assertEquals(2, rels.size());
        List<RelNode> orderedRels = orderRels(rels);

        assertEquals(MapIndexScanPhysicalRel.class, orderedRels.get(0).getClass());
        assertEquals(MapIndexScanPhysicalRel.class, orderedRels.get(1).getClass());

        MapIndexScanPhysicalRel scan1 = (MapIndexScanPhysicalRel) orderedRels.get(0);
        assertEquals("sorted_f1", scan1.getIndex().getName());
        RelCollation collation1 = getScanCollation(scan1);
        assertEquals(1, collation1.getFieldCollations().size());
        RelFieldCollation fieldCollation = collation1.getFieldCollations().get(0);
        assertEquals(1, fieldCollation.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation.getDirection());

        MapIndexScanPhysicalRel scan2 = (MapIndexScanPhysicalRel) orderedRels.get(1);
        assertEquals("sorted_f1", scan1.getIndex().getName());
        RelCollation collation2 = getScanCollation(scan2);
        assertEquals(1, collation2.getFieldCollations().size());
        RelFieldCollation fieldCollation2 = collation2.getFieldCollations().get(0);
        assertEquals(1, fieldCollation2.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation2.getDirection());
    }

    @Test
    public void testFullScanManyIndexes() {
        Collection<RelNode> rels = createIndexScans(
                Arrays.asList(
                        new MapTableIndex("sorted_f1", IndexType.SORTED, 1, singletonList(1), singletonList(INT)),
                        new MapTableIndex("sorted_f1_f2", IndexType.SORTED, 2, Arrays.asList(1, 2), Arrays.asList(INT, INT)),
                        new MapTableIndex("sorted_f1_f2_f3", IndexType.SORTED, 3, Arrays.asList(1, 2, 3), Arrays.asList(INT, INT, INT)),
                        new MapTableIndex("sorted_f2_f3", IndexType.SORTED, 2, Arrays.asList(2, 3), Arrays.asList(INT, INT))
                ),
                null);

        // sorted_f1 and sorted_f1_f2_f3 covered
        assertEquals(4, rels.size());
        List<RelNode> orderedRels = orderRels(rels);

        assertMapIndexScanPhysicalRel(orderedRels);

        MapIndexScanPhysicalRel scan1 = (MapIndexScanPhysicalRel) orderedRels.get(0);
        assertEquals("sorted_f1_f2_f3", scan1.getIndex().getName());
        RelCollation collation = getScanCollation(scan1);
        assertEquals(3, collation.getFieldCollations().size());
        RelFieldCollation fieldCollation1 = collation.getFieldCollations().get(0);
        assertEquals(1, fieldCollation1.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation1.getDirection());
        RelFieldCollation fieldCollation2 = collation.getFieldCollations().get(1);
        assertEquals(2, fieldCollation2.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation2.getDirection());
        RelFieldCollation fieldCollation3 = collation.getFieldCollations().get(2);
        assertEquals(3, fieldCollation3.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation3.getDirection());

        MapIndexScanPhysicalRel scan2 = (MapIndexScanPhysicalRel) orderedRels.get(1);
        assertEquals("sorted_f1_f2_f3", scan1.getIndex().getName());
        collation = getScanCollation(scan2);
        assertEquals(3, collation.getFieldCollations().size());
        fieldCollation1 = collation.getFieldCollations().get(0);
        assertEquals(1, fieldCollation1.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation1.getDirection());
        fieldCollation2 = collation.getFieldCollations().get(1);
        assertEquals(2, fieldCollation2.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation2.getDirection());
        fieldCollation3 = collation.getFieldCollations().get(2);
        assertEquals(3, fieldCollation3.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation3.getDirection());

        MapIndexScanPhysicalRel scan3 = (MapIndexScanPhysicalRel) orderedRels.get(2);
        assertEquals("sorted_f2_f3", scan3.getIndex().getName());
        collation = getScanCollation(scan3);
        assertEquals(2, collation.getFieldCollations().size());
        fieldCollation1 = collation.getFieldCollations().get(0);
        assertEquals(2, fieldCollation1.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation1.getDirection());
        fieldCollation2 = collation.getFieldCollations().get(1);
        assertEquals(3, fieldCollation2.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation2.getDirection());

        MapIndexScanPhysicalRel scan4 = (MapIndexScanPhysicalRel) orderedRels.get(3);
        assertEquals("sorted_f2_f3", scan4.getIndex().getName());
        collation = getScanCollation(scan4);
        assertEquals(2, collation.getFieldCollations().size());
        fieldCollation1 = collation.getFieldCollations().get(0);
        assertEquals(2, fieldCollation1.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation1.getDirection());
        fieldCollation2 = collation.getFieldCollations().get(1);
        assertEquals(3, fieldCollation2.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation2.getDirection());
    }

    @Test
    public void testFullScanManyIndexesWithSimpleFilter() {
        Collection<RelNode> rels = createIndexScans(
                Arrays.asList(
                        new MapTableIndex("hash_f1", IndexType.HASH, 1, singletonList(1), singletonList(INT)),
                        new MapTableIndex("sorted_f1_f2_f3", IndexType.SORTED, 3, Arrays.asList(1, 2, 3), Arrays.asList(INT, INT, INT)),
                        new MapTableIndex("sorted_f2_f3", IndexType.SORTED, 2, Arrays.asList(2, 3), Arrays.asList(INT, INT))
                ),
                makeFiler(1));

        // sorted_f1 and sorted_f1_f2_f3 covered
        assertEquals(5, rels.size());
        List<RelNode> orderedRels = orderRels(rels);

        assertMapIndexScanPhysicalRel(orderedRels);

        MapIndexScanPhysicalRel scan1 = (MapIndexScanPhysicalRel) orderedRels.get(0);
        assertEquals("hash_f1", scan1.getIndex().getName());
        RelCollation collation = getScanCollation(scan1);
        assertEquals(0, collation.getFieldCollations().size());
        assertNotNull(scan1.getIndexFilter());
        assertEquals(IndexEqualsFilter.class, scan1.getIndexFilter().getClass());

        MapIndexScanPhysicalRel scan2 = (MapIndexScanPhysicalRel) orderedRels.get(1);
        assertEquals("sorted_f1_f2_f3", scan2.getIndex().getName());
        collation = getScanCollation(scan2);
        assertEquals(IndexRangeFilter.class, scan2.getIndexFilter().getClass());
        assertEquals(3, collation.getFieldCollations().size());
        RelFieldCollation fieldCollation1 = collation.getFieldCollations().get(0);
        assertEquals(1, fieldCollation1.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation1.getDirection());
        RelFieldCollation fieldCollation2 = collation.getFieldCollations().get(1);
        assertEquals(2, fieldCollation2.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation2.getDirection());
        RelFieldCollation fieldCollation3 = collation.getFieldCollations().get(2);
        assertEquals(3, fieldCollation3.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation3.getDirection());

        MapIndexScanPhysicalRel scan3 = (MapIndexScanPhysicalRel) orderedRels.get(2);
        assertEquals("sorted_f1_f2_f3", scan3.getIndex().getName());
        assertNotNull(scan3.getIndexFilter());
        assertEquals(IndexRangeFilter.class, scan3.getIndexFilter().getClass());
        collation = getScanCollation(scan3);
        assertEquals(3, collation.getFieldCollations().size());
        fieldCollation1 = collation.getFieldCollations().get(0);
        assertEquals(1, fieldCollation1.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation1.getDirection());
        fieldCollation2 = collation.getFieldCollations().get(1);
        assertEquals(2, fieldCollation2.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation2.getDirection());
        fieldCollation3 = collation.getFieldCollations().get(2);
        assertEquals(3, fieldCollation3.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation3.getDirection());

        MapIndexScanPhysicalRel scan4 = (MapIndexScanPhysicalRel) orderedRels.get(3);
        assertEquals("sorted_f2_f3", scan4.getIndex().getName());
        collation = getScanCollation(scan4);
        assertNull(scan4.getIndexFilter());
        assertEquals(2, collation.getFieldCollations().size());
        fieldCollation2 = collation.getFieldCollations().get(0);
        assertEquals(2, fieldCollation2.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation2.getDirection());
        fieldCollation3 = collation.getFieldCollations().get(1);
        assertEquals(3, fieldCollation3.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation3.getDirection());

        MapIndexScanPhysicalRel scan5 = (MapIndexScanPhysicalRel) orderedRels.get(4);
        assertEquals("sorted_f2_f3", scan5.getIndex().getName());
        collation = getScanCollation(scan5);
        assertNull(scan5.getIndexFilter());
        assertEquals(2, collation.getFieldCollations().size());
        fieldCollation2 = collation.getFieldCollations().get(0);
        assertEquals(2, fieldCollation2.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation2.getDirection());
        fieldCollation3 = collation.getFieldCollations().get(1);
        assertEquals(3, fieldCollation3.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation3.getDirection());
    }

    @Test
    public void testFullScanManyIndexesWithComplexFilter() {
        Collection<RelNode> rels = createIndexScans(
                Arrays.asList(
                        new MapTableIndex("hash_f1", IndexType.HASH, 1, singletonList(1), singletonList(INT)),
                        new MapTableIndex("sorted_f2", IndexType.SORTED, 1, singletonList(2), singletonList(INT)),
                        new MapTableIndex("sorted_f1_f2_f3", IndexType.SORTED, 3, Arrays.asList(1, 2, 3), Arrays.asList(INT, INT, INT)),
                        new MapTableIndex("sorted_f2_f3", IndexType.SORTED, 2, Arrays.asList(2, 3), Arrays.asList(INT, INT))
                ),
                makeAndFilter());

        assertEquals(7, rels.size());
        List<RelNode> orderedRels = orderRels(rels);

        assertMapIndexScanPhysicalRel(orderedRels);

        MapIndexScanPhysicalRel scan1 = (MapIndexScanPhysicalRel) orderedRels.get(0);
        assertEquals("hash_f1", scan1.getIndex().getName());
        RelCollation collation = getScanCollation(scan1);
        assertEquals(0, collation.getFieldCollations().size());
        assertNotNull(scan1.getIndexFilter());
        assertEquals(IndexEqualsFilter.class, scan1.getIndexFilter().getClass());

        MapIndexScanPhysicalRel scan2 = (MapIndexScanPhysicalRel) orderedRels.get(1);
        assertEquals("sorted_f1_f2_f3", scan2.getIndex().getName());
        collation = getScanCollation(scan2);
        assertEquals(IndexRangeFilter.class, scan2.getIndexFilter().getClass());
        assertEquals(3, collation.getFieldCollations().size());
        RelFieldCollation fieldCollation1 = collation.getFieldCollations().get(0);
        assertEquals(1, fieldCollation1.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation1.getDirection());
        RelFieldCollation fieldCollation2 = collation.getFieldCollations().get(1);
        assertEquals(2, fieldCollation2.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation2.getDirection());
        RelFieldCollation fieldCollation3 = collation.getFieldCollations().get(2);
        assertEquals(3, fieldCollation3.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation3.getDirection());

        MapIndexScanPhysicalRel scan3 = (MapIndexScanPhysicalRel) orderedRels.get(2);
        assertEquals("sorted_f1_f2_f3", scan3.getIndex().getName());
        assertNotNull(scan3.getIndexFilter());
        assertEquals(IndexRangeFilter.class, scan3.getIndexFilter().getClass());
        collation = getScanCollation(scan3);
        assertEquals(3, collation.getFieldCollations().size());
        fieldCollation1 = collation.getFieldCollations().get(0);
        assertEquals(1, fieldCollation1.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation1.getDirection());
        fieldCollation2 = collation.getFieldCollations().get(1);
        assertEquals(2, fieldCollation2.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation2.getDirection());
        fieldCollation3 = collation.getFieldCollations().get(2);
        assertEquals(3, fieldCollation3.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation3.getDirection());

        MapIndexScanPhysicalRel scan4 = (MapIndexScanPhysicalRel) orderedRels.get(3);
        assertEquals("sorted_f2", scan4.getIndex().getName());
        collation = getScanCollation(scan4);
        assertEquals(IndexEqualsFilter.class, scan4.getIndexFilter().getClass());
        assertEquals(1, collation.getFieldCollations().size());
        fieldCollation2 = collation.getFieldCollations().get(0);
        assertEquals(2, fieldCollation2.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation2.getDirection());

        MapIndexScanPhysicalRel scan5 = (MapIndexScanPhysicalRel) orderedRels.get(4);
        assertEquals("sorted_f2", scan5.getIndex().getName());
        collation = getScanCollation(scan5);
        assertEquals(IndexEqualsFilter.class, scan5.getIndexFilter().getClass());
        assertEquals(1, collation.getFieldCollations().size());
        fieldCollation2 = collation.getFieldCollations().get(0);
        assertEquals(2, fieldCollation2.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation2.getDirection());

        MapIndexScanPhysicalRel scan6 = (MapIndexScanPhysicalRel) orderedRels.get(5);
        assertEquals("sorted_f2_f3", scan6.getIndex().getName());
        collation = getScanCollation(scan6);
        assertEquals(IndexRangeFilter.class, scan6.getIndexFilter().getClass());
        assertEquals(2, collation.getFieldCollations().size());
        fieldCollation2 = collation.getFieldCollations().get(0);
        assertEquals(2, fieldCollation2.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation2.getDirection());
        fieldCollation3 = collation.getFieldCollations().get(1);
        assertEquals(3, fieldCollation3.getFieldIndex());
        assertEquals(ASCENDING, fieldCollation3.getDirection());

        MapIndexScanPhysicalRel scan7 = (MapIndexScanPhysicalRel) orderedRels.get(6);
        assertEquals("sorted_f2_f3", scan7.getIndex().getName());
        collation = getScanCollation(scan7);
        assertEquals(IndexRangeFilter.class, scan7.getIndexFilter().getClass());
        assertEquals(2, collation.getFieldCollations().size());
        fieldCollation2 = collation.getFieldCollations().get(0);
        assertEquals(2, fieldCollation2.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation2.getDirection());
        fieldCollation3 = collation.getFieldCollations().get(1);
        assertEquals(3, fieldCollation3.getFieldIndex());
        assertEquals(DESCENDING, fieldCollation3.getDirection());
    }

    @Test
    public void testRelCollationComparator() {
        RelCollationComparator comparator = RelCollationComparator.INSTANCE;

        RelCollation coll1 = RelCollations.of(new RelFieldCollation(0, DESCENDING));

        RelCollation coll2 = RelCollations.of(Collections.emptyList());

        int cmp = comparator.compare(coll1, coll2);
        assertTrue(cmp > 0);
        cmp = comparator.compare(coll2, coll1);
        assertTrue(cmp < 0);

        coll2 = RelCollations.of(new RelFieldCollation(0, DESCENDING));
        cmp = comparator.compare(coll1, coll2);
        assertTrue(cmp == 0);

        coll2 = RelCollations.of(new RelFieldCollation(0, ASCENDING));
        cmp = comparator.compare(coll1, coll2);
        assertTrue(cmp > 0);

        coll2 = RelCollations.of(new RelFieldCollation(1, DESCENDING));
        cmp = comparator.compare(coll1, coll2);
        assertTrue(cmp < 0);

        coll2 = RelCollations.of(new RelFieldCollation(0, DESCENDING),
                new RelFieldCollation(1, ASCENDING));
        cmp = comparator.compare(coll2, coll1);
        assertTrue(cmp > 0);

        coll1 = RelCollations.of(new RelFieldCollation(0, DESCENDING),
                new RelFieldCollation(1, DESCENDING));

        coll2 = RelCollations.of(new RelFieldCollation(0, DESCENDING),
                new RelFieldCollation(1, DESCENDING),
                new RelFieldCollation(2, ASCENDING));

        // prefix
        cmp = comparator.compare(coll1, coll2);
        assertTrue(cmp < 0);

        coll1 = RelCollations.of(new RelFieldCollation(0, DESCENDING),
                new RelFieldCollation(3, DESCENDING));

        coll2 = RelCollations.of(new RelFieldCollation(0, DESCENDING),
                new RelFieldCollation(1, DESCENDING),
                new RelFieldCollation(2, ASCENDING));

        cmp = comparator.compare(coll1, coll2);
        assertTrue(cmp > 0);

        cmp = comparator.compare(coll2, coll2);
        assertTrue(cmp == 0);
    }

    private Collection<RelNode> createIndexScans(List<MapTableIndex> indexes, RexNode filter) {
        HazelcastSchema schema = createSchema(indexes);

        DistributionTraitDef distributionTraitDef = new DistributionTraitDef(2);
        DistributionTrait distributionTrait = distributionTraitDef.getTraitReplicated();

        MapScanLogicalRel scan = createMapScanLogicalRel(schema, filter);

        HazelcastTable table = (HazelcastTable) schema.getTable("p");
        PartitionedMapTable pTable = table.getTarget();
        return IndexResolver.createIndexScans(scan, distributionTrait, pTable.getIndexes());
    }

    private MapScanLogicalRel createMapScanLogicalRel(HazelcastSchema schema, RexNode filter) {
        OptimizerContext context = OptimizerContext.create(
                HazelcastSchemaUtils.createCatalog(schema),
                QueryUtils.prepareSearchPaths(null, null),
                emptyList(),
                2,
                new HazelcastSqlBackend(null),
                null
        );

        RelOptCluster cluster = context.getCluster();

        RelTraitSet traitSet = OptUtils.traitPlus(RelTraitSet.createEmpty(),
                buildCollationTrait()
        );

        HazelcastRelOptTable relOptTable =
                (HazelcastRelOptTable) context.getCatalogReader().getTable(Collections.singletonList("p"));

        RelOptTable newRelTable = OptUtils.createRelTable(
                relOptTable,
                relOptTable.unwrap(HazelcastTable.class).withFilter(filter),
                cluster.getTypeFactory()
        );

        return new MapScanLogicalRel(cluster, traitSet,
                newRelTable);
    }

    private static RelCollation getScanCollation(MapIndexScanPhysicalRel scan) {
        return scan.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
    }

    private static RelCollation buildCollationTrait() {
        List<RelFieldCollation> fields = new ArrayList<>(1);
        RelFieldCollation fieldCollation = new RelFieldCollation(0);
        fields.add(fieldCollation);
        return RelCollations.of(fields);
    }

    private RexNode makeFiler(int fieldIndex) {
        RelDataTypeFactory typeFactory = HazelcastTypeFactory.INSTANCE;
        RexBuilder rexBuild = new RexBuilder(typeFactory);

        HazelcastIntegerType type = HazelcastIntegerType.create(Integer.SIZE, false);
        RexInputRef operand1 = new RexInputRef(fieldIndex, type);
        RexLiteral operand2 = rexBuild.makeBigintLiteral(BigDecimal.ONE);

        HazelcastComparisonPredicate predicate1 = HazelcastComparisonPredicate.EQUALS;

        RelDataType booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

        return rexBuild.makeCall(booleanType, predicate1, asList(operand1, operand2));
    }

    private RexNode makeAndFilter() {
        RexNode operand1 = makeFiler(1);
        RexNode operand2 = makeFiler(2);

        RelDataTypeFactory typeFactory = HazelcastTypeFactory.INSTANCE;
        RexBuilder rexBuild = new RexBuilder(typeFactory);

        RelDataType booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

        return rexBuild.makeCall(booleanType, SqlStdOperatorTable.AND, asList(operand1, operand2));
    }

    /**
     * Order rels by [indexName, descending flag, indexExpr]
     *
     * @param rels the rels
     * @return the ordered rels
     */
    private List<RelNode> orderRels(Collection<RelNode> rels) {
        RelNode[] relsArray = rels.toArray(new RelNode[rels.size()]);
        Arrays.sort(relsArray, (r1, r2) -> {
            MapIndexScanPhysicalRel scan1 = (MapIndexScanPhysicalRel) r1;
            MapIndexScanPhysicalRel scan2 = (MapIndexScanPhysicalRel) r2;

            String indexName1 = scan1.getIndex().getName();
            String indexName2 = scan2.getIndex().getName();

            int cmp = indexName1.compareTo(indexName2);
            if (cmp == 0) {
                RelCollation collation1 = scan1.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
                RelCollation collation2 = scan2.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

                if (collation1 == null) {
                    if (collation2 != null) {
                        return -1;
                    }
                }

                if (collation2 == null) {
                    if (collation1 != null) {
                        return 1;
                    }
                }

                cmp = compare(collation1, collation2);

                if (cmp == 0) {
                    IndexFilter filter1 = scan1.getIndexFilter();
                    IndexFilter filter2 = scan1.getIndexFilter();
                    if (filter1 == null) {
                        assertTrue(filter2 != null);
                        return -1;
                    }
                    if (filter2 == null) {
                        return 1;
                    }

                    return filter1.toString().compareTo(filter2.toString());
                }

            }
            return cmp;
        });
        return Arrays.asList(relsArray);
    }

    private int compare(RelCollation coll1, RelCollation coll2) {
        if (coll1 == null && coll2 == null) {
            return 0;
        }
        if (coll1 == null) {
            return -1;
        }

        if (coll2 == null) {
            return 1;
        }

        int size1 = coll1.getFieldCollations().size();
        int size2 = coll2.getFieldCollations().size();

        int cmp = Integer.compare(size1, size2);

        if (cmp == 0) {
            for (int i = 0; i < size1; ++i) {
                RelFieldCollation field1 = coll1.getFieldCollations().get(i);
                RelFieldCollation field2 = coll2.getFieldCollations().get(i);
                cmp = Integer.compare(field1.getFieldIndex(), field2.getFieldIndex());
                if (cmp != 0) {
                    return cmp;
                }

                cmp = Boolean.compare(field1.getDirection().isDescending(), field2.getDirection().isDescending());
                if (cmp != 0) {
                    return cmp;
                }
            }
        }
        return cmp;
    }

    private void assertMapIndexScanPhysicalRel(List<RelNode> orderedRels) {
        for (RelNode relNode : orderedRels) {
            assertEquals(MapIndexScanPhysicalRel.class, relNode.getClass());
        }
    }
}
