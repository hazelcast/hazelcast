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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.internal.util.MutableByte;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.connector.test.TestAbstractSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.WatermarkKeysAssigner;
import com.hazelcast.jet.sql.impl.opt.physical.AggregateAccumulateByKeyPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.AggregateCombineByKeyPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.CalcPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.DropLateItemsPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowAggregatePhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.StreamToStreamJoinPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.UnionPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.schema.RelationsStorage;
import com.hazelcast.jet.sql.impl.schema.TableResolverImpl;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.RelNode;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.BIGINT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WatermarkKeysAssignerTest extends OptimizerTestSupport {
    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void when_scanAndDropArePresent_then_keyWasAssigned() {
        HazelcastTable table = partitionedTable("map", asList(field(KEY, INT), field(VALUE, INT)), 1);
        List<QueryDataType> parameterTypes = asList(INT, INT);

        final String sql = "SELECT * FROM TABLE(IMPOSE_ORDER((SELECT __key, this FROM map), DESCRIPTOR(this), 1))";

        PhysicalRel optimizedPhysicalRel = optimizePhysical(sql, parameterTypes, table).getPhysical();

        assertPlan(optimizedPhysicalRel, plan(
                planRow(0, DropLateItemsPhysicalRel.class),
                planRow(1, FullScanPhysicalRel.class)
        ));

        WatermarkKeysAssigner keysAssigner = new WatermarkKeysAssigner(optimizedPhysicalRel);
        keysAssigner.assignWatermarkKeys();

        // Watermark key was propagated to DropLateItemsPhysicalRel
        Map<Integer, MutableByte> map = keysAssigner.getWatermarkedFieldsKey(optimizedPhysicalRel);
        assertThat(map).isNotNull();
        assertThat(map).isNotEmpty();
        assertThat(map.get(1)).isNotNull();
        assertThat(map.get(1).getValue()).isEqualTo((byte) 0);

        // Watermark key on FullScan was assigned correctly.
        map = keysAssigner.getWatermarkedFieldsKey(optimizedPhysicalRel.getInput(0));
        assertThat(map).isNotNull();
        assertThat(map).isNotEmpty();
        assertThat(map.get(1)).isNotNull(); // 2nd field (this) is watermarked, that's why we have index 1.
        assertThat(map.get(1).getValue()).isEqualTo((byte) 0);
    }

    @Test
    public void when_calcIsPresent_then_keyWasPropagated() {
        HazelcastTable table = partitionedTable("map", asList(field(KEY, INT), field(VALUE, INT)), 1);
        List<QueryDataType> parameterTypes = asList(INT, INT);

        final String sql = "SELECT window_end FROM " +
                "TABLE(HOP(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER((SELECT __key, this FROM map), DESCRIPTOR(__key), 1))), " +
                "DESCRIPTOR(__key), 2, 1)) " +
                "GROUP BY window_start, window_end, __key, this";

        PhysicalRel optimizedPhysicalRel = optimizePhysical(sql, parameterTypes, table).getPhysical();

        assertPlan(optimizedPhysicalRel, plan(
                planRow(0, CalcPhysicalRel.class),
                planRow(1, AggregateCombineByKeyPhysicalRel.class),
                planRow(2, AggregateAccumulateByKeyPhysicalRel.class),
                planRow(3, CalcPhysicalRel.class),
                planRow(4, SlidingWindowPhysicalRel.class),
                planRow(5, FullScanPhysicalRel.class)
        ));

        WatermarkKeysAssigner keysAssigner = new WatermarkKeysAssigner(optimizedPhysicalRel);
        keysAssigner.assignWatermarkKeys();

        assertThat(optimizedPhysicalRel).isInstanceOf(CalcPhysicalRel.class);
        // upper-level calc should not contain any w-marked field
        Map<Integer, MutableByte> map = keysAssigner.getWatermarkedFieldsKey(optimizedPhysicalRel);
        assertThat(map).isNull();

        RelNode rel = optimizedPhysicalRel.getInput(0);
        while (!(rel instanceof CalcPhysicalRel)) {
            rel = rel.getInput(0);
        }

        assertThat(rel).isInstanceOf(CalcPhysicalRel.class);
        map = keysAssigner.getWatermarkedFieldsKey(rel);
        assertThat(map).isNotNull();
        assertThat(map.size()).isEqualTo(1);
        assertThat(map.get(2).getValue()).isEqualTo((byte) 0);
    }

    @Test
    public void when_unionIsPresent_then_keyWasPropagated() {
        NodeEngine nodeEngine = getNodeEngine(instance());
        TableResolverImpl resolver = new TableResolverImpl(
                nodeEngine,
                new RelationsStorage(nodeEngine),
                new SqlConnectorCache(nodeEngine));

        TestStreamSqlConnector.create(
                instance().getSql(),
                "s",
                singletonList("a"),
                singletonList(BIGINT),
                row(1L)
        );

        assertInstanceOf(TestAbstractSqlConnector.TestTable.class, resolver.getTables().get(0));

        HazelcastTable table = streamingTable(resolver.getTables().get(0), 1L);

        String sql = "(SELECT * FROM TABLE(IMPOSE_ORDER((SELECT * FROM s), DESCRIPTOR(a), 1)))" +
                " UNION ALL" +
                " (SELECT * FROM TABLE(IMPOSE_ORDER((SELECT * FROM s), DESCRIPTOR(a), 1)))";

        PhysicalRel optPhysicalRel = optimizePhysical(sql, singletonList(QueryDataType.BIGINT), table).getPhysical();

        assertPlan(optPhysicalRel, plan(
                planRow(0, DropLateItemsPhysicalRel.class),
                planRow(1, UnionPhysicalRel.class),
                planRow(2, FullScanPhysicalRel.class),
                planRow(2, FullScanPhysicalRel.class)
        ));

        assertThat(OptUtils.isUnbounded(optPhysicalRel)).isTrue();
        PhysicalRel finalOptRel = CalciteSqlOptimizer.postOptimizationRewrites(optPhysicalRel);

        WatermarkKeysAssigner keysAssigner = new WatermarkKeysAssigner(finalOptRel);
        keysAssigner.assignWatermarkKeys();

        assertThat(finalOptRel.getInput(0)).isInstanceOf(UnionPhysicalRel.class);
        UnionPhysicalRel unionRel = (UnionPhysicalRel) finalOptRel.getInput(0);

        // Watermark key was propagated to UnionPhysicalRel
        Map<Integer, MutableByte> map = keysAssigner.getWatermarkedFieldsKey(unionRel);
        assertThat(map).isNotNull();
        assertThat(map).isNotEmpty();
        assertThat(map.get(0)).isNotNull(); // 1st field (this) is watermarked, that's why we have index 0.
        assertThat(map.get(0).getValue()).isEqualTo((byte) 0);

        Map<Integer, MutableByte> leftInputKeys = keysAssigner.getWatermarkedFieldsKey(unionRel.getInput(0));
        Map<Integer, MutableByte> rightInputKeys = keysAssigner.getWatermarkedFieldsKey(unionRel.getInput(1));

        assertThat(leftInputKeys.values().iterator().next().getValue()).isEqualTo((byte) 0);
        assertThat(rightInputKeys.values().iterator().next().getValue()).isEqualTo((byte) 0);
    }

    @Test
    public void when_streamToStreamJoinIsPresent_then_keyWasPropagated() {
        NodeEngine nodeEngine = getNodeEngine(instance());
        TableResolverImpl resolver = new TableResolverImpl(
                nodeEngine,
                new RelationsStorage(nodeEngine),
                new SqlConnectorCache(nodeEngine));

        String stream = "s";
        TestStreamSqlConnector.create(
                instance().getSql(),
                stream,
                singletonList("a"),
                singletonList(BIGINT),
                row(1L)
        );

        HazelcastTable table = streamingTable(resolver.getTables().get(0), 1L);

        String sql = "SELECT * FROM " +
                "(SELECT * FROM TABLE(IMPOSE_ORDER(TABLE s, DESCRIPTOR(a), 1))) s1 " +
                " INNER JOIN " +
                "(SELECT * FROM TABLE(IMPOSE_ORDER(TABLE s, DESCRIPTOR(a), 1))) s2 " +
                " ON s1.a = s2.a";

        PhysicalRel optPhysicalRel = optimizePhysical(sql, singletonList(QueryDataType.BIGINT), table).getPhysical();

        assertPlan(optPhysicalRel, plan(
                planRow(0, StreamToStreamJoinPhysicalRel.class),
                planRow(1, FullScanPhysicalRel.class),
                planRow(1, FullScanPhysicalRel.class)
        ));

        assertThat(OptUtils.isUnbounded(optPhysicalRel)).isTrue();
        PhysicalRel finalOptRel = CalciteSqlOptimizer.postOptimizationRewrites(optPhysicalRel);

        WatermarkKeysAssigner keysAssigner = new WatermarkKeysAssigner(finalOptRel);
        keysAssigner.assignWatermarkKeys();

        assertThat(finalOptRel).isInstanceOf(StreamToStreamJoinPhysicalRel.class);

        // Watermark key was propagated to StreamToStreamJoinPhysicalRel.
        Map<Integer, MutableByte> map = keysAssigner.getWatermarkedFieldsKey(finalOptRel);
        assertThat(map).isNotNull();
        assertThat(map).isNotEmpty();
        assertThat(map.get(0)).isNotNull();
        assertThat(map.get(0).getValue()).isEqualTo((byte) 0);
    }

    @Test
    public void when_s2sJoinWithWindowAggregationIsPresent_then_keyWasPropagated() {
        NodeEngine nodeEngine = getNodeEngine(instance());
        TableResolverImpl resolver = new TableResolverImpl(
                nodeEngine,
                new RelationsStorage(nodeEngine),
                new SqlConnectorCache(nodeEngine));

        String stream = "s";
        TestStreamSqlConnector.create(
                instance().getSql(),
                stream,
                singletonList("a"),
                singletonList(BIGINT),
                row(1L)
        );

        HazelcastTable table = streamingTable(resolver.getTables().get(0), 1L);

        String sql = "SELECT * FROM " +
                "( SELECT window_end, AVG(a) AS price FROM " +
                "    TABLE(HOP((SELECT * FROM TABLE(IMPOSE_ORDER(TABLE s, DESCRIPTOR(a), 1))), DESCRIPTOR(a), 4, 1))" +
                "    GROUP BY window_end, a) s1" +
                "  RIGHT JOIN " +
                "( SELECT window_end, AVG(a) AS price FROM " +
                "    TABLE(HOP((SELECT * FROM TABLE(IMPOSE_ORDER(TABLE s, DESCRIPTOR(a), 1))), DESCRIPTOR(a), 4, 1))" +
                "    GROUP BY window_end, a) s2" +
                " ON s1.window_end = s2.window_end";

        PhysicalRel optPhysicalRel = optimizePhysical(sql, singletonList(QueryDataType.BIGINT), table).getPhysical();

        assertPlan(optPhysicalRel, plan(
                planRow(0, CalcPhysicalRel.class),
                planRow(1, StreamToStreamJoinPhysicalRel.class),
                planRow(2, SlidingWindowAggregatePhysicalRel.class),
                planRow(3, CalcPhysicalRel.class),
                planRow(4, FullScanPhysicalRel.class),
                planRow(2, SlidingWindowAggregatePhysicalRel.class),
                planRow(3, CalcPhysicalRel.class),
                planRow(4, FullScanPhysicalRel.class)
        ));

        assertThat(OptUtils.isUnbounded(optPhysicalRel)).isTrue();
        PhysicalRel finalOptRel = CalciteSqlOptimizer.postOptimizationRewrites(optPhysicalRel);

        WatermarkKeysAssigner keysAssigner = new WatermarkKeysAssigner(finalOptRel);
        keysAssigner.assignWatermarkKeys();

        // Watermark key was propagated to StreamToStreamJoinPhysicalRel
        Map<Integer, MutableByte> map = keysAssigner.getWatermarkedFieldsKey(finalOptRel);
        assertThat(map).isNotNull();
        assertThat(map).isNotEmpty();
        assertThat(map.size()).isEqualTo(2);
        assertThat(map.get(0)).isNotNull();
        assertThat(map.get(0).getValue()).isEqualTo((byte) 0);
        assertThat(map.get(2)).isNotNull();
        assertThat(map.get(2).getValue()).isEqualTo((byte) 1);

        // Test also checks watermark keys are different in left and right SlidingWindowAggregatePhysicalRel:
        SlidingWindowAggregatePhysicalRel leftAgg = (SlidingWindowAggregatePhysicalRel) finalOptRel.getInput(0).getInput(0);
        SlidingWindowAggregatePhysicalRel rightAgg = (SlidingWindowAggregatePhysicalRel) finalOptRel.getInput(0).getInput(1);
        Map<Integer, MutableByte> leftAggMap = keysAssigner.getWatermarkedFieldsKey(leftAgg);
        Map<Integer, MutableByte> rightAggMap = keysAssigner.getWatermarkedFieldsKey(rightAgg);

        assertThat(leftAggMap).isNotNull();
        assertThat(rightAggMap).isNotNull();

        assertThat(leftAggMap.size()).isOne();
        assertThat(rightAggMap.size()).isOne();

        // Here we need to ensure that watermark keys are different.
        // These keys are assigning in CreateTopLevelDagVisitor#onSlidingWindowAggregate
        assertThat(leftAggMap.get(0).getValue()).isEqualTo((byte) 0);
        assertThat(rightAggMap.get(0).getValue()).isEqualTo((byte) 1);
    }

    @Test
    public void when_slidingWindowAggregationIsPresent_andWindowEndProjects_then_keyWasPropagated() {
        NodeEngine nodeEngine = getNodeEngine(instance());
        TableResolverImpl resolver = new TableResolverImpl(
                nodeEngine,
                new RelationsStorage(nodeEngine),
                new SqlConnectorCache(nodeEngine));

        String stream = "s";
        TestStreamSqlConnector.create(
                instance().getSql(),
                stream,
                singletonList("a"),
                singletonList(BIGINT),
                row(1L)
        );

        HazelcastTable table = streamingTable(resolver.getTables().get(0), 1L);

        String sql = "SELECT window_end, AVG(a) AS price FROM " +
                "     TABLE(HOP((SELECT * FROM TABLE(IMPOSE_ORDER(TABLE s, DESCRIPTOR(a), 1))), DESCRIPTOR(a), 4, 1))" +
                "     GROUP BY window_end";

        PhysicalRel optPhysicalRel = optimizePhysical(sql, singletonList(QueryDataType.BIGINT), table).getPhysical();

        assertPlan(optPhysicalRel, plan(
                planRow(0, SlidingWindowAggregatePhysicalRel.class),
                planRow(1, CalcPhysicalRel.class),
                planRow(2, FullScanPhysicalRel.class)
        ));

        assertThat(OptUtils.isUnbounded(optPhysicalRel)).isTrue();
        PhysicalRel finalOptRel = CalciteSqlOptimizer.postOptimizationRewrites(optPhysicalRel);

        WatermarkKeysAssigner keysAssigner = new WatermarkKeysAssigner(finalOptRel);
        keysAssigner.assignWatermarkKeys();

        // Correct key was propagated to SlidingWindowAggregatePhysicalRel
        SlidingWindowAggregatePhysicalRel swaRel = (SlidingWindowAggregatePhysicalRel) finalOptRel;
        Map<Integer, MutableByte> rootKeyMap = keysAssigner.getWatermarkedFieldsKey(finalOptRel);
        assertThat(rootKeyMap).isNotEmpty();
        assertThat(rootKeyMap.get(swaRel.watermarkedFields().findFirst()).getValue()).isEqualTo((byte) 0);
    }

    @Test
    public void when_slidingWindowAggregationIsPresent_andWindowEndDoesNotProject_then_keyWasNotPropagated() {
        NodeEngine nodeEngine = getNodeEngine(instance());
        TableResolverImpl resolver = new TableResolverImpl(
                nodeEngine,
                new RelationsStorage(nodeEngine),
                new SqlConnectorCache(nodeEngine));

        String stream = "s";
        TestStreamSqlConnector.create(
                instance().getSql(),
                stream,
                singletonList("a"),
                singletonList(BIGINT),
                row(1L)
        );

        HazelcastTable table = streamingTable(resolver.getTables().get(0), 1L);

        String sql = "SELECT window_start, AVG(a) AS price FROM " +
                "     TABLE(HOP((SELECT * FROM TABLE(IMPOSE_ORDER(TABLE s, DESCRIPTOR(a), 1))), DESCRIPTOR(a), 4, 1))" +
                "     GROUP BY window_start";

        PhysicalRel optPhysicalRel = optimizePhysical(sql, singletonList(QueryDataType.BIGINT), table).getPhysical();

        assertPlan(optPhysicalRel, plan(
                planRow(0, SlidingWindowAggregatePhysicalRel.class),
                planRow(1, CalcPhysicalRel.class),
                planRow(2, FullScanPhysicalRel.class)
        ));

        assertThat(OptUtils.isUnbounded(optPhysicalRel)).isTrue();
        PhysicalRel finalOptRel = CalciteSqlOptimizer.postOptimizationRewrites(optPhysicalRel);

        WatermarkKeysAssigner keysAssigner = new WatermarkKeysAssigner(finalOptRel);
        keysAssigner.assignWatermarkKeys();

        // Key was _NOT_ propagated to SlidingWindowAggregatePhysicalRel
        assertThat(keysAssigner.getWatermarkedFieldsKey(finalOptRel)).isEmpty();
    }

    @Test
    public void when_slidingWindowAggregationsAreUnionized_andWindowEndDoesNotProject_then_keyWasNotPropagated() {
        NodeEngine nodeEngine = getNodeEngine(instance());
        TableResolverImpl resolver = new TableResolverImpl(
                nodeEngine,
                new RelationsStorage(nodeEngine),
                new SqlConnectorCache(nodeEngine));

        String stream = "s";
        TestStreamSqlConnector.create(
                instance().getSql(),
                stream,
                singletonList("a"),
                singletonList(BIGINT),
                row(1L)
        );

        HazelcastTable table = streamingTable(resolver.getTables().get(0), 1L);

        String sql =
                "(SELECT window_start, AVG(a) AS price FROM " +
                "     TABLE(HOP((SELECT * FROM TABLE(IMPOSE_ORDER(TABLE s, DESCRIPTOR(a), 1))), DESCRIPTOR(a), 4, 1))" +
                "     GROUP BY window_start)" +
                " UNION ALL " +
                "(SELECT window_start, AVG(a) AS price FROM " +
                "     TABLE(HOP((SELECT * FROM TABLE(IMPOSE_ORDER(TABLE s, DESCRIPTOR(a), 1))), DESCRIPTOR(a), 4, 1))" +
                "     GROUP BY window_start)" +
                " UNION ALL " +
                "(SELECT window_start, AVG(a) AS price FROM " +
                "     TABLE(HOP((SELECT * FROM TABLE(IMPOSE_ORDER(TABLE s, DESCRIPTOR(a), 1))), DESCRIPTOR(a), 4, 1))" +
                "     GROUP BY window_start)" +
                "";

        PhysicalRel optPhysicalRel = optimizePhysical(sql, singletonList(QueryDataType.BIGINT), table).getPhysical();

        assertPlan(optPhysicalRel, plan(
                planRow(0, UnionPhysicalRel.class),
                planRow(1, SlidingWindowAggregatePhysicalRel.class),
                planRow(2, CalcPhysicalRel.class),
                planRow(3, FullScanPhysicalRel.class),
                planRow(1, SlidingWindowAggregatePhysicalRel.class),
                planRow(2, CalcPhysicalRel.class),
                planRow(3, FullScanPhysicalRel.class),
                planRow(1, SlidingWindowAggregatePhysicalRel.class),
                planRow(2, CalcPhysicalRel.class),
                planRow(3, FullScanPhysicalRel.class)
        ));

        assertThat(OptUtils.isUnbounded(optPhysicalRel)).isTrue();
        PhysicalRel finalOptRel = CalciteSqlOptimizer.postOptimizationRewrites(optPhysicalRel);

        WatermarkKeysAssigner keysAssigner = new WatermarkKeysAssigner(finalOptRel);
        keysAssigner.assignWatermarkKeys();

        // Key was _NOT_ propagated to UNION
        assertThat(keysAssigner.getWatermarkedFieldsKey(finalOptRel)).isEmpty();
    }

    @Test
    public void when_slidingWindowIsPresent_then_inputWatermarkedFieldWatermarked() {
        HazelcastTable table = partitionedTable("map", asList(field(KEY, INT), field(VALUE, INT)), 1);
        List<QueryDataType> parameterTypes = asList(INT, INT);

        final String sql = "SELECT window_start, window_end FROM " +
                "TABLE(HOP(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER((SELECT __key, this FROM map), DESCRIPTOR(__key), 1))), " +
                "DESCRIPTOR(__key), 2, 1)) " +
                "GROUP BY window_start, window_end, __key, this";

        PhysicalRel optimizedPhysicalRel = optimizePhysical(sql, parameterTypes, table).getPhysical();

        assertPlan(optimizedPhysicalRel, plan(
                planRow(0, CalcPhysicalRel.class),
                planRow(1, AggregateCombineByKeyPhysicalRel.class),
                planRow(2, AggregateAccumulateByKeyPhysicalRel.class),
                planRow(3, CalcPhysicalRel.class),
                planRow(4, SlidingWindowPhysicalRel.class),
                planRow(5, FullScanPhysicalRel.class)
        ));

        WatermarkKeysAssigner keysAssigner = new WatermarkKeysAssigner(optimizedPhysicalRel);
        keysAssigner.assignWatermarkKeys();

        // check if SlidingWindowPhysicalRel adds window bounds
        PhysicalRel sw = optimizedPhysicalRel;
        while (!(sw instanceof SlidingWindowPhysicalRel)) {
            sw = (PhysicalRel) sw.getInput(0);
        }
        assertThat(sw).isInstanceOf(SlidingWindowPhysicalRel.class);

        Map<Integer, MutableByte> map = keysAssigner.getWatermarkedFieldsKey(sw);
        assertThat(map).isNotNull();
        assertThat(map.size()).isEqualTo(1);
        assertThat(map.get(0).getValue()).isEqualTo((byte) 0); // original watermarked field
    }

    @Test
    public void when_upperRelsDoNotSupportWatermarks_then_justPartOfTreeIsWatermarked() {
        HazelcastTable table = partitionedTable("map", asList(field(KEY, INT), field(VALUE, INT)), 1);
        List<QueryDataType> parameterTypes = asList(INT, INT);

        final String sql = "SELECT window_start, window_end FROM " +
                "TABLE(HOP(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER((SELECT __key, this FROM map), DESCRIPTOR(__key), 1))), " +
                "DESCRIPTOR(__key), 2, 1)) " +
                "GROUP BY window_start, window_end, __key, this";

        PhysicalRel optimizedPhysicalRel = optimizePhysical(sql, parameterTypes, table).getPhysical();

        assertPlan(optimizedPhysicalRel, plan(
                planRow(0, CalcPhysicalRel.class),
                planRow(1, AggregateCombineByKeyPhysicalRel.class),
                planRow(2, AggregateAccumulateByKeyPhysicalRel.class),
                planRow(3, CalcPhysicalRel.class),
                planRow(4, SlidingWindowPhysicalRel.class),
                planRow(5, FullScanPhysicalRel.class)
        ));

        WatermarkKeysAssigner keysAssigner = new WatermarkKeysAssigner(optimizedPhysicalRel);
        keysAssigner.assignWatermarkKeys();

        // Watermark key was not propagated to root rel
        Map<Integer, MutableByte> map = keysAssigner.getWatermarkedFieldsKey(optimizedPhysicalRel);
        assertThat(map).isNull();

        // Watermark key propagation chain was ripped on AggregateAccumulateByKeyPhysicalRel
        PhysicalRel aggAccumRel = optimizedPhysicalRel;
        while (!(aggAccumRel instanceof AggregateAccumulateByKeyPhysicalRel)) {
            aggAccumRel = (PhysicalRel) aggAccumRel.getInput(0);
        }
        assertThat(aggAccumRel).isInstanceOf(AggregateAccumulateByKeyPhysicalRel.class);
        map = keysAssigner.getWatermarkedFieldsKey(aggAccumRel);
        assertThat(map).isNull();

        // Next rel after AggregateAccumulateByKeyPhysicalRel has watermark key
        PhysicalRel nextRelContainsWm = (PhysicalRel) ((AggregateAccumulateByKeyPhysicalRel) aggAccumRel).getInput();
        assertThat(nextRelContainsWm).isInstanceOf(CalcPhysicalRel.class);
        map = keysAssigner.getWatermarkedFieldsKey(nextRelContainsWm);
        assertThat(map).isNotNull();
        assertThat(map).isNotEmpty();
        assertThat(map.get(2)).isNotNull();
        assertThat(map.get(2).getValue()).isEqualTo((byte) 0);
    }
}
