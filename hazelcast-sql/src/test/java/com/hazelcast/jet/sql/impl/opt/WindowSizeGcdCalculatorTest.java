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

package com.hazelcast.jet.sql.impl.opt;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.sql.impl.opt.physical.CalcPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.DropLateItemsPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.ShouldNotExecuteRel;
import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowAggregatePhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.StreamToStreamJoinPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.UnionPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.WindowSizeGcdCalculator.DEFAULT_THROTTLING_FRAME_SIZE;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WindowSizeGcdCalculatorTest extends OptimizerTestSupport {
    static ExpressionEvalContext MOCK_EEC;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
        MOCK_EEC = new ExpressionEvalContext(emptyList(), new DefaultSerializationServiceBuilder().build());
    }

    @Test
    public void when_noSlidingWindowInTree_then_returnDefault() {
        HazelcastTable table = partitionedTable("map", asList(field(KEY, INT), field(VALUE, INT)), 1);
        List<QueryDataType> parameterTypes = asList(INT, INT);

        final String sql = "SELECT * FROM TABLE(IMPOSE_ORDER((SELECT __key, this FROM map), DESCRIPTOR(this), 1))";

        PhysicalRel optimizedPhysicalRel = optimizePhysical(sql, parameterTypes, table).getPhysical();

        assertPlan(optimizedPhysicalRel, plan(
                planRow(0, DropLateItemsPhysicalRel.class),
                planRow(1, FullScanPhysicalRel.class)
        ));

        WindowSizeGcdCalculator windowSizeGCDCalculator = new WindowSizeGcdCalculator(MOCK_EEC);
        windowSizeGCDCalculator.calculate(optimizedPhysicalRel);

        assertThat(optimizedPhysicalRel.getInput(0)).isInstanceOf(FullScanPhysicalRel.class);
        assertThat(windowSizeGCDCalculator.get()).isEqualTo(DEFAULT_THROTTLING_FRAME_SIZE);
    }

    @Test
    public void when_shouldNotExecutePlan_then_returnDefault() {
        HazelcastTable table = streamGeneratorTable("_stream", 10);
        List<QueryDataType> parameterTypes = Collections.singletonList(INT);

        final String sql = "SELECT MAX(v) FROM " +
                "TABLE(HOP(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER((SELECT * FROM TABLE(GENERATE_STREAM(10))), DESCRIPTOR(v), 1)))" +
                "  , DESCRIPTOR(v) , 6, 3))";

        PhysicalRel optimizedPhysicalRel = optimizePhysical(sql, parameterTypes, table).getPhysical();

        assertPlan(optimizedPhysicalRel, plan(planRow(0, ShouldNotExecuteRel.class)));

        WindowSizeGcdCalculator windowSizeGCDCalculator = new WindowSizeGcdCalculator(MOCK_EEC);
        windowSizeGCDCalculator.calculate(optimizedPhysicalRel);
        assertThat(windowSizeGCDCalculator.get()).isEqualTo(DEFAULT_THROTTLING_FRAME_SIZE);

        ShouldNotExecuteRel sneRel = (ShouldNotExecuteRel) optimizedPhysicalRel;
        assertThat(sneRel.message()).contains("Streaming aggregation is supported only for window aggregation");
    }

    @Test
    public void when_onlySlidingWindowInTree_then_returnWindowSize() {
        HazelcastTable streamTable = streamGeneratorTable("_stream", 10);
        List<QueryDataType> parameterTypes = Collections.singletonList(INT);

        final String sql = "SELECT MAX(v) FROM " +
                "TABLE(HOP(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER((SELECT * FROM TABLE(GENERATE_STREAM(10))), DESCRIPTOR(v), 1)))" +
                "  , DESCRIPTOR(v) , 6, 3)) " +
                "GROUP BY window_start, v";

        PhysicalRel optimizedPhysicalRel = optimizePhysical(sql, parameterTypes, streamTable).getPhysical();

        assertPlan(optimizedPhysicalRel, plan(
                planRow(0, CalcPhysicalRel.class),
                planRow(1, SlidingWindowAggregatePhysicalRel.class),
                planRow(2, CalcPhysicalRel.class),
                planRow(3, FullScanPhysicalRel.class)
        ));

        WindowSizeGcdCalculator windowSizeGCDCalculator = new WindowSizeGcdCalculator(MOCK_EEC);
        windowSizeGCDCalculator.calculate(optimizedPhysicalRel);
        assertThat(windowSizeGCDCalculator.get()).isEqualTo(6L);
    }

    @Test
    public void when_twoConsecutiveSlidingWindowsAgg_then_returnGcdOfWindowsSize() {
        HazelcastTable table = streamGeneratorTable("s1", 100);
        HazelcastTable table2 = partitionedTable("map", asList(field(KEY, INT), field(VALUE, INT)), 1);
        List<QueryDataType> parameterTypes = asList(INT, INT);

        final String sql = "SELECT window_end, window_end_inner, v, COUNT(v) FROM " +
                "TABLE(HOP(" +
                "   (SELECT v, window_end AS window_end_inner FROM " +
                "       TABLE(HOP(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER((SELECT * FROM TABLE(GENERATE_STREAM(10))), DESCRIPTOR(v), 1)))" +
                "       , DESCRIPTOR(v), 6, 2" +
                "       )) GROUP BY v, window_end_inner" +
                "   )" +
                "   , DESCRIPTOR(window_end_inner), 15, 5" +
                ")) " +
                "GROUP BY window_end, window_end_inner, v";

        PhysicalRel optimizedPhysicalRel = optimizePhysical(sql, parameterTypes, table, table2).getPhysical();

        assertPlan(optimizedPhysicalRel, plan(
                planRow(0, SlidingWindowAggregatePhysicalRel.class),
                planRow(1, CalcPhysicalRel.class),
                planRow(2, SlidingWindowAggregatePhysicalRel.class),
                planRow(3, CalcPhysicalRel.class),
                planRow(4, FullScanPhysicalRel.class)
        ));

        WindowSizeGcdCalculator windowSizeGCDCalculator = new WindowSizeGcdCalculator(MOCK_EEC);
        windowSizeGCDCalculator.calculate(optimizedPhysicalRel);
        // GCD(15, 6) = 3
        assertThat(windowSizeGCDCalculator.get()).isEqualTo(3L);

    }

    @Ignore("Bug with SlidingWindow#windowPolicyProvider")
    @Test
    public void when_unionAboveSlidingWindows_then_returnGcdOfWindowsSize() {
        HazelcastTable table = streamGeneratorTable("s1", 1);
        HazelcastTable table2 = streamGeneratorTable("s2", 10);
        HazelcastTable table3 = streamGeneratorTable("s3", 100);
        List<QueryDataType> parameterTypes = Collections.singletonList(INT);

        int expectedGcd = 12;

        final String query = "SELECT * FROM "
                + hop("s1", expectedGcd * 4, expectedGcd)
                + " UNION ALL "
                + hop("s2", expectedGcd * 3, expectedGcd)
                + " UNION ALL "
                + hop("s3", expectedGcd * 2, expectedGcd);

        PhysicalRel optimizedPhysicalRel = optimizePhysical(query, parameterTypes, table, table2, table3).getPhysical();

        // assert plan for better visibility of what was generated
        assertPlan(optimizedPhysicalRel, plan(
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

        WindowSizeGcdCalculator windowSizeGCDCalculator = new WindowSizeGcdCalculator(MOCK_EEC);
        windowSizeGCDCalculator.calculate(optimizedPhysicalRel);
        // GCD(48, 36, 24) = 12
        assertThat(windowSizeGCDCalculator.get()).isEqualTo(expectedGcd);
    }

    @Test
    public void when_streamToStreamJoin_then_returnMinPostponeTime() {
        HazelcastTable table = streamGeneratorTable("t1", 1);
        HazelcastTable table2 = streamGeneratorTable("t2", 10);
        HazelcastTable table3 = streamGeneratorTable("t3", 100);
        List<QueryDataType> parameterTypes = Collections.singletonList(INT);

        int expectedWindowSize = 25;

        final String query = "SELECT * FROM "
                + joinSubQuery("t1", "s1", 1)
                + " JOIN "
                + joinSubQuery("t2", "s2", 2)
                + " ON s1.v BETWEEN s2.v - 180 AND s2.v + 70 "    // 'window_size' = 250
                + " JOIN "
                + joinSubQuery("t3", "s3", 3)
                + " ON s3.v BETWEEN s1.v - 100 AND s1.v + 100 ";   // 'window_size' = 200

        PhysicalRel optPhysicalRel = optimizePhysical(query, parameterTypes, table, table2, table3).getPhysical();

        // assert plan for better visibility of what was generated
        assertPlan(optPhysicalRel, plan(
                planRow(0, StreamToStreamJoinPhysicalRel.class),
                planRow(1, StreamToStreamJoinPhysicalRel.class),
                planRow(2, FullScanPhysicalRel.class),
                planRow(2, FullScanPhysicalRel.class),
                planRow(1, FullScanPhysicalRel.class)
        ));

        WindowSizeGcdCalculator windowSizeGCDCalculator = new WindowSizeGcdCalculator(MOCK_EEC);
        windowSizeGCDCalculator.calculate(optPhysicalRel);
        // GCD(100, 40) = 4
        assertThat(windowSizeGCDCalculator.get()).isEqualTo(expectedWindowSize);
    }

    @SuppressWarnings("SameParameterValue")
    private static String hop(String name, int windowSize, int windowSlide) {
        return "(SELECT window_start, MAX(v) AS res FROM " +
                "TABLE(HOP(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(v), 1)))" +
                "  , DESCRIPTOR(v) , " + windowSize + " , " + windowSlide + ")) " +
                "GROUP BY window_start) ";
    }

    private static String joinSubQuery(String stream, String alias, int lag) {
        return "(SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + stream + ", DESCRIPTOR(v), " + lag + "))) AS " + alias;
    }
}
