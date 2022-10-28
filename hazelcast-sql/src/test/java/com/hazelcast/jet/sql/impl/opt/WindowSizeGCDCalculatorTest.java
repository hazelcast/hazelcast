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
import com.hazelcast.jet.sql.impl.opt.physical.AggregateAccumulateByKeyPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.AggregateCombineByKeyPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.CalcPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.DropLateItemsPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.JoinHashPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.JoinNestedLoopPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.RelNode;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.WindowSizeGCDCalculator.DEFAULT_THROTTLING_FRAME_SIZE;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

public class WindowSizeGCDCalculatorTest extends OptimizerTestSupport {
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

        WindowSizeGCDCalculator windowSizeGCDCalculator = new WindowSizeGCDCalculator(MOCK_EEC);
        windowSizeGCDCalculator.calculate(optimizedPhysicalRel);

        assertThat(optimizedPhysicalRel.getInput(0)).isInstanceOf(FullScanPhysicalRel.class);
        FullScanPhysicalRel scan = (FullScanPhysicalRel) optimizedPhysicalRel.getInput(0);

        assertThat(windowSizeGCDCalculator.get()).isEqualTo(DEFAULT_THROTTLING_FRAME_SIZE);
    }

    @Test
    public void when_onlySlidingWindowInTree_then_returnWindowSize() {
        HazelcastTable table = partitionedTable("map", asList(field(KEY, INT), field(VALUE, INT)), 1);
        List<QueryDataType> parameterTypes = asList(INT, INT);

        final String sql = "SELECT window_start, SUM(__key) FROM " +
                "TABLE(HOP(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER((SELECT __key, this FROM map), DESCRIPTOR(this), 1)))" +
                "  , DESCRIPTOR(this) , 6, 3)) " +
                "GROUP BY window_start";

        PhysicalRel optimizedPhysicalRel = optimizePhysical(sql, parameterTypes, table).getPhysical();

        assertPlan(optimizedPhysicalRel, plan(
                planRow(0, AggregateCombineByKeyPhysicalRel.class),
                planRow(1, AggregateAccumulateByKeyPhysicalRel.class),
                planRow(2, CalcPhysicalRel.class),
                planRow(3, SlidingWindowPhysicalRel.class),
                planRow(4, FullScanPhysicalRel.class)
        ));

        WindowSizeGCDCalculator windowSizeGCDCalculator = new WindowSizeGCDCalculator(MOCK_EEC);
        windowSizeGCDCalculator.calculate(optimizedPhysicalRel);

        RelNode rel = optimizedPhysicalRel;
        while (!(rel instanceof FullScanPhysicalRel)) {
            rel = rel.getInput(0);
        }

        FullScanPhysicalRel scan = (FullScanPhysicalRel) rel;

        assertThat(windowSizeGCDCalculator.get()).isEqualTo(6L);
    }

    @Test
    public void when_joinBelowSlidingWindows_then_returnEqualGCDOfWindowsSize() {
        HazelcastTable table = partitionedTable("map", asList(field(KEY, INT), field(VALUE, INT)), 1);
        HazelcastTable table2 = partitionedTable("map2", asList(field(KEY, INT), field(VALUE, INT)), 1);
        List<QueryDataType> parameterTypes = asList(INT, INT);

        final String sql = "SELECT window_start, window_start_inner, this, COUNT(*) FROM " +
                "TABLE(HOP(" +
                "   (SELECT ts, window_start window_start_inner, this FROM " +
                "       TABLE(HOP(" +
                "           (SELECT this AS ts FROM TABLE(IMPOSE_ORDER((SELECT this FROM map), DESCRIPTOR(this), 3)))" +
                "           , DESCRIPTOR(ts), 6, 3" +
                "       )) JOIN map2 ON ts = this" +
                "   )" +
                "   , DESCRIPTOR(this), 4, 2 )) " +
                "GROUP BY window_start, window_start_inner, this";

        PhysicalRel optimizedPhysicalRel = optimizePhysical(sql, parameterTypes, table, table2).getPhysical();

        assertPlan(optimizedPhysicalRel, plan(
                planRow(0, AggregateCombineByKeyPhysicalRel.class),
                planRow(1, AggregateAccumulateByKeyPhysicalRel.class),
                planRow(2, CalcPhysicalRel.class),
                planRow(3, SlidingWindowPhysicalRel.class),
                planRow(4, CalcPhysicalRel.class),
                planRow(5, JoinNestedLoopPhysicalRel.class),
                planRow(6, SlidingWindowPhysicalRel.class),
                planRow(7, FullScanPhysicalRel.class),
                planRow(6, FullScanPhysicalRel.class)
        ));

        WindowSizeGCDCalculator windowSizeGCDCalculator = new WindowSizeGCDCalculator(MOCK_EEC);
        windowSizeGCDCalculator.calculate(optimizedPhysicalRel);
        // GCD(4, 6) = 2
        assertThat(windowSizeGCDCalculator.get()).isEqualTo(2L);

    }

    @Test
    public void when_joinAboveSlidingWindows_then_returnEqualGCDOfWindowsSize() {
        HazelcastTable table = partitionedTable("map", asList(field(KEY, INT), field(VALUE, INT)), 1);
        HazelcastTable table2 = partitionedTable("map2", asList(field(KEY, INT), field(VALUE, INT)), 1);
        List<QueryDataType> parameterTypes = asList(INT, INT);

        final String query = "SELECT * FROM " + hop("map", 9, 3)
                + " JOIN " + hop("map2", 6, 3) + " USING (res)";

        PhysicalRel optimizedPhysicalRel = optimizePhysical(query, parameterTypes, table, table2).getPhysical();

        // assert plan for better visibility of what was generated
        assertPlan(optimizedPhysicalRel, plan(
                planRow(0, CalcPhysicalRel.class),
                planRow(1, JoinHashPhysicalRel.class),
                // left side of join
                planRow(2, AggregateCombineByKeyPhysicalRel.class),
                planRow(3, AggregateAccumulateByKeyPhysicalRel.class),
                planRow(4, CalcPhysicalRel.class),
                planRow(5, SlidingWindowPhysicalRel.class),
                planRow(6, FullScanPhysicalRel.class),
                // right side of join
                planRow(2, AggregateCombineByKeyPhysicalRel.class),
                planRow(3, AggregateAccumulateByKeyPhysicalRel.class),
                planRow(4, CalcPhysicalRel.class),
                planRow(5, SlidingWindowPhysicalRel.class),
                planRow(6, FullScanPhysicalRel.class)
        ));

        WindowSizeGCDCalculator windowSizeGCDCalculator = new WindowSizeGCDCalculator(MOCK_EEC);
        windowSizeGCDCalculator.calculate(optimizedPhysicalRel);
        assertThat(windowSizeGCDCalculator.get()).isEqualTo(3L);
    }

    @SuppressWarnings("SameParameterValue")
    static String hop(String mapName, int windowSize, int windowSlide) {
        return "(SELECT window_start, SUM(__key) AS res FROM " +
                "TABLE(HOP(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER((SELECT __key, this FROM " + mapName + "), DESCRIPTOR(this), 1)))" +
                "  , DESCRIPTOR(this) , " + windowSize + " , " + windowSlide + ")) " +
                "GROUP BY window_start)";
    }
}
