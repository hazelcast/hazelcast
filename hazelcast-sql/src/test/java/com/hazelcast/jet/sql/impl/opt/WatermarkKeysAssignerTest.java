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

import com.hazelcast.internal.util.MutableByte;
import com.hazelcast.jet.sql.impl.opt.physical.AggregateAccumulateByKeyPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.AggregateCombineByKeyPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.CalcPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.DropLateItemsPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.UnionPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WatermarkKeysAssignerTest extends OptimizerTestSupport {
    // Note: unit test for Join will be added in S2S PR, since join is revamped there.

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void when_scanAndDropArePresent_then_keyWasAssigned() {
        HazelcastTable table = partitionedTable("map", asList(field(KEY, INT), field(VALUE, INT)), 1);
        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, QueryDataType.INT);

        final String sql = "(SELECT * FROM TABLE(IMPOSE_ORDER((SELECT __key, this FROM map), DESCRIPTOR(this), 1)))";

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

    @Ignore("Rework")
    @Test
    public void when_calcIsPresent_then_keyWasPropagated() {
        HazelcastTable table = partitionedTable("map", asList(field(KEY, INT), field(VALUE, INT)), 1);
        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, QueryDataType.INT);

        String sql = "(SELECT this, __key FROM TABLE(IMPOSE_ORDER((SELECT __key, this FROM map), DESCRIPTOR(this), 1)))";

        PhysicalRel optimizedPhysicalRel = optimizePhysical(sql, parameterTypes, table).getPhysical();

        assertPlan(optimizedPhysicalRel, plan(
                planRow(0, DropLateItemsPhysicalRel.class),
                planRow(1, FullScanPhysicalRel.class)
        ));

        WatermarkKeysAssigner keysAssigner = new WatermarkKeysAssigner(optimizedPhysicalRel);
        keysAssigner.assignWatermarkKeys();

        assertThat(optimizedPhysicalRel).isInstanceOf(CalcPhysicalRel.class);

        // Watermark key was propagated to CalcPhysicalRel
        Map<Integer, MutableByte> map = keysAssigner.getWatermarkedFieldsKey(optimizedPhysicalRel);
        assertThat(map).isNotNull();
        assertThat(map).isNotEmpty();
        assertThat(map.get(0)).isNotNull();
        assertThat(map.get(0).getValue()).isEqualTo((byte) 0);
    }

    @Test
    public void when_unionIsPresent_then_keyWasPropagated() {
        HazelcastTable table = partitionedTable("map", asList(field(KEY, INT), field(VALUE, INT)), 1);
        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, QueryDataType.INT);

        String sql = "(SELECT * FROM TABLE(IMPOSE_ORDER((SELECT __key, this FROM map), DESCRIPTOR(this), 1)))" +
                " UNION ALL" +
                " (SELECT * FROM TABLE(IMPOSE_ORDER((SELECT __key, this FROM map), DESCRIPTOR(this), 1)))";

        PhysicalRel optimizedPhysicalRel = optimizePhysical(sql, parameterTypes, table).getPhysical();

        assertPlan(optimizedPhysicalRel, plan(
                planRow(0, UnionPhysicalRel.class),
                planRow(1, DropLateItemsPhysicalRel.class),
                planRow(2, FullScanPhysicalRel.class),
                planRow(1, DropLateItemsPhysicalRel.class),
                planRow(2, FullScanPhysicalRel.class)
        ));

        WatermarkKeysAssigner keysAssigner = new WatermarkKeysAssigner(optimizedPhysicalRel);
        keysAssigner.assignWatermarkKeys();

        assertThat(optimizedPhysicalRel).isInstanceOf(UnionPhysicalRel.class);

        // Watermark key was propagated to UnionPhysicalRel
        Map<Integer, MutableByte> map = keysAssigner.getWatermarkedFieldsKey(optimizedPhysicalRel);
        assertThat(map).isNotNull();
        assertThat(map).isNotEmpty();
        assertThat(map.get(1)).isNotNull(); // 2nd field (this) is watermarked, that's why we have index 1.
        assertThat(map.get(1).getValue()).isEqualTo((byte) 0);
    }

    @Test
    public void when_slidingWindowIsPresent_then_windowBoundsWereAdded() {
        HazelcastTable table = partitionedTable("map", asList(field(KEY, INT), field(VALUE, INT)), 1);
        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, QueryDataType.INT);

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
    public void when_upperRelsDoesntSupportWatermarks_then_justPartOfTreeIsWatermarked() {
        HazelcastTable table = partitionedTable("map", asList(field(KEY, INT), field(VALUE, INT)), 1);
        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, QueryDataType.INT);

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

        // Watermark key propagation chain was ripped on AggregateAccumulateByKeyPhysicalRel.
        PhysicalRel aggAccumRel = optimizedPhysicalRel;
        while (!(aggAccumRel instanceof AggregateAccumulateByKeyPhysicalRel)) {
            aggAccumRel = (PhysicalRel) aggAccumRel.getInput(0);
        }
        assertThat(aggAccumRel).isInstanceOf(AggregateAccumulateByKeyPhysicalRel.class);
        map = keysAssigner.getWatermarkedFieldsKey(aggAccumRel);
        assertThat(map).isNull();

        // Next rel after AggregateAccumulateByKeyPhysicalRel has watermark key.
        PhysicalRel nextRelContainsWm = (PhysicalRel) ((AggregateAccumulateByKeyPhysicalRel) aggAccumRel).getInput();
        assertThat(nextRelContainsWm).isInstanceOf(CalcPhysicalRel.class);
        map = keysAssigner.getWatermarkedFieldsKey(nextRelContainsWm);
        assertThat(map).isNotNull();
        assertThat(map).isNotEmpty();
        assertThat(map.get(2)).isNotNull();
        assertThat(map.get(2).getValue()).isEqualTo((byte) 0);
    }
}
