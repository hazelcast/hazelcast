/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.opt.physical.index;

import com.hazelcast.config.IndexType;
import com.hazelcast.sql.impl.calcite.opt.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ProjectPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.SortPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.SortMergeExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.schema.Table;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PhysicalSortIndexTest extends IndexOptimizerTestSupport {

    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        HazelcastTable pTable = partitionedTable(
                "p",
                fields("ret", INT, "f1", INT, "f2", INT, "f3", INT),
                Arrays.asList(
                        new MapTableIndex("sorted_f1", IndexType.SORTED, 1, singletonList(1), singletonList(INT)),
                        new MapTableIndex("sorted_f1_f3", IndexType.SORTED, 2, asList(1, 3), asList(INT, INT))

                ),
                100,
                false
        );

        tableMap.put("p", pTable);

        return new HazelcastSchema(tableMap);
    }

    @Test
    public void testTrivialSort() {
        assertPlan(
                optimizePhysical("SELECT f1, f2, f3 FROM p ORDER BY f1", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0]], fetch=[null], offset=[null]", 100d),
                        planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testTrivialSortDescending() {
        assertPlan(
                optimizePhysical("SELECT f1, f2, f3 FROM p ORDER BY f1 DESC", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0 DESC]], fetch=[null], offset=[null]", 100d),
                        planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testCompositeSort() {
        assertPlan(
                optimizePhysical("SELECT f1, f3 FROM p ORDER BY f1, f3", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0, 1]], fetch=[null], offset=[null]", 100d),
                        planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testCompositeSortWithProject2() {
        assertPlan(
                optimizePhysical("SELECT f1 + f3 FROM p ORDER BY f1, f3", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, ProjectPhysicalRel.class, "EXPR$0=[$0]"),
                        planRow(2, SortMergeExchangePhysicalRel.class, "collation=[[1, 2]], fetch=[null], offset=[null]", 100d),
                        planRow(3, ProjectPhysicalRel.class, "EXPR$0=[+($0, $1)], f1=[$0], f3=[$1]"),
                        planRow(4, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testCompositeSortWithProject3() {
        assertPlan(
                optimizePhysical("SELECT f1, f1 + f3, f3 FROM p ORDER BY f1, f3", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0, 2]], fetch=[null], offset=[null]", 100d),
                        planRow(2, ProjectPhysicalRel.class, "f1=[$0], EXPR$1=[+($0, $1)], f3=[$1]"),
                        planRow(3, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testCompositeSortWithProject4() {
        assertPlan(
                optimizePhysical("SELECT a, b FROM (SELECT f1+f3 a, f1-f3 b FROM p)"
                       + "ORDER BY a, b", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0, 1]], fetch=[null], offset=[null]"),
                        planRow(2, SortPhysicalRel.class, "sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC]", 100d),
                        planRow(3, ProjectPhysicalRel.class, "a=[+($0, $1)], b=[-($0, $1)]"),
                        planRow(4, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]]", 100d)
                )
        );
    }


}
