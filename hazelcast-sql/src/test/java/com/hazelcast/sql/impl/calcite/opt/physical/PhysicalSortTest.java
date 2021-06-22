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

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.SortMergeExchangePhysicalRel;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PhysicalSortTest extends OptimizerTestSupport {

    @Test
    public void testTrivialSort() {
        assertPlan(
                optimizePhysical("SELECT f0, f1, f2, f3, f4 FROM p ORDER BY f3", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[3]], fetch=[null], offset=[null]", 100d),
                        planRow(2, SortPhysicalRel.class, "sort0=[$3], dir0=[ASC], requiresSort=[true]", 100d),
                        planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2, 3, 4]]]]", 100d)
                )
        );
    }

    @Test
    public void testSortWithProject() {
        assertPlan(
                optimizePhysical("SELECT f0 + f1, f2, f3, f4 FROM p ORDER BY f3", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[2]], fetch=[null], offset=[null]", 100d),
                        planRow(2, SortPhysicalRel.class, "sort0=[$2], dir0=[ASC], requiresSort=[true]", 100d),
                        planRow(3, ProjectPhysicalRel.class, "EXPR$0=[+(CAST($0):BIGINT(32), CAST($1):BIGINT(32))], f2=[$2], f3=[$3], f4=[$4]"),
                        planRow(4, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2, 3, 4]]]]", 100d)
                )
        );
    }


    @Test
    public void testAscDescSort() {
        assertPlan(
                optimizePhysical("SELECT f0, f1, f2, f3, f4 FROM p ORDER BY f3 ASC, f1 DESC", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[3, 1 DESC]], fetch=[null], offset=[null]", 100d),
                        planRow(2, SortPhysicalRel.class, "sort0=[$3], sort1=[$1], dir0=[ASC], dir1=[DESC], requiresSort=[true]", 100d),
                        planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2, 3, 4]]]]", 100d)
                )
        );
    }

    @Test
    public void testSortAndOffset() {
        assertPlan(
                optimizePhysical("SELECT f0, f1, f2, f3, f4 FROM p ORDER BY f3 OFFSET 10 ROWS", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[3]], fetch=[null], offset=[10:TINYINT(4)]", 100d),
                        planRow(2, SortPhysicalRel.class, "sort0=[$3], dir0=[ASC], requiresSort=[true]", 100d),
                        planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2, 3, 4]]]]", 100d)
                )
        );
    }


    @Test
    public void testSortAndFetch() {
        assertPlan(
                optimizePhysical("SELECT f0, f1, f2, f3, f4 FROM p ORDER BY f3 FETCH FIRST 10 ROWS ONLY", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[3]], fetch=[10:TINYINT(4)], offset=[null]", 100d),
                        planRow(2, SortPhysicalRel.class, "sort0=[$3], dir0=[ASC], requiresSort=[true]", 100d),
                        planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2, 3, 4]]]]", 100d)
                )
        );
    }

    @Test
    public void testSortAndFetchAndOffset() {
        assertPlan(
                optimizePhysical("SELECT f0, f1, f2, f3, f4 FROM p ORDER BY f3 OFFSET 10 ROWS FETCH FIRST 10 ROWS ONLY", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[3]], fetch=[10:TINYINT(4)], offset=[10:TINYINT(4)]", 100d),
                        planRow(2, SortPhysicalRel.class, "sort0=[$3], dir0=[ASC], requiresSort=[true]", 100d),
                        planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2, 3, 4]]]]", 100d)
                )
        );
    }

    @Test
    public void testFetchAndOffsetOnly() {
        assertPlan(
                optimizePhysical("SELECT f0, f1, f2, f3, f4 FROM p OFFSET 10 ROWS FETCH FIRST 10 ROWS ONLY", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[]], fetch=[10:TINYINT(4)], offset=[10:TINYINT(4)]", 100d),
                        planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2, 3, 4]]]]", 100d)
                )
        );
    }

    @Test
    public void testFetchAndOffsetOnlyOneMember() {
        assertPlan(
                optimizePhysical("SELECT f0, f1, f2, f3, f4 FROM p OFFSET 10 ROWS FETCH FIRST 10 ROWS ONLY", 1),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 10d),
                        planRow(1, SortPhysicalRel.class, "offset=[10:TINYINT(4)], fetch=[10:TINYINT(4)], requiresSort=[false]", 10d),
                        planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2, 3, 4]]]]", 100d)
                )
        );
    }
}
