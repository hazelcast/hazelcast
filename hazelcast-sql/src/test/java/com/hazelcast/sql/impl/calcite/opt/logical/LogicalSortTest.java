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

package com.hazelcast.sql.impl.calcite.opt.logical;

import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test for sort optimizations.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(ParallelJVMTest.class)
public class LogicalSortTest extends OptimizerTestSupport {

    @Test
    public void testSortAscAndScan() {
        assertPlan(
                optimizeLogical("SELECT f0, f1 FROM p ORDER BY f0"),
                plan(
                        planRow(0, RootLogicalRel.class, "", 100d),
                        planRow(1, SortLogicalRel.class, "sort0=[$0], dir0=[ASC]", 100d),
                        planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[0, 1]]]]", 100d)
                )
        );
    }

    @Test
    public void testSortAscAndScan2() {
        assertPlan(
                optimizeLogical("SELECT f0, f1 FROM p ORDER BY f1"),
                plan(
                        planRow(0, RootLogicalRel.class, "", 100d),
                        planRow(1, SortLogicalRel.class, "sort0=[$1], dir0=[ASC]", 100d),
                        planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[0, 1]]]]", 100d)
                )
        );
    }

    @Test
    public void testSortDescAndScan() {
        assertPlan(
                optimizeLogical("SELECT f0, f1 FROM p ORDER BY f0 DESC"),
                plan(
                        planRow(0, RootLogicalRel.class, "", 100d),
                        planRow(1, SortLogicalRel.class, "sort0=[$0], dir0=[DESC]", 100d),
                        planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[0, 1]]]]", 100d)
                )
        );
    }

    @Test
    public void testSortDescAndScan2() {
        assertPlan(
                optimizeLogical("SELECT f0, f1 FROM p ORDER BY f1 DESC"),
                plan(
                        planRow(0, RootLogicalRel.class, "", 100d),
                        planRow(1, SortLogicalRel.class, "sort0=[$1], dir0=[DESC]", 100d),
                        planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[0, 1]]]]", 100d)
                )
        );
    }

    @Test
    public void testSortAscDescAndScan() {
        assertPlan(
                optimizeLogical("SELECT f0, f1 FROM p ORDER BY f0 ASC, f1 DESC"),
                plan(
                        planRow(0, RootLogicalRel.class, "", 100d),
                        planRow(1, SortLogicalRel.class, "sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC]", 100d),
                        planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[0, 1]]]]", 100d)
                )
        );
    }

    @Test
    public void testSimpleOrderByAndOffset() {
        assertPlan(
                optimizeLogical("SELECT f0, f1 FROM p ORDER BY f0 OFFSET 10 ROWS"),
                plan(
                        planRow(0, RootLogicalRel.class, "", 90d),
                        planRow(1, SortLogicalRel.class, "sort0=[$0], dir0=[ASC], offset=[10]", 90d),
                        planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[0, 1]]]]", 100d)
                )
        );
    }

    @Test
    public void testSimpleOrderByAndFetch() {
        assertPlan(
                optimizeLogical("SELECT f0, f1 FROM p ORDER BY f0 FETCH FIRST 10 ROWS ONLY"),
                plan(
                        planRow(0, RootLogicalRel.class, "", 10d),
                        planRow(1, SortLogicalRel.class, "sort0=[$0], dir0=[ASC], fetch=[10]", 10d),
                        planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[0, 1]]]]", 100d)
                )
        );
    }

    @Test
    public void testSimpleOrderByAndFetchAndOffset() {
        assertPlan(
                optimizeLogical("SELECT f0, f1 FROM p ORDER BY f0 OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY"),
                plan(
                        planRow(0, RootLogicalRel.class, "", 10d),
                        planRow(1, SortLogicalRel.class, "sort0=[$0], dir0=[ASC], offset=[10], fetch=[10]", 10d),
                        planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[0, 1]]]]", 100d)
                )
        );
    }

    @Test
    public void testFetchOnly() {
        assertPlan(
                optimizeLogical("SELECT f0, f1 FROM p FETCH FIRST 10 ROWS ONLY"),
                plan(
                        planRow(0, RootLogicalRel.class, "", 10d),
                        planRow(1, SortLogicalRel.class, "fetch=[10]", 10d),
                        planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[0, 1]]]]", 100d)
                )
        );
    }

    @Test
    public void testOffsetOnly() {
        assertPlan(
                optimizeLogical("SELECT f0, f1 FROM p OFFSET 10 ROWS"),
                plan(
                        planRow(0, RootLogicalRel.class, "", 90d),
                        planRow(1, SortLogicalRel.class, "offset=[10]", 90d),
                        planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[0, 1]]]]", 100d)
                )
        );
    }

    @Test
    public void testFetchAndOffsetOnly() {
        assertPlan(
                optimizeLogical("SELECT f0, f1 FROM p OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY"),
                plan(
                        planRow(0, RootLogicalRel.class, "", 10d),
                        planRow(1, SortLogicalRel.class, "offset=[5], fetch=[10]", 10d),
                        planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[0, 1]]]]", 100d)
                )
        );
    }


}
