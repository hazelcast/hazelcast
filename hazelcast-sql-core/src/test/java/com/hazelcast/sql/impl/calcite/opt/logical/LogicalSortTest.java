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

package com.hazelcast.sql.impl.calcite.opt.logical;

import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test for sort optimizations.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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


}
