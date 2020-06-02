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
 * Test for project/filter optimizations.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LogicalProjectFilterTest extends OptimizerTestSupport {
    @Test
    public void test_project() {
        assertPlan(
            optimizeLogical("SELECT f1, f2 FROM p"),
            plan(
                planRow(0, RootLogicalRel.class, "", 100d),
                planRow(1, ProjectLogicalRel.class, "f1=[$0], f2=[$1]", 100d),
                planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, p]], projects=[[0, 1, 2, 3, 4]]", 100d)
            )
        );
    }

    @Test
    public void test_projectFilter() {
        assertPlan(
            optimizeLogical("SELECT f1, f2 FROM p WHERE f3 IS NULL"),
            plan(
                planRow(0, RootLogicalRel.class, "", 25d),
                planRow(1, ProjectLogicalRel.class, "f1=[$0], f2=[$1]", 25d),
                planRow(2, FilterLogicalRel.class, "condition=[IS NULL($2)]", 25d),
                planRow(3, ProjectLogicalRel.class, "f1=[$0], f2=[$1], f3=[$2]", 100d),
                planRow(4, MapScanLogicalRel.class, "table=[[hazelcast, p]], projects=[[0, 1, 2, 3, 4]]", 100d)
            )
        );
    }

    @Test
    public void test_project_project() {
        assertPlan(
            optimizeLogical("SELECT f1 FROM (SELECT f1, f2 FROM p)"),
            plan(
                planRow(0, RootLogicalRel.class, "", 100d),
                planRow(1, ProjectLogicalRel.class, "f1=[$0]", 100d),
                planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, p]], projects=[[0, 1, 2, 3, 4]]", 100d)
            )
        );
    }

    @Test
    public void test_project_projectFilter() {
        assertPlan(
            optimizeLogical("SELECT f1 FROM (SELECT f1, f2 FROM p WHERE f3 IS NULL)"),
            plan(
                planRow(0, RootLogicalRel.class, "", 25d),
                planRow(1, ProjectLogicalRel.class, "f1=[$0]", 25d),
                planRow(2, FilterLogicalRel.class, "condition=[IS NULL($1)]", 25d),
                planRow(3, ProjectLogicalRel.class, "f1=[$0], f3=[$2]", 100d),
                planRow(4, MapScanLogicalRel.class, "table=[[hazelcast, p]], projects=[[0, 1, 2, 3, 4]]", 100d)
            )
        );
    }

    @Test
    public void test_projectFilter_project() {
        assertPlan(
            optimizeLogical("SELECT f1 FROM (SELECT f1, f2 FROM p) WHERE f2 IS NULL"),
            plan(
                planRow(0, RootLogicalRel.class, "", 25d),
                planRow(1, ProjectLogicalRel.class, "f1=[$0]", 25d),
                planRow(2, FilterLogicalRel.class, "condition=[IS NULL($1)]", 25d),
                planRow(3, ProjectLogicalRel.class, "f1=[$0], f2=[$1]", 100d),
                planRow(4, MapScanLogicalRel.class, "table=[[hazelcast, p]], projects=[[0, 1, 2, 3, 4]]", 100d)
            )
        );
    }

    @Test
    public void test_projectFilter_projectFilter() {
        assertPlan(
            optimizeLogical("SELECT f1 FROM (SELECT f1, f2 FROM p WHERE f3 IS NULL) WHERE f2 IS NULL"),
            plan(
                planRow(0, RootLogicalRel.class, "", 6.2d),
                planRow(1, ProjectLogicalRel.class, "f1=[$0]", 6.2d),
                planRow(2, FilterLogicalRel.class, "condition=[IS NULL($1)]", 6.2d),
                planRow(3, ProjectLogicalRel.class, "f1=[$0], f2=[$1]", 25d),
                planRow(4, FilterLogicalRel.class, "condition=[IS NULL($2)]", 25d),
                planRow(5, ProjectLogicalRel.class, "f1=[$0], f2=[$1], f3=[$2]", 100d),
                planRow(6, MapScanLogicalRel.class, "table=[[hazelcast, p]], projects=[[0, 1, 2, 3, 4]]", 100d)
            )
        );
    }
}
