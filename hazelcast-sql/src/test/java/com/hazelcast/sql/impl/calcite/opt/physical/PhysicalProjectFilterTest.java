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

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.RootExchangePhysicalRel;
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
public class PhysicalProjectFilterTest extends OptimizerTestSupport {
    @Test
    public void testTrivialProject() {
        assertPlan(
            optimizePhysical("SELECT f0, f1, f2, f3, f4 FROM p", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 100d),
                planRow(1, RootExchangePhysicalRel.class, "", 100d),
                planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2, 3, 4]]]]", 100d)
            )
        );
    }

    @Test
    public void testTrivialProjectProject() {
        assertPlan(
            optimizePhysical("SELECT f0, f1, f2, f3, f4 FROM (SELECT f0, f1, f2, f3, f4 FROM p)", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 100d),
                planRow(1, RootExchangePhysicalRel.class, "", 100d),
                planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2, 3, 4]]]]", 100d)
            )
        );
    }

    @Test
    public void testTrivialStarProject() {
        assertPlan(
            optimizePhysical("SELECT * FROM p", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 100d),
                planRow(1, RootExchangePhysicalRel.class, "", 100d),
                planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2, 3, 4]]]]", 100d)
            )
        );
    }

    @Test
    public void testTrivialStarProjectProject() {
        assertPlan(
            optimizePhysical("SELECT * FROM (SELECT * FROM p)", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 100d),
                planRow(1, RootExchangePhysicalRel.class, "", 100d),
                planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2, 3, 4]]]]", 100d)
            )
        );
    }

    /**
     * Before: Project <- Scan
     * After : Scan(Project)
     */
    @Test
    public void testProjectIntoScan() {
        assertPlan(
            optimizePhysical("SELECT f0, f1 FROM p", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 100d),
                planRow(1, RootExchangePhysicalRel.class, "", 100d),
                planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1]]]]", 100d)
            )
        );
    }

    @Test
    public void testProjectWithoutInputReferences() {
        assertPlan(
            optimizePhysical("SELECT TRUE FROM p", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 100d),
                planRow(1, RootExchangePhysicalRel.class, "", 100d),
                planRow(2, ProjectPhysicalRel.class, "EXPR$0=[true]", 100d),
                planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[]]]]", 100d)
            )
        );
    }

    /**
     * Before: Project(exp) <- Scan
     * After : Project(exp) <- Scan(Project)
     */
    @Test
    public void testProjectExpressionIntoScan() {
        assertPlan(
            optimizePhysical("SELECT f0 + f1, f2 FROM p", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 100d),
                planRow(1, RootExchangePhysicalRel.class, "", 100d),
                planRow(2, ProjectPhysicalRel.class, "EXPR$0=[+($0, $1)], f2=[$2]", 100d),
                planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2]]]]", 100d)
            )
        );
    }

    /**
     * Before: Project <- Filter <- Scan
     * After : Scan(Project, Filter)
     */
    @Test
    public void testProjectFilterIntoScan() {
        assertPlan(
            optimizePhysical("SELECT f0, f1 FROM p WHERE f2 > 1", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 50d),
                planRow(1, RootExchangePhysicalRel.class, "", 50d),
                planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1], filter=>($2, 1)]]]", 50d)
            )
        );
    }

    /**
     * Before: Project(exp) <- Filter <- Scan
     * After : Project(exp) <- Scan(Project, Filter)
     */
    @Test
    public void testProjectExpressionFilterScan() {
        assertPlan(
            optimizePhysical("SELECT f0 + f1, f2 FROM p WHERE f3 > 1", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 50d),
                planRow(1, RootExchangePhysicalRel.class, "", 50d),
                planRow(2, ProjectPhysicalRel.class, "EXPR$0=[+($0, $1)], f2=[$2]", 50d),
                planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2], filter=>($3, 1)]]]", 50d)
            )
        );
    }

    /**
     * Before: Project2 <- Project1 <- Scan
     * After : Scan(Project2)
     */
    @Test
    public void testProjectProjectIntoScan() {
        assertPlan(
            optimizePhysical("SELECT f0 FROM (SELECT f0, f1 FROM p)", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 100d),
                planRow(1, RootExchangePhysicalRel.class, "", 100d),
                planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0]]]]", 100d)
            )
        );
    }

    /**
     * Before: Project2 <- Project1(expression) <- Scan
     * After : Scan(Project2)
     */
    @Test
    public void testProjectProjectExpressionIntoScan() {
        assertPlan(
            optimizePhysical("SELECT d1, f2 FROM (SELECT f0 + f1 d1, f2, f3 FROM p)", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 100d),
                planRow(1, RootExchangePhysicalRel.class, "", 100d),
                planRow(2, ProjectPhysicalRel.class, "d1=[+($0, $1)], f2=[$2]", 100d),
                planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2]]]]", 100d)
            )
        );
    }

    /**
     * Before: Project2(expression) <- Project1 <- Scan
     * After : Project2 <- Scan(Project2)
     */
    @Test
    public void testProjectExpressionProjectIntoScan() {
        assertPlan(
            optimizePhysical("SELECT f0 + f1, f2 FROM (SELECT f0, f1, f2, f3 FROM p)", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 100d),
                planRow(1, RootExchangePhysicalRel.class, "", 100d),
                planRow(2, ProjectPhysicalRel.class, "EXPR$0=[+($0, $1)], f2=[$2]", 100d),
                planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2]]]]", 100d)
            )
        );
    }

    /**
     * Before: Project2(expression) <- Project1(expression) <- Scan
     * After : Scan(Project2)
     */
    @Test
    public void testProjectExpressionProjectExpressionIntoScan() {
        assertPlan(
            optimizePhysical("SELECT d1 + f2 FROM (SELECT f0 + f1 d1, f2, f3 FROM p)", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 100d),
                planRow(1, RootExchangePhysicalRel.class, "", 100d),
                planRow(2, ProjectPhysicalRel.class, "EXPR$0=[+(+($0, $1), $2)]", 100d),
                planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2]]]]", 100d)
            )
        );
    }

    /**
     * Before: Project2 <- Project1 <- Filter <- Scan
     * After : Scan(Project2, Filter)
     */
    @Test
    public void testProjectProjectFilterIntoScan() {
        assertPlan(
            optimizePhysical("SELECT f0 FROM (SELECT f0, f1 FROM p WHERE f2 > 1)", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 50d),
                planRow(1, RootExchangePhysicalRel.class, "", 50d),
                planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0], filter=>($2, 1)]]]", 50d)
            )
        );
    }

    /**
     * Before: Project2 <- Project1(expression) <- Filter <- Scan
     * After : Project <- Scan(Project, Filter)
     */
    @Test
    public void testProjectProjectExpressionFilterIntoScan() {
        assertPlan(
            optimizePhysical("SELECT d1, f2 FROM (SELECT f0 + f1 d1, f2, f3 FROM p WHERE f4 > 1)", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 50d),
                planRow(1, RootExchangePhysicalRel.class, "", 50d),
                planRow(2, ProjectPhysicalRel.class, "d1=[+($0, $1)], f2=[$2]", 50d),
                planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2], filter=>($4, 1)]]]", 50d)
            )
        );
    }

    /**
     * Before: Project2(expression) <- Project1 <- Filter <- Scan
     * After : Project <- Scan(Project, Filter)
     */
    @Test
    public void testProjectExpressionProjectFilterIntoScan() {
        assertPlan(
            optimizePhysical("SELECT f0 + f1 FROM (SELECT f0, f1, f2, f3 FROM p WHERE f2 > 1)", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 50d),
                planRow(1, RootExchangePhysicalRel.class, "", 50d),
                planRow(2, ProjectPhysicalRel.class, "EXPR$0=[+($0, $1)]", 50d),
                planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1], filter=>($2, 1)]]]", 50d)
            )
        );
    }

    /**
     * Before: Project2(expression) <- Project1(expression) <- Filter <- Scan
     * After : Project <- Scan(Project, Filter)
     */
    @Test
    public void testProjectExpressionProjectExpressionFilterIntoScan() {
        assertPlan(
            optimizePhysical("SELECT d1 + f2 FROM (SELECT f0 + f1 d1, f2, f3 FROM p WHERE f4 > 1)", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 50d),
                planRow(1, RootExchangePhysicalRel.class, "", 50d),
                planRow(2, ProjectPhysicalRel.class, "EXPR$0=[+(+($0, $1), $2)]", 50d),
                planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2], filter=>($4, 1)]]]", 50d)
            )
        );
    }

    /**
     * Before: Project2 <- Filter <- Project1 <- Scan
     * After : Scan(Project2, Filter)
     */
    @Test
    public void testProjectFilterProjectIntoScan() {
        assertPlan(
            optimizePhysical("SELECT f0 FROM (SELECT f0, f1, f2 FROM p) WHERE f1 > 1", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 50d),
                planRow(1, RootExchangePhysicalRel.class, "", 50d),
                planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0], filter=>($1, 1)]]]", 50d)
            )
        );
    }

    /**
     * Before: Project2 <- Filter <- Project1(expression) <- Scan
     * After : Project <- Project <- Scan(Project2, Filter)
     *
     */
    @Test
    public void testProjectFilterProjectExpressionIntoScan() {
        assertPlan(
            optimizePhysical("SELECT d1, f2 FROM (SELECT f0 + f1 d1, f2, f3, f4 FROM p) WHERE f3 > 1", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 50d),
                planRow(1, RootExchangePhysicalRel.class, "", 50d),
                planRow(2, ProjectPhysicalRel.class, "d1=[+($0, $1)], f2=[$2]", 50d),
                planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2], filter=>($3, 1)]]]", 50d)
            )
        );
    }

    /**
     * Before: Project2(expression) <- Filter <- Project1 <- Scan
     * After : Project <-  Scan(Project, Filter)
     *
     */
    @Test
    public void testProjectExpressionFilterProjectIntoScan() {
        assertPlan(
            optimizePhysical("SELECT f0 + f1 FROM (SELECT f0, f1, f2, f3 FROM p) WHERE f2 > 1", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 50d),
                planRow(1, RootExchangePhysicalRel.class, "", 50d),
                planRow(2, ProjectPhysicalRel.class, "EXPR$0=[+($0, $1)]", 50d),
                planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1], filter=>($2, 1)]]]", 50d)
            )
        );
    }

    /**
     * Before: Project2(expression) <- Filter <- Project1(expression) <- Scan
     * After : Project <- Project <- Scan(Project, Filter)
     *
     */
    @Test
    public void testProjectExpressionFilterProjectExpressionIntoScan() {
        assertPlan(
            optimizePhysical("SELECT d1 + f2 FROM (SELECT f0 + f1 d1, f2, f3, f4 FROM p) WHERE f3 > 1", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 50d),
                planRow(1, RootExchangePhysicalRel.class, "", 50d),
                planRow(2, ProjectPhysicalRel.class, "EXPR$0=[+(+($0, $1), $2)]", 50d),
                planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2], filter=>($3, 1)]]]", 50d)
            )
        );
    }

    /**
     * Before: Project1 <- Filter1 <- Project2 <- Filter2 <- Scan
     * After : Scan(Project1, Filter1 and Filter2)
     */
    @Test
    public void testProjectFilterProjectFilterIntoScan() {
        assertPlan(
            optimizePhysical("SELECT f0 FROM (SELECT f0, f1, f2 FROM p WHERE f3 > 1) WHERE f1 > 1", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 25d),
                planRow(1, RootExchangePhysicalRel.class, "", 25d),
                planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0], filter=AND(>($3, 1), >($1, 1))]]]", 25d)
            )
        );
    }

    /**
     * Before: Project1 <- Filter1 <- Project2(expression) <- Filter2 <- Scan
     * After : Project1 <- Project2 <- Scan(Project, Filter1 and Filter2)
     */
    @Test
    public void testProjectFilterProjectExpressionFilterIntoScan() {
        assertPlan(
            optimizePhysical("SELECT d1, f2 FROM (SELECT f0 + f1 d1, f2, f3, f4 FROM p WHERE f3 > 1) WHERE f2 > 2", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 25d),
                planRow(1, RootExchangePhysicalRel.class, "", 25d),
                planRow(2, ProjectPhysicalRel.class, "d1=[+($0, $1)], f2=[$2]", 25d),
                planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2], filter=AND(>($3, 1), >($2, 2))]]]", 25d)
            )
        );
    }

    /**
     * Before: Project1(expression) <- Filter1 <- Project2 <- Filter2 <- Scan
     * After : Project <- Scan(Project, Filter1 and Filter2)
     */
    @Test
    public void testProjectExpressionFilterProjectFilterIntoScan() {
        assertPlan(
            optimizePhysical("SELECT f0 + f1 FROM (SELECT f0, f1, f2, f3 FROM p WHERE f3 > 1) WHERE f2 > 1", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 25d),
                planRow(1, RootExchangePhysicalRel.class, "", 25d),
                planRow(2, ProjectPhysicalRel.class, "EXPR$0=[+($0, $1)]", 25d),
                planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1], filter=AND(>($3, 1), >($2, 1))]]]", 25d)
            )
        );
    }

    /**
     * Before: Project1(expression) <- Filter1 <- Project2(expression) <- Filter2 <- Scan
     * After : Project1 <- Project2 <- Scan(Project, Filter1 and Filter2)
     */
    @Test
    public void testProjectExpressionFilterProjectExpressionFilterIntoScan() {
        assertPlan(
            optimizePhysical("SELECT d1 + f2 FROM (SELECT f0 + f1 d1, f2, f3, f4 FROM p WHERE f3 > 1) WHERE f2 > 2", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 25d),
                planRow(1, RootExchangePhysicalRel.class, "", 25d),
                planRow(2, ProjectPhysicalRel.class, "EXPR$0=[+(+($0, $1), $2)]", 25d),
                planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2], filter=AND(>($3, 1), >($2, 2))]]]", 25d)
            )
        );
    }

    @Test
    public void testFilterCompoundExpression() {
        assertPlan(
            optimizePhysical("SELECT f2 FROM (SELECT f0 + f1 d1, f2 FROM p) WHERE d1 + f2 > 2", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 50d),
                planRow(1, RootExchangePhysicalRel.class, "", 50d),
                planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[2], filter=>(+(+($0, $1), $2), 2)]]]", 50d)
            )
        );
    }
}
