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

package com.hazelcast.sql.optimizer;

import com.hazelcast.sql.impl.calcite.opt.logical.ProjectLogicalRel;
import com.hazelcast.sql.impl.expression.math.PlusFunction;
import com.hazelcast.sql.impl.expression.predicate.AndPredicate;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.optimizer.support.LogicalOptimizerTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.RelNode;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test for project/filter optimizations.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LogicalOptimizerProjectFilterTest extends LogicalOptimizerTestSupport {
    /**
     * Before: Project <- Scan
     * After : Scan(Project)
     */
    @Test
    public void testProjectIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT f1, f2 FROM p");

        assertScan(rootInput, list(0, 1), null);
    }

    /**
     * Before: Project(exp) <- Scan
     * After : Project(exp) <- Scan(Project)
     */
    @Test
    public void testProjectExpressionIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT f1 + f2, f3 FROM p");

        ProjectLogicalRel project = assertProject(
            rootInput,
            list(
                PlusFunction.create(column(0), column(1)),
                column(2)
            )
        );

        assertScan(project.getInput(), list(0, 1, 2), null);
    }

    /**
     * Before: Project <- Filter <- Scan
     * After : Scan(Project, Filter)
     */
    @Test
    public void testProjectFilterIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT f1, f2 FROM p WHERE f3 > 1");

        assertScan(
            rootInput,
            list(0, 1),
            compare(
                column(2),
                constant(1),
                ComparisonMode.GREATER_THAN
            )
        );
    }

    /**
     * Before: Project(exp) <- Filter <- Scan
     * After : Project(exp) <- Scan(Project, Filter)
     */
    @Test
    public void testProjectExpressionFilterScan() {
        RelNode rootInput = optimizeLogical("SELECT f1 + f2, f3 FROM p WHERE f4 > 1");

        ProjectLogicalRel project = assertProject(
            rootInput,
            list(
                PlusFunction.create(column(0), column(1)),
                column(2)
            )
        );

        assertScan(
            project.getInput(),
            list(0, 1, 2),
            compare(column(3), constant(1), ComparisonMode.GREATER_THAN)
        );
    }

    /**
     * Before: Project2 <- Project1 <- Scan
     * After : Scan(Project2)
     */
    @Test
    public void testProjectProjectIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT f1 FROM (SELECT f1, f2 FROM p)");

        assertScan(rootInput, list(0), null);
    }

    /**
     * Before: Project2 <- Project1(expression) <- Scan
     * After : Scan(Project2)
     */
    @Test
    public void testProjectProjectExpressionIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT d1, f3 FROM (SELECT f1 + f2 d1, f3, f4 FROM p)");

        ProjectLogicalRel project = assertProject(
            rootInput,
            list(
                PlusFunction.create(column(0), column(1)),
                column(2)
            )
        );

        assertScan(project.getInput(), list(0, 1, 2), null);
    }

    /**
     * Before: Project2(expression) <- Project1 <- Scan
     * After : Project2 <- Scan(Project2)
     */
    @Test
    public void testProjectExpressionProjectIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT f1 + f2, f3 FROM (SELECT f1, f2, f3, f4 FROM p)");

        ProjectLogicalRel project = assertProject(
            rootInput,
            list(
                PlusFunction.create(column(0), column(1)),
                column(2)
            )
        );

        assertScan(project.getInput(), list(0, 1, 2), null);
    }

    /**
     * Before: Project2(expression) <- Project1(expression) <- Scan
     * After : Scan(Project2)
     */
    @Test
    public void testProjectExpressionProjectExpressionIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT d1 + f3 FROM (SELECT f1 + f2 d1, f3, f4 FROM p)");

        ProjectLogicalRel project = assertProject(
            rootInput,
            list(
                PlusFunction.create(
                    PlusFunction.create(column(0), column(1)),
                    column(2)
                )
            )
        );

        assertScan(project.getInput(), list(0, 1, 2), null);
    }

    /**
     * Before: Project2 <- Project1 <- Filter <- Scan
     * After : Scan(Project2, Filter)
     */
    @Test
    public void testProjectProjectFilterIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT f1 FROM (SELECT f1, f2 FROM p WHERE f3 > 1)");

        assertScan(
            rootInput,
            list(0),
            compare(column(2), constant(1), ComparisonMode.GREATER_THAN)
        );
    }

    /**
     * Before: Project2 <- Project1(expression) <- Filter <- Scan
     * After : Project <- Scan(Project, Filter)
     */
    @Test
    public void testProjectProjectExpressionFilterIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT d1, f3 FROM (SELECT f1 + f2 d1, f3, f4 FROM p WHERE f5 > 1)");

        ProjectLogicalRel project = assertProject(
            rootInput,
            list(
                PlusFunction.create(column(0), column(1)),
                column(2)
            )
        );

        assertScan(
            project.getInput(),
            list(0, 1, 2),
            compare(column(4), constant(1), ComparisonMode.GREATER_THAN)
        );
    }

    /**
     * Before: Project2(expression) <- Project1 <- Filter <- Scan
     * After : Project <- Scan(Project, Filter)
     */
    @Test
    public void testProjectExpressionProjectFilterIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT f1 + f2 FROM (SELECT f1, f2, f3, f4 FROM p WHERE f3 > 1)");

        ProjectLogicalRel project = assertProject(
            rootInput,
            list(
                PlusFunction.create(column(0), column(1))
            )
        );

        assertScan(
            project.getInput(),
            list(0, 1),
            compare(column(2), constant(1), ComparisonMode.GREATER_THAN)
        );
    }

    /**
     * Before: Project2(expression) <- Project1(expression) <- Filter <- Scan
     * After : Project <- Scan(Project, Filter)
     */
    @Test
    public void testProjectExpressionProjectExpressionFilterIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT d1 + f3 FROM (SELECT f1 + f2 d1, f3, f4 FROM p WHERE f5 > 1)");

        ProjectLogicalRel project = assertProject(
            rootInput,
            list(
                PlusFunction.create(
                    PlusFunction.create(column(0), column(1)),
                    column(2)
                )
            )
        );

        assertScan(
            project.getInput(),
            list(0, 1, 2),
            compare(column(4), constant(1), ComparisonMode.GREATER_THAN)
        );
    }

    /**
     * Before: Project2 <- Filter <- Project1 <- Scan
     * After : Scan(Project2, Filter)
     */
    @Test
    public void testProjectFilterProjectIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT f1 FROM (SELECT f1, f2, f3 FROM p) WHERE f2 > 1");

        assertScan(
            rootInput,
            list(0),
            compare(column(1), constant(1), ComparisonMode.GREATER_THAN)
        );
    }

    /**
     * Before: Project2 <- Filter <- Project1(expression) <- Scan
     * After : Project <- Project <- Scan(Project2, Filter)
     *
     */
    @Test
    public void testProjectFilterProjectExpressionIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT d1, f3 FROM (SELECT f1 + f2 d1, f3, f4, f5 FROM p) WHERE f4 > 1");

        // TODO: Two projects cannot be merged together because ProjectMergeRule is disabled. Implement or fail this test intentionally.
        ProjectLogicalRel topProject = assertProject(
            rootInput,
            list(
                column(0),
                column(1)
            )
        );

        ProjectLogicalRel bottomProject = assertProject(
            topProject.getInput(),
            list(
                PlusFunction.create(column(0), column(1)),
                column(2),
                column(3)
            )
        );

        assertScan(
            bottomProject.getInput(),
            list(0, 1, 2, 3),
            compare(column(3), constant(1), ComparisonMode.GREATER_THAN)
        );
    }

    /**
     * Before: Project2(expression) <- Filter <- Project1 <- Scan
     * After : Project <-  Scan(Project, Filter)
     *
     */
    @Test
    public void testProjectExpressionFilterProjectIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT f1 + f2 FROM (SELECT f1, f2, f3, f4 FROM p) WHERE f3 > 1");

        ProjectLogicalRel project = assertProject(
            rootInput,
            list(
                PlusFunction.create(column(0), column(1))
            )
        );

        assertScan(
            project.getInput(),
            list(0, 1),
            compare(column(2), constant(1), ComparisonMode.GREATER_THAN)
        );
    }

    /**
     * Before: Project2(expression) <- Filter <- Project1(expression) <- Scan
     * After : Project <- Project <- Scan(Project, Filter)
     *
     */
    @Test
    public void testProjectExpressionFilterProjectExpressionIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT d1 + f3 FROM (SELECT f1 + f2 d1, f3, f4, f5 FROM p) WHERE f4 > 1");

        // TODO: Two projects cannot be merged together because ProjectMergeRule is disabled. Implement or fail this test intentionally.
        ProjectLogicalRel topProject = assertProject(
            rootInput,
            list(
                PlusFunction.create(column(0), column(1))
            )
        );

        ProjectLogicalRel bottomProject = assertProject(
            topProject.getInput(),
            list(
                PlusFunction.create(column(0), column(1)),
                column(2),
                column(3)
            )
        );

        assertScan(
            bottomProject.getInput(),
            list(0, 1, 2, 3),
            compare(column(3), constant(1), ComparisonMode.GREATER_THAN)
        );
    }

    /**
     * Before: Project1 <- Filter1 <- Project2 <- Filter2 <- Scan
     * After : Scan(Project1, Filter1 and Filter2)
     */
    @Test
    public void testProjectFilterProjectFilterIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT f1 FROM (SELECT f1, f2, f3 FROM p WHERE f4 > 1) WHERE f2 > 1");

        assertScan(
            rootInput,
            list(0),
            AndPredicate.create(
                compare(column(3), constant(1), ComparisonMode.GREATER_THAN),
                compare(column(1), constant(1), ComparisonMode.GREATER_THAN)
            )
        );
    }

    /**
     * Before: Project1 <- Filter1 <- Project2(expression) <- Filter2 <- Scan
     * After : Project1 <- Project2 <- Scan(Project, Filter1 and Filter2)
     */
    @Test
    public void testProjectFilterProjectExpressionFilterIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT d1, f3 FROM (SELECT f1 + f2 d1, f3, f4, f5 FROM p WHERE f4 > 1) WHERE f3 > 2");

        ProjectLogicalRel project = assertProject(
            rootInput,
            list(
                PlusFunction.create(column(0), column(1)),
                column(2)
            )
        );

        assertScan(
            project.getInput(),
            list(0, 1, 2),
            AndPredicate.create(
                compare(column(3), constant(1), ComparisonMode.GREATER_THAN),
                compare(column(2), constant(2), ComparisonMode.GREATER_THAN)
            )
        );
    }

    /**
     * Before: Project1(expression) <- Filter1 <- Project2 <- Filter2 <- Scan
     * After : Project <- Scan(Project, Filter1 and Filter2)
     */
    @Test
    public void testProjectExpressionFilterProjectFilterIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT f1 + f2 FROM (SELECT f1, f2, f3, f4 FROM p WHERE f4 > 1) WHERE f3 > 1");

        ProjectLogicalRel project = assertProject(
            rootInput,
            list(
                PlusFunction.create(column(0), column(1))
            )
        );

        assertScan(
            project.getInput(),
            list(0, 1),
            AndPredicate.create(
                compare(column(3), constant(1), ComparisonMode.GREATER_THAN),
                compare(column(2), constant(1), ComparisonMode.GREATER_THAN)
            )
        );
    }

    /**
     * Before: Project1(expression) <- Filter1 <- Project2(expression) <- Filter2 <- Scan
     * After : Project1 <- Project2 <- Scan(Project, Filter1 and Filter2)
     */
    @Test
    public void testProjectExpressionFilterProjectExpressionFilterIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT d1 + f3 FROM (SELECT f1 + f2 d1, f3, f4, f5 FROM p WHERE f4 > 1) WHERE f3 > 2");

        // TODO: Two projects cannot be merged together because ProjectMergeRule is disabled. Implement or fail this test intentionally.
        ProjectLogicalRel topProject = assertProject(
            rootInput,
            list(
                PlusFunction.create(column(0), column(1))
            )
        );

        ProjectLogicalRel bottomProject = assertProject(
            topProject.getInput(),
            list(
                PlusFunction.create(column(0), column(1)),
                column(2)
            )
        );

        assertScan(
            bottomProject.getInput(),
            list(0, 1, 2),
            AndPredicate.create(
                compare(column(3), constant(1), ComparisonMode.GREATER_THAN),
                compare(column(2), constant(2), ComparisonMode.GREATER_THAN)
            )
        );
    }
}
