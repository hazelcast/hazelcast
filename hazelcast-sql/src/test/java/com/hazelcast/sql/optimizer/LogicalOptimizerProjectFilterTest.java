/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.math.PlusMinusFunction;
import com.hazelcast.sql.impl.expression.predicate.AndOrPredicate;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
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

        assertScan(rootInput, list("f1", "f2"), list(0, 1), null);
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
                new PlusMinusFunction(new ColumnExpression(0), new ColumnExpression(1), false),
                new ColumnExpression(2)
            )
        );

        assertScan(project.getInput(), list("f1", "f2", "f3"), list(0, 1, 2), null);
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
            list("f3", "f1", "f2"),
            list(1, 2),
            new ComparisonPredicate(
                new ColumnExpression(0),
                new ConstantExpression<>(1),
                CallOperator.GREATER_THAN
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
                new PlusMinusFunction(new ColumnExpression(0), new ColumnExpression(1), false),
                new ColumnExpression(2)
            )
        );

        assertScan(
            project.getInput(),
            list("f4", "f1", "f2", "f3"),
            list(1, 2, 3),
            new ComparisonPredicate(new ColumnExpression(0), new ConstantExpression<>(1), CallOperator.GREATER_THAN)
        );
    }

    /**
     * Before: Project2 <- Project1 <- Scan
     * After : Scan(Project2)
     */
    @Test
    public void testProjectProjectIntoScan() {
        RelNode rootInput = optimizeLogical("SELECT f1 FROM (SELECT f1, f2 FROM p)");

        assertScan(rootInput, list("f1", "f2"), list(0), null);
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
                new PlusMinusFunction(new ColumnExpression(0), new ColumnExpression(1), false),
                new ColumnExpression(2)
            )
        );

        assertScan(project.getInput(), list("f1", "f2", "f3", "f4"), list(0, 1, 2), null);
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
                new PlusMinusFunction(new ColumnExpression(0), new ColumnExpression(1), false),
                new ColumnExpression(2)
            )
        );

        assertScan(project.getInput(), list("f1", "f2", "f3", "f4"), list(0, 1, 2), null);
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
                new PlusMinusFunction(
                    new PlusMinusFunction(new ColumnExpression(0), new ColumnExpression(1), false),
                    new ColumnExpression(2),
                    false
                )
            )
        );

        assertScan(project.getInput(), list("f1", "f2", "f3", "f4"), list(0, 1, 2), null);
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
            list("f3", "f1", "f2"),
            list(1),
            new ComparisonPredicate(new ColumnExpression(0), new ConstantExpression<>(1), CallOperator.GREATER_THAN)
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
                new PlusMinusFunction(new ColumnExpression(0), new ColumnExpression(1), false),
                new ColumnExpression(2)
            )
        );

        assertScan(
            project.getInput(),
            list("f5", "f1", "f2", "f3", "f4"),
            list(1, 2, 3),
            new ComparisonPredicate(new ColumnExpression(0), new ConstantExpression<>(1), CallOperator.GREATER_THAN)
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
                new PlusMinusFunction(new ColumnExpression(0), new ColumnExpression(1), false)
            )
        );

        assertScan(
            project.getInput(),
            list("f3", "f1", "f2", "f4"),
            list(1, 2),
            new ComparisonPredicate(new ColumnExpression(0), new ConstantExpression<>(1), CallOperator.GREATER_THAN)
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
                new PlusMinusFunction(
                    new PlusMinusFunction(new ColumnExpression(0), new ColumnExpression(1), false),
                    new ColumnExpression(2),
                    false
                )
            )
        );

        assertScan(
            project.getInput(),
            list("f5", "f1", "f2", "f3", "f4"),
            list(1, 2, 3),
            new ComparisonPredicate(new ColumnExpression(0), new ConstantExpression<>(1), CallOperator.GREATER_THAN)
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
            list("f1", "f2", "f3"), list(0),
            new ComparisonPredicate(new ColumnExpression(1), new ConstantExpression<>(1), CallOperator.GREATER_THAN)
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
                new ColumnExpression(0),
                new ColumnExpression(1)
            )
        );

        ProjectLogicalRel bottomProject = assertProject(
            topProject.getInput(),
            list(
                new PlusMinusFunction(new ColumnExpression(0), new ColumnExpression(1), false),
                new ColumnExpression(2),
                new ColumnExpression(3)
            )
        );

        assertScan(
            bottomProject.getInput(),
            list("f1", "f2", "f3", "f4", "f5"), list(0, 1, 2, 3),
            new ComparisonPredicate(new ColumnExpression(3), new ConstantExpression<>(1), CallOperator.GREATER_THAN)
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
                new PlusMinusFunction(new ColumnExpression(0), new ColumnExpression(1), false)
            )
        );

        assertScan(
            project.getInput(),
            list("f1", "f2", "f3", "f4"), list(0, 1),
            new ComparisonPredicate(new ColumnExpression(2), new ConstantExpression<>(1), CallOperator.GREATER_THAN)
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
                new PlusMinusFunction(new ColumnExpression(0), new ColumnExpression(1), false)
            )
        );

        ProjectLogicalRel bottomProject = assertProject(
            topProject.getInput(),
            list(
                new PlusMinusFunction(new ColumnExpression(0), new ColumnExpression(1), false),
                new ColumnExpression(2),
                new ColumnExpression(3)
            )
        );

        assertScan(
            bottomProject.getInput(),
            list("f1", "f2", "f3", "f4", "f5"),
            list(0, 1, 2, 3),
            new ComparisonPredicate(new ColumnExpression(3), new ConstantExpression<>(1), CallOperator.GREATER_THAN)
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
            list("f4", "f1", "f2", "f3"),
            list(1),
            new AndOrPredicate(
                new ComparisonPredicate(new ColumnExpression(0), new ConstantExpression<>(1), CallOperator.GREATER_THAN),
                new ComparisonPredicate(new ColumnExpression(2), new ConstantExpression<>(1), CallOperator.GREATER_THAN),
                false
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
                new PlusMinusFunction(new ColumnExpression(0), new ColumnExpression(1), false),
                new ColumnExpression(2)
            )
        );

        assertScan(
            project.getInput(),
            list("f4", "f1", "f2", "f3", "f5"),
            list(1, 2, 3),
            new AndOrPredicate(
                new ComparisonPredicate(new ColumnExpression(0), new ConstantExpression<>(1), CallOperator.GREATER_THAN),
                new ComparisonPredicate(new ColumnExpression(3), new ConstantExpression<>(2), CallOperator.GREATER_THAN),
                false
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
                new PlusMinusFunction(new ColumnExpression(0), new ColumnExpression(1), false)
            )
        );

        assertScan(
            project.getInput(),
            list("f4", "f1", "f2", "f3"),
            list(1, 2),
            new AndOrPredicate(
                new ComparisonPredicate(new ColumnExpression(0), new ConstantExpression<>(1), CallOperator.GREATER_THAN),
                new ComparisonPredicate(new ColumnExpression(3), new ConstantExpression<>(1), CallOperator.GREATER_THAN),
                false
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
                new PlusMinusFunction(new ColumnExpression(0), new ColumnExpression(1), false)
            )
        );

        ProjectLogicalRel bottomProject = assertProject(
            topProject.getInput(),
            list(
                new PlusMinusFunction(new ColumnExpression(0), new ColumnExpression(1), false),
                new ColumnExpression(2)
            )
        );

        assertScan(
            bottomProject.getInput(),
            list("f4", "f1", "f2", "f3", "f5"),
            list(1, 2, 3),
            new AndOrPredicate(
                new ComparisonPredicate(new ColumnExpression(0), new ConstantExpression<>(1), CallOperator.GREATER_THAN),
                new ComparisonPredicate(new ColumnExpression(3), new ConstantExpression<>(2), CallOperator.GREATER_THAN),
                false
            )
        );
    }
}
