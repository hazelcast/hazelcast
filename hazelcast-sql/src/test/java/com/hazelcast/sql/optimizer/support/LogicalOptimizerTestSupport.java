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

package com.hazelcast.sql.optimizer.support;

import com.hazelcast.sql.impl.calcite.ExpressionConverterRexVisitor;
import com.hazelcast.sql.impl.calcite.logical.rel.LogicalRel;
import com.hazelcast.sql.impl.calcite.logical.rel.MapScanLogicalRel;
import com.hazelcast.sql.impl.calcite.logical.rel.ProjectLogicalRel;
import com.hazelcast.sql.impl.calcite.logical.rel.RootLogicalRel;
import com.hazelcast.sql.impl.expression.Expression;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

/**
 * Utility methods for logical optimizer tests.
 */
public abstract class LogicalOptimizerTestSupport extends OptimizerTestSupport {
    @Override
    protected final boolean isOptimizePhysical() {
        return false;
    }

    /**
     * Perform logical optimization.
     *
     * @param sql SQL.
     * @return Input of the root node
     */
    protected RelNode optimizeLogical(String sql){
        LogicalRel rel = optimize(sql).getLogical();

        RootLogicalRel root = assertRoot(rel);

        return root.getInput();
    }

    protected static RootLogicalRel assertRoot(RelNode node) {
        return assertClass(node, RootLogicalRel.class);
    }

    protected static void assertScan(RelNode node, List<String> expFields, List<Integer> expProjects, Expression expFilter) {
        MapScanLogicalRel scan = assertClass(node, MapScanLogicalRel.class);

        assertFields(expFields, scan.getTable().getRowType().getFieldNames());
        assertProjectedFields(expProjects, scan.getProjects());

        Expression filter = scan.getFilter() != null ? scan.getFilter().accept(ExpressionConverterRexVisitor.INSTANCE) : null;

        assertEquals(expFilter, filter);
    }

    protected static void assertFields(List<String> expFields, List<String> fields) {
        if (fields == null) {
            fields = new ArrayList<>();
        } else {
            fields = new ArrayList<>(fields);
        }

        assertEquals(expFields, fields);
    }

    protected static void assertProjectedFields(List<Integer> expProjects, List<Integer> projects) {
        if (projects == null) {
            projects = new ArrayList<>();
        } else {
            projects = new ArrayList<>(projects);
        }

        assertEquals(expProjects, projects);
    }

    protected ProjectLogicalRel assertProject(RelNode rel, List<Expression> expProjects) {
        ProjectLogicalRel project = assertClass(rel, ProjectLogicalRel.class);

        List<Expression> projects = new ArrayList<>();

        for (RexNode projectExpr : project.getProjects()) {
            projects.add(projectExpr.accept(ExpressionConverterRexVisitor.INSTANCE));
        }

        expProjects = expProjects != null ? new ArrayList<>(expProjects) : new ArrayList<>();

        assertEquals(expProjects, projects);

        return project;
    }

    @SuppressWarnings("unchecked")
    protected static <T> T assertClass(RelNode rel, Class<? extends LogicalRel> expClass) {
        assertEquals(expClass, rel.getClass());

        return (T)rel;
    }
}
