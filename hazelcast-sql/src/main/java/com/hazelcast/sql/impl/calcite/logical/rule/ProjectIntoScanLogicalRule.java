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

package com.hazelcast.sql.impl.calcite.logical.rule;

import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.logical.rel.MapScanLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.List;

public final class ProjectIntoScanLogicalRule extends RelOptRule {
    public static final ProjectIntoScanLogicalRule INSTANCE = new ProjectIntoScanLogicalRule();

    private ProjectIntoScanLogicalRule() {
        super(
            operand(Project.class,
                operandJ(TableScan.class, null, MapScanLogicalRel::isProjectableFilterable, none())),
            RelFactories.LOGICAL_BUILDER,
            ProjectIntoScanLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        TableScan scan = call.rel(1);

        Mappings.TargetMapping mapping = project.getMapping();

        RelNode transformed;

        if (mapping == null) {
            transformed = processComplex(project, scan);

            if (transformed == null) {
                // TODO: Remove when implemented.
                return;
            }
        } else if (Mappings.isIdentity(mapping)) {
            // Project returns all the rows of the scan. Let ProjectRemoveRule do it's job.
            return;
        } else {
            transformed = processSimple(mapping, scan);
        }

        call.transformTo(transformed);
    }

    /**
     * Process simple case when all project expressions are direct field access.
     *
     * @param mapping Projects mapping.
     * @param scan Scan.
     * @return Transformed node (new scan).
     */
    private static RelNode processSimple(Mappings.TargetMapping mapping, TableScan scan) {
        List<Integer> oldProjects = null;
        RexNode filter = null;

        if (scan instanceof MapScanLogicalRel) {
            MapScanLogicalRel scan0 = (MapScanLogicalRel) scan;

            oldProjects = scan0.getProjects();
            filter = scan0.getFilter();

        }

        if (oldProjects == null) {
            oldProjects = scan.identity();
        }

        List<Integer> newProjects = Mappings.apply((Mapping) mapping, oldProjects);

        return new MapScanLogicalRel(
            scan.getCluster(),
            RuleUtils.toLogicalConvention(scan.getTraitSet()),
            scan.getTable(),
            newProjects,
            filter
        );
    }

    /**
     * Process complex project with expressions. Projection will remain as is, but the number of returned fields might be decreased in scan.
     *
     * @param project Project.
     * @param scan Scan.
     * @return Transformed node (new project with new scan).
     */
    private RelNode processComplex(Project project, TableScan scan) {
        // TODO: Early exit here produce suboptimal results. Consider that we have [Project(a + 1) <- Filter(b) <- Scan] tree.
        // TODO: Calcite will access [a, b] columns which will be recorded inside the scan row type.
        // TODO: After merging filter and scan we will get: [Project(a + 1) <- Scan(a, b; filter(b))].
        // TODO: Since this project is not identity (i.e. it is not a plain column), we will exit. So final scan will return [a, b]
        // TODO: columns, even though only [a] is needed by the project.
        // TODO: What we need to do here instead, is visit all project expressions and collect accessed columns. Then we should compare
        // TODO: those columns with the columns recorded in Scan dynamic row type and remove unneccesary scan columns, shifting indexes
        // TODO: in project expressions accordingly. This was already done in the first version of this rule, with the idea borrowed
        // TODO: from Drill. Need to return. See commit db80cf0a.
        return null;
    }
}
