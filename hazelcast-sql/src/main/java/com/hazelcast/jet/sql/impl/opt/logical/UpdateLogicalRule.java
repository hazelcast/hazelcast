/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;

final class UpdateLogicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new UpdateLogicalRule();

    private UpdateLogicalRule() {
        super(
                operandJ(LogicalTableModify.class, null, TableModify::isUpdate, operand(LogicalProject.class, any())),
                RelFactories.LOGICAL_BUILDER,
                UpdateLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalTableModify update = call.rel(0);
        LogicalProject project = call.rel(1);

        UpdateLogicalRel rel = new UpdateLogicalRel(
                update.getCluster(),
                OptUtils.toLogicalConvention(update.getTraitSet()),
                update.getTable(),
                update.getCatalogReader(),
                OptUtils.toLogicalInput(project.getInput()),
                update.getOperation(),
                update.getUpdateColumnList(),
                update.getSourceExpressionList(),
                update.isFlattened(),
                project.getProjects()
        );
        call.transformTo(rel);
    }
}
