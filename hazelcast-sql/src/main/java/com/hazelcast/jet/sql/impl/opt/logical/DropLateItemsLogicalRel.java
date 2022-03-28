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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.function.ToLongFunction;

public class DropLateItemsLogicalRel extends SingleRel implements LogicalRel {
    private final RexNode wmField;
    private final ToLongFunction<ExpressionEvalContext> allowedLagProvider;

    protected DropLateItemsLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RexNode wmField,
            ToLongFunction<ExpressionEvalContext> allowedLagProvider
    ) {
        super(cluster, traitSet, input);
        this.wmField = wmField;
        this.allowedLagProvider = allowedLagProvider;
    }

    public RexNode wmField() {
        return wmField;
    }

    public ToLongFunction<ExpressionEvalContext> allowedLagProvider() {
        return allowedLagProvider;
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DropLateItemsLogicalRel(getCluster(), traitSet, sole(inputs), wmField, allowedLagProvider);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("traitSet", traitSet);
    }
}
