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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.OptUtils.createRexToExpressionVisitor;

public class DropLateItemsPhysicalRel extends SingleRel implements PhysicalRel {
    private final RexNode wmField;

    protected DropLateItemsPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RexNode wmField
    ) {
        super(cluster, traitSet, input);
        this.wmField = wmField;
    }

    public Expression<?> timestampExpression() {
        QueryParameterMetadata parameterMetadata = ((HazelcastRelOptCluster) getCluster()).getParameterMetadata();
        RexVisitor<Expression<?>> visitor = createRexToExpressionVisitor(schema(parameterMetadata), parameterMetadata);
        return wmField.accept(visitor);
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        return OptUtils.schema(getRowType());
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onDropLateItems(this);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DropLateItemsPhysicalRel(getCluster(), traitSet, sole(inputs), wmField);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeHugeCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("traitSet", traitSet);
    }
}
