/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
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

public class LimitPhysicalRel extends SingleRel implements PhysicalRel {

    private final RexNode offset;

    private final RexNode fetch;

    /**
     * Creates a {@link LimitPhysicalRel}.
     */
    protected LimitPhysicalRel(RexNode offset, RexNode fetch, RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
        this.offset = offset;
        this.fetch = fetch;
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        return ((PhysicalRel) input).schema(parameterMetadata);
    }

    @Override
    public <V> V accept(CreateDagVisitor<V> visitor) {
        return visitor.onLimit(this);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LimitPhysicalRel(offset, fetch, getCluster(), traitSet, sole(inputs));
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .itemIf("offset", offset, offset != null)
                .itemIf("fetch", fetch, fetch != null);
    }

    public Expression<?> offset(QueryParameterMetadata parameterMetadata) {
        PlanNodeSchema schema = schema(parameterMetadata);
        RexVisitor<Expression<?>> visitor = OptUtils.createRexToExpressionVisitor(schema, parameterMetadata);
        return offset.accept(visitor);
    }

    public Expression<?> fetch(QueryParameterMetadata parameterMetadata) {
        PlanNodeSchema schema = schema(parameterMetadata);
        RexVisitor<Expression<?>> visitor = OptUtils.createRexToExpressionVisitor(schema, parameterMetadata);
        return fetch.accept(visitor);
    }

    public RexNode offset() {
        return offset;
    }

    public RexNode fetch() {
        return fetch;
    }
}
