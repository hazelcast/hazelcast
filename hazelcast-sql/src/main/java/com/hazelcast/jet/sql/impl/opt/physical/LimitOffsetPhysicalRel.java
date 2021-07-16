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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;

import java.util.List;

public class LimitOffsetPhysicalRel extends SingleRel implements PhysicalRel {
    private final RexNode limit;
    private final RexNode offset;

    LimitOffsetPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            RexNode limit,
            RexNode offset
    ) {
        super(cluster, traits, input);
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LimitOffsetPhysicalRel(getCluster(), traitSet, sole(inputs), getLimit(), getOffset());
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        return OptUtils.schema(getRowType());
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onLimitOffset(this);
    }

    public RexNode getLimit() {
        return limit;
    }

    public RexNode getOffset() {
        return offset;
    }

    public Expression<?> limit(QueryParameterMetadata parameterMetadata) {
        if (limit == null) {
            return null;
        }
        PlanNodeSchema schema = schema(parameterMetadata);
        RexVisitor<Expression<?>> visitor = OptUtils.createRexToExpressionVisitor(schema, parameterMetadata);
        return limit.accept(visitor);
    }

    public Expression<?> offset(QueryParameterMetadata parameterMetadata) {
        if (offset == null) {
            return null;
        }
        PlanNodeSchema schema = schema(parameterMetadata);
        RexVisitor<Expression<?>> visitor = OptUtils.createRexToExpressionVisitor(schema, parameterMetadata);
        return offset.accept(visitor);
    }
}
