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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;

public class SortPhysicalRel extends Sort implements PhysicalRel {

    private final RelDataType rowType;

    SortPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            RelCollation collation,
            RexNode offset,
            RexNode fetch,
            RelDataType rowType
    ) {
        super(cluster, traits, input, collation, offset, fetch);

        this.rowType = rowType;
    }

    public Expression<?> fetch(QueryParameterMetadata parameterMetadata) {
        PlanNodeSchema schema = schema(parameterMetadata);
        RexVisitor<Expression<?>> visitor = OptUtils.createRexToExpressionVisitor(schema, parameterMetadata);
        return fetch.accept(visitor);
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        return OptUtils.schema(rowType);
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onSort(this);
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new SortPhysicalRel(getCluster(), traitSet, input, collation, offset, fetch, rowType);
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }
}
