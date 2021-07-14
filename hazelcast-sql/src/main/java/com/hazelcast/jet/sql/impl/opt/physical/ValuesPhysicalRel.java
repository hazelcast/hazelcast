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
import com.hazelcast.jet.sql.impl.opt.ExpressionValues;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import java.util.stream.Collectors;

public class ValuesPhysicalRel extends AbstractRelNode implements PhysicalRel {

    private final List<ExpressionValues> values;

    ValuesPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelDataType rowType,
            List<ExpressionValues> values
    ) {
        super(cluster, traits);

        this.rowType = rowType;
        this.values = values;
    }

    public List<ExpressionValues> values() {
        return values;
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        return OptUtils.schema(getRowType());
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onValues(this);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ValuesPhysicalRel(getCluster(), traitSet, getRowType(), values);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("values", values.stream().map(ExpressionValues::toString).collect(Collectors.joining(", ")));
    }
}
