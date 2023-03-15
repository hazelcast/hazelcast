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

import com.hazelcast.jet.sql.impl.opt.FieldCollation;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.stream.Collectors;

public class SortPhysicalRel extends Sort implements PhysicalRel {

    SortPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            RelCollation collation,
            RelDataType rowType
    ) {
        super(cluster, traits, input, collation, null, null);
        this.rowType = rowType;
    }

    public List<FieldCollation> getCollations() {
        return getCollation().getFieldCollations()
                .stream().map(FieldCollation::new).collect(Collectors.toList());
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        return OptUtils.schema(rowType);
    }

    @Override
    public <V> V accept(CreateDagVisitor<V> visitor) {
        return visitor.onSort(this);
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new SortPhysicalRel(getCluster(), traitSet, input, collation, rowType);
    }
}
