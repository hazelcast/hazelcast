/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.calcite.opt.AbstractFilterRel;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

public class FilterPhysicalRel extends AbstractFilterRel implements PhysicalRel {

    FilterPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            RexNode condition
    ) {
        super(cluster, traits, input, condition);
    }

    public Expression<Boolean> filter() {
        return filter(schema(), condition);
    }

    @Override
    public PlanNodeSchema schema() {
        return ((PhysicalRel) getInput()).schema();
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onFilter(this);
    }

    @Override
    public final Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new FilterPhysicalRel(getCluster(), traitSet, input, condition);
    }
}
