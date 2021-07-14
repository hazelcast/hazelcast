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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

public class FilterLogicalRel extends Filter implements LogicalRel {

    FilterLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            RexNode condition
    ) {
        super(cluster, traits, input, condition);
    }

    @Override
    public final Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new FilterLogicalRel(getCluster(), traitSet, input, condition);
    }
}
