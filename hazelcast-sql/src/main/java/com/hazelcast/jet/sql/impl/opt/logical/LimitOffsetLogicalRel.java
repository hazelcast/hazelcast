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
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class LimitOffsetLogicalRel extends SingleRel implements LogicalRel {
    private final RexNode limit;
    private final RexNode offset;

    LimitOffsetLogicalRel(
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
        return new LimitOffsetLogicalRel(getCluster(), traitSet, sole(inputs), getLimit(), getOffset());
    }

    public RexNode getLimit() {
        return limit;
    }

    public RexNode getOffset() {
        return offset;
    }
}
