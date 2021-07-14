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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;

public class SortLogicalRel extends Sort implements LogicalRel {

    SortLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            RelCollation collation,
            RexNode offset,
            RexNode fetch
    ) {
        super(cluster, traits, input, collation, offset, fetch);
    }

    public RexNode getFetch() {
        return fetch;
    }

    @Override
    public Sort copy(RelTraitSet traits, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new SortLogicalRel(getCluster(), traits, input, collation, offset, fetch);
    }
}
