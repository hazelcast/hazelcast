/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.logical.rel;

import com.hazelcast.sql.impl.calcite.SqlCalcitePlanVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;

public class HazelcastSortRel extends Sort implements HazelcastRel {
    public HazelcastSortRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelCollation collation
    ) {
        super(cluster, traits, child, collation);
    }

    public HazelcastSortRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelCollation collation,
        RexNode offset,
        RexNode fetch
    ) {
        super(cluster, traits, child, collation, offset, fetch);
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new HazelcastSortRel(getCluster(), traitSet, input, collation, offset, fetch);
    }

    @Override
    public void visitForPlan(SqlCalcitePlanVisitor visitor) {
        ((HazelcastRel)getInput()).visitForPlan(visitor);

        visitor.visitSort(this);
    }
}
