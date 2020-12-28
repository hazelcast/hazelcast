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

package com.hazelcast.sql.impl.calcite.opt.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;

/**
 * Logical sorting.
 */
public class SortLogicalRel extends Sort implements LogicalRel {
    public SortLogicalRel(
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
    public final Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new SortLogicalRel(getCluster(), traitSet, input, collation, offset, fetch);
    }

    @Override
    public final RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw);
    }
}
