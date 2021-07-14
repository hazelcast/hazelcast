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

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PhysicalRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;

/**
 * Physical sort performed locally. Throws unsupported exception at runtime if actual sorting
 * is required.
 */
public class SortPhysicalRel extends Sort implements PhysicalRel {

    // Whether the input is actually requires sorting
    private final boolean requiresSort;

    public SortPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RelCollation collation,
            boolean requiresSort,
            RexNode offset,
            RexNode fetch
    ) {
        super(cluster, traits, child, collation, offset, fetch);
        this.requiresSort = requiresSort;
    }

    public boolean requiresSort() {
        return requiresSort;
    }

    @Override
    public final Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new SortPhysicalRel(getCluster(), traitSet, input, collation, requiresSort, offset, fetch);
    }

    @Override
    public final RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("requiresSort", requiresSort);
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        ((PhysicalRel) input).visit(visitor);

        visitor.onSort(this);
    }
}
