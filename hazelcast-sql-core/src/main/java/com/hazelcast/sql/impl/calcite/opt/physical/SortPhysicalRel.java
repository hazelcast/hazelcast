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

    // Whether the input is already pre-sorted
    private boolean preSortedInput;

    public SortPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RelCollation collation,
            boolean preSortedInput
    ) {
        super(cluster, traits, child, collation, null, null);
        this.preSortedInput = preSortedInput;
    }

    @Override
    public final Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new SortPhysicalRel(getCluster(), traitSet, input, collation, preSortedInput);
    }

    @Override
    public final RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("preSortedInput", preSortedInput);
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        ((PhysicalRel) input).visit(visitor);

        if (!preSortedInput) {
            visitor.onSort(this);
        }
    }
}
