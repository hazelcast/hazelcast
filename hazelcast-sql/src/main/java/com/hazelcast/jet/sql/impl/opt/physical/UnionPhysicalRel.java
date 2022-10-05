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

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.opt.Conventions;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Collections.nCopies;

public class UnionPhysicalRel extends Union implements PhysicalRel {

    // SELECT * FROM a
    // UNION ALL
    // SELECT * FROM b

    // +- UNION
    //   +- SCAN (a)
    //   +- SCAN (b)

    UnionPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            boolean all
    ) {
        super(cluster, traitSet, inputs, all);
    }

    @Override
    public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        return PhysicalRel.super.passThroughTraits(required);
    }

    /**
     * TODO: clarify if UNION preserves collation. My opinion - it doesn't.
     */
    @Override
    public RelTraitSet passThroughCollationTraits(RelNode rel, RelTraitSet required) {
        return required.replace(RelCollations.EMPTY);
    }

    @Override
    public @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        if (childTraits.getConvention() != Conventions.PHYSICAL) {
            return null;
        }

        RelTraitSet relTraitSet = this.getTraitSet().replace(RelCollations.EMPTY);
        List<RelTraitSet> childTraitSet = nCopies(this.getInputs().size(), childTraits);
        return Pair.of(relTraitSet, childTraitSet);
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        return ((PhysicalRel) inputs.get(0)).schema(parameterMetadata);
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onUnion(this);
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new UnionPhysicalRel(getCluster(), traitSet, inputs, all);
    }
}
