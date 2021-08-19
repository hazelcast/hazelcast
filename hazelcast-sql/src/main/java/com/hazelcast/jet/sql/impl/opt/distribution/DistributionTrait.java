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

package com.hazelcast.jet.sql.impl.opt.distribution;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

import static com.hazelcast.jet.sql.impl.opt.distribution.DistributionType.ANY;
import static com.hazelcast.jet.sql.impl.opt.distribution.DistributionType.REPLICATED;
import static com.hazelcast.jet.sql.impl.opt.distribution.DistributionType.ROOT;

/**
 * Defines how the given relation is distributed in the cluster.
 */
public class DistributionTrait implements RelTrait {
    /** Trait definition. */
    private final DistributionTraitDef traitDef;

    /** Distribution type. */
    private final DistributionType type;

    public DistributionTrait(DistributionTraitDef traitDef, DistributionType type) {
        this.traitDef = traitDef;
        this.type = type;
    }

    public DistributionType getType() {
        return type;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public RelTraitDef getTraitDef() {
        return traitDef;
    }

    @Override
    public boolean satisfies(RelTrait targetTrait) {
        if (!(targetTrait instanceof DistributionTrait)) {
            return false;
        }

        // For single-member deployments all distributions satisfy each other.
        if (traitDef.getMemberCount() == 1) {
            return true;
        }

        DistributionTrait targetTrait0 = (DistributionTrait) targetTrait;

        // Any type satisfies ANY.
        if (targetTrait0.getType() == ANY) {
            return true;
        }

        // Converting from REPLICATED to ROOT is always OK.
        if (type == REPLICATED && targetTrait0.getType() == ROOT) {
            return true;
        }

        // Otherwise compare two distributions.
        return this.equals(targetTrait);
    }

    /**
     * Checks whether the result set of the node having this trait is guaranteed to exist on all members that will execute a
     * fragment with this node.
     *
     * @return {@code true} if the full result set exists on all participants of the fragment hosting this node.
     */
    public boolean isFullResultSetOnAllParticipants() {
        if (traitDef.getMemberCount() == 1) {
            // If the plan is created for a single member, then the condition is true by definition.
            return true;
        }

        if (type == ROOT) {
            // Root fragment is always executed on a single member.
            return true;
        }

        if (type == REPLICATED) {
            // Replicated distribution assumes that the whole result set is available on all members.
            return true;
        }

        return false;
    }

    @Override
    public void register(RelOptPlanner planner) {
        // No-op.
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DistributionTrait other = (DistributionTrait) o;

        return traitDef.equals(other.traitDef) && type == other.type;
    }

    @Override
    public int hashCode() {
        return 31 * traitDef.hashCode() + type.hashCode();
    }

    @Override
    public String toString() {
        return type.name();
    }
}
