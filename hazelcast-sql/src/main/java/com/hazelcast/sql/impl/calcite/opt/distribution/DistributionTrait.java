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

package com.hazelcast.sql.impl.calcite.opt.distribution;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

import static com.hazelcast.sql.impl.calcite.opt.distribution.DistributionType.ANY;

/**
 * Defines how the given relation is distributed in the cluster.
 */
public class DistributionTrait implements RelTrait {
    /** Trait definition. */
    private final DistributionTraitDef traitDef;

    /** Distribution type. */
    private final DistributionType type;

    DistributionTrait(DistributionTraitDef traitDef, DistributionType type) {
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

        DistributionType targetType = ((DistributionTrait) targetTrait).getType();

        // Any type satisfies ANY.
        if (targetType == ANY) {
            return true;
        }

        // Otherwise compare two distributions.
        return this.equals(targetTrait);
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
