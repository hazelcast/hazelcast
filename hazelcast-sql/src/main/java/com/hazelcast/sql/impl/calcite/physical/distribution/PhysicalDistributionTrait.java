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

package com.hazelcast.sql.impl.calcite.physical.distribution;

import com.hazelcast.sql.impl.calcite.HazelcastCalciteContext;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PhysicalDistributionTrait implements RelTrait {
    /** Data is distributed between nodes, but actual distribution column is unknown. */
    public static final PhysicalDistributionTrait DISTRIBUTED =
        new PhysicalDistributionTrait(PhysicalDistributionType.DISTRIBUTED);

    /** Data is distributed in replicated map. */
    public static final PhysicalDistributionTrait REPLICATED =
        new PhysicalDistributionTrait(PhysicalDistributionType.REPLICATED);

    /** Consume the whole stream on a single node. */
    public static final PhysicalDistributionTrait SINGLETON =
        new PhysicalDistributionTrait(PhysicalDistributionType.SINGLETON);

    /** Distribution without any restriction. */
    public static final PhysicalDistributionTrait ANY = new PhysicalDistributionTrait(PhysicalDistributionType.ANY);

    /** Distribution type. */
    private final PhysicalDistributionType type;

    /** Distribution fields. */
    // TODO: Maintain a set of equivalent fields instead of a single list: Set<List<>> instead of List<>.
    // TODO: Motivation is explained in "CollocatedJoinPhysicalRule" TODOs.
    private final List<PhysicalDistributionField> fields;

    private PhysicalDistributionTrait(PhysicalDistributionType type) {
        this(type, null);
    }

    public PhysicalDistributionTrait(PhysicalDistributionType type, List<PhysicalDistributionField> fields) {
        this.type = type;
        this.fields = fields != null ? new ArrayList<>(fields) : Collections.emptyList();
    }

    public PhysicalDistributionType getType() {
        return type;
    }

    public List<PhysicalDistributionField> getFields() {
        return fields;
    }

    @Override
    public RelTraitDef getTraitDef() {
        return PhysicalDistributionTraitDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait targetTrait) {
        if (targetTrait instanceof PhysicalDistributionTrait) {
            PhysicalDistributionType targetType = ((PhysicalDistributionTrait) targetTrait).getType();

            // Any type satisfies ANY.
            if (targetType == PhysicalDistributionType.ANY) {
                return true;
            } else {
                // Any distributed mode satisfies DISTRIBUTED, as it is an arbitrary distribution.
                switch (type) {
                    case DISTRIBUTED:
                    case DISTRIBUTED_PARTITIONED:
                        if (targetType == PhysicalDistributionType.DISTRIBUTED) {
                            return true;
                        }

                        break;

                    case REPLICATED:
                        if (targetType == PhysicalDistributionType.SINGLETON
                            && HazelcastCalciteContext.get().isDataMember()) {
                            return true;
                        }

                        break;

                    default:
                        break;
                }
            }

            return this.equals(targetTrait);
        }

        return false;
    }

    /**
     * @return {@code True} if input is distributed, i.e. it's tuples are spread between multiple nodes, and there is
     * only one instance of each tuple.
     */
    public boolean isDistributed() {
        return type == PhysicalDistributionType.DISTRIBUTED || type == PhysicalDistributionType.DISTRIBUTED_PARTITIONED;
    }

    /**
     * Check if input of the distribution is complete, i.e. the whole set of tuples is available locally. This holds
     * for SINGLETON distribution (follow from it's definition) and for REPLICATED distribution (all data members
     * has the whole result set).
     *
     * @return {@code True} if distribution is complete, {@code false} otherwise.
     */
    public boolean isComplete() {
        return type == PhysicalDistributionType.SINGLETON || type == PhysicalDistributionType.REPLICATED;
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

        PhysicalDistributionTrait other = (PhysicalDistributionTrait) o;

        return type == other.type && fields.equals(other.fields);
    }

    @Override
    public int hashCode() {
        return 31 * type.hashCode() + fields.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder(type.name());

        if (!fields.isEmpty()) {
            res.append("{");

            for (int i = 0; i < fields.size(); i++) {
                if (i != 0) {
                    res.append(", ");
                }

                PhysicalDistributionField field = fields.get(i);

                res.append("$").append(field.getIndex());

                if (field.getNestedField() != null) {
                    res.append(".").append(field.getNestedField());
                }
            }

            res.append("}");
        }

        return res.toString();
    }

    public static PhysicalDistributionTrait distributedPartitioned(List<PhysicalDistributionField> fields) {
        return new PhysicalDistributionTrait(PhysicalDistributionType.DISTRIBUTED_PARTITIONED, fields);
    }
}
