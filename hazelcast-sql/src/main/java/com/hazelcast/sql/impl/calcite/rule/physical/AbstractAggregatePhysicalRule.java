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

package com.hazelcast.sql.impl.calcite.rule.physical;

import com.hazelcast.sql.impl.calcite.rel.logical.AggregateLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base class for aggregate rules.
 */

// TODO: Check if any transpose rules are necessary
public abstract class AbstractAggregatePhysicalRule extends RelOptRule {
    protected AbstractAggregatePhysicalRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    /**
     * Get collation for local aggregate.
     *
     * @param agg Aggregate.
     * @param input Input.
     * @return Collation for the given aggregate (empty if input's collation is either empty or destroyed).
     */
    protected static AggregateCollation getLocalCollation(AggregateLogicalRel agg, RelNode input) {
        assert agg.getGroupCount() == 1 : "Grouping sets are not supported at the moment: " + agg;

        RelCollation inputCollation = input.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

        List<RelFieldCollation> inputFieldCollations = inputCollation.getFieldCollations();

        // GROUP BY columns should be a prefix of collation columns. E.g. "GROUP BY a" on top of an input sorted
        // by "a, b" is OK.

        int groupFieldCount = agg.getGroupSet().cardinality();

        if (inputFieldCollations.size() < groupFieldCount) {
            return AggregateCollation.EMPTY;
        }

        List<RelFieldCollation> aggCollationFields = new ArrayList<>(groupFieldCount);

        int idx = 0;

        for (Integer groupFieldIdx : agg.getGroupSet()) {
            RelFieldCollation inputCollationField = inputFieldCollations.get(idx);

            if (inputCollationField.getFieldIndex() == groupFieldIdx) {
                RelFieldCollation collationField = new RelFieldCollation(
                    groupFieldIdx,
                    inputCollationField.getDirection(),
                    inputCollationField.nullDirection
                );

                aggCollationFields.add(collationField);
            } else {
                // TODO: Consider adding partial prefix supoprt. In this case it is not possible to avoid blocking
                // TODO: behavior, but only part of the group key is needed for mapping. E.g. GROUP BY (a, b) on top
                // TODO: input sorted by (a) will have "fixed" group key component of "a", and "blocking" map
                // TODO: (group key -> group value) for "b" only. As a result, instead of storing the whole group
                // TODO: map in memory, we may return the next row as soon as the next value of "a" is different from
                // TODO: the current one. This descreases memory demands and partially preserves the collation.

                return AggregateCollation.EMPTY;
            }

            idx++;
        }

        return new AggregateCollation(RelCollationImpl.of(aggCollationFields), true);
    }

    /**
     * Collation which should be applied to aggregate.
     */
    protected static class AggregateCollation {

        private static final AggregateCollation EMPTY =
            new AggregateCollation(RelCollationImpl.of(Collections.emptyList()), false);

        /** Collation to be applied to aggregate. */
        private final RelCollation collation;

        /** Whether it matches input's collation */
        private final boolean matchesInput;

        public AggregateCollation(RelCollation collation, boolean matchesInput) {
            this.collation = collation;
            this.matchesInput = matchesInput;
        }

        public RelCollation getCollation() {
            return collation;
        }

        public boolean isMatchesInput() {
            return matchesInput;
        }
    }
}
