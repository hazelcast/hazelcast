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

package com.hazelcast.sql.impl.calcite.physical.rule;

import com.hazelcast.sql.impl.calcite.logical.rel.AggregateLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for aggregate rules.
 */
// TODO: Handle cases with inputs sorted on grouping key (save collation)
// TODO: Understand ROLLUP/CUBE/GROUPING stuff
// TODO: Check if any transpose rules are necessary
public abstract class AbstractAggregatePhysicalRule extends RelOptRule {
    protected AbstractAggregatePhysicalRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    /**
     * Get collation for aggregate. Currently it
     *
     * @param agg Aggregate.
     * @param input Input.
     * @return Collation for the given aggregate or {@code EMPTY} if input's collation is either empty or destroyed.
     */
    protected static AggregateCollation getCollation(AggregateLogicalRel agg, RelNode input) {
        RelCollation inputCollation = input.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

        List<RelFieldCollation> inputFieldCollations = inputCollation.getFieldCollations();

        if (inputFieldCollations.isEmpty())
            return new AggregateCollation(RelCollationImpl.EMPTY, false);

        // GROUP BY columns should be a prefix of collation columns.
        List<Integer> inputIndexes = new ArrayList<>(inputFieldCollations.size());

        for (RelFieldCollation inputFieldCollation : inputFieldCollations)
            inputIndexes.add(inputFieldCollation.getFieldIndex());

        List<RelFieldCollation> aggFieldCollations = null;

        // TODO
        return new AggregateCollation(RelCollationImpl.EMPTY, false);
    }

    /**
     * Collation which should be applied to aggregate.
     */
    protected static class AggregateCollation {
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
