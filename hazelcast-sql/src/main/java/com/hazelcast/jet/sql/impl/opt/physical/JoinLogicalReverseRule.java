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
import com.hazelcast.jet.sql.impl.opt.logical.CorrelateLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.JoinLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.JoinLogicalRule;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

/**
 * Converts {@link JoinLogicalRel} back to {@link LogicalJoin} - reverts action
 * of {@link JoinLogicalRule}. Allows to reuse Calcite nested loops ({@link
 * org.apache.calcite.rel.logical.LogicalCorrelate}) support during physical
 * optimization phase.
 * <p>
 * Nested loops planning is a special case that does not fit nicely in
 * logical/physical optimization phases. Unlike ordinary join (hash or merge),
 * nested loops join has join condition/correlation predicate pushed to the
 * right side of the operator. It also requires correlation variables support
 * during execution.
 * <p>
 * We optimize nested loops joins roughly as follows during physical phase:
 * <ol>
 *     <li>Convert "our" logical join to Calcite join</li>
 *     <li>Convert Calcite join to nested loops by
 *         {@link org.apache.calcite.rel.rules.CoreRules#JOIN_TO_CORRELATE}</li>
 *     <li>Optimize predicates, filters, etc using Calcite and our logical
 *         transformations. Some transformations from logical phase are reused</li>
 *     <li>Convert nested loop to physical operation ({@link CorrelatePhysicalRule}</li>
 * </ol>
 * This should give both nested loops and hash join plans where applicable.
 */
final class JoinLogicalReverseRule extends ConverterRule {

    static final RelOptRule INSTANCE = new JoinLogicalReverseRule();

    private JoinLogicalReverseRule() {
        super(
                JoinLogicalRel.class, LOGICAL, Convention.NONE,
                JoinLogicalReverseRule.class.getSimpleName()
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        JoinLogicalRel join = (JoinLogicalRel) rel;

        if (join instanceof CorrelateLogicalRel) {
            return null;
        }

        return new LogicalJoin(
                join.getCluster(),
                join.getTraitSet().replace(Convention.NONE),
                join.getHints(),
                join.getLeft(),
                join.getRight(),
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType(),
                join.isSemiJoinDone(),
                ImmutableList.copyOf(join.getSystemFieldList())
        );
    }
}
