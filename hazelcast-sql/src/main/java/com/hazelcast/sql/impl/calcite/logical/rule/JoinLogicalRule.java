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

package com.hazelcast.sql.impl.calcite.logical.rule;

import com.google.common.collect.Lists;
import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.logical.rel.FilterLogicalRel;
import com.hazelcast.sql.impl.calcite.logical.rel.JoinLogicalRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.List;

/**
 * Convert abstract join to logical join. Optional filter pullout is performed for equi-joins.
 */
// TODO: Create or reuse a rule to move a filter past join (investigate Calcite's built-in rules).
public class JoinLogicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new JoinLogicalRule();

    private JoinLogicalRule() {
        super(
            RuleUtils.single(LogicalJoin.class, Convention.NONE),
            RelFactories.LOGICAL_BUILDER,
            JoinLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);

        RelNode leftInput = RuleUtils.toLogicalInput(join.getLeft());
        RelNode rightInput = RuleUtils.toLogicalInput(join.getRight());

        List<Integer> leftKeys = Lists.newArrayList();
        List<Integer> rightKeys = Lists.newArrayList();
        List<Boolean> filterNulls = Lists.newArrayList();

        RexNode filterCondition = RelOptUtil.splitJoinCondition(
            leftInput,
            rightInput,
            join.getCondition(),
            leftKeys,
            rightKeys,
            filterNulls
        );

        RelNode transform;

        if (join.getJoinType()== JoinRelType.INNER && leftKeys.size() > 0 && leftKeys.size() == rightKeys.size()) {
            // Perform filter pullout for equi-join.
            RexNode joinCondition = createEquiJoinCondition(
                leftInput,
                rightInput,
                leftKeys,
                rightKeys,
                filterNulls
            );

            RelNode newJoin = new JoinLogicalRel(
                join.getCluster(),
                RuleUtils.toLogicalConvention(join.getTraitSet()),
                leftInput,
                rightInput,
                joinCondition,
                join.getJoinType(),
                leftKeys,
                rightKeys
            );

            transform = new FilterLogicalRel(
                join.getCluster(),
                RuleUtils.toLogicalConvention(join.getTraitSet()),
                newJoin,
                filterCondition
            );
        }
        else {
            // Otherwise leave original condition as is.
            transform = new JoinLogicalRel(
                join.getCluster(),
                RuleUtils.toLogicalConvention(join.getTraitSet()),
                leftInput,
                rightInput,
                join.getCondition(),
                join.getJoinType(),
                leftKeys,
                rightKeys
            );
        }

        call.transformTo(transform);
    }

    private RexNode createEquiJoinCondition(
        RelNode leftInput,
        RelNode rightInput,
        List<Integer> leftKeys,
        List<Integer> rightKeys,
        List<Boolean> filterNulls
    ) {
        assert leftKeys.size() == rightKeys.size();

        RexBuilder builder = leftInput.getCluster().getRexBuilder();

        List<RexNode> conditions = Lists.newArrayList();

        List<RelDataTypeField> leftFields = leftInput.getRowType().getFieldList();
        List<RelDataTypeField> rightFields = rightInput.getRowType().getFieldList();

        for (int i=0; i < leftKeys.size(); i++) {
            int leftKeyIndex = leftKeys.get(i);
            int rightKeyIndex = rightKeys.get(i);

            SqlOperator operator = filterNulls.get(i) ?
                SqlStdOperatorTable.EQUALS : SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;

            RexNode leftOperand = builder.makeInputRef(
                leftFields.get(leftKeyIndex).getType(),
                leftKeyIndex
            );

            RexNode rightOperand = builder.makeInputRef(
                rightFields.get(rightKeyIndex).getType(),
                rightKeyIndex + leftFields.size()
            );

            RexNode condition = builder.makeCall(operator, leftOperand, rightOperand);

            conditions.add(condition);
        }

        return RexUtil.composeConjunction(builder, conditions, false);
    }
}
