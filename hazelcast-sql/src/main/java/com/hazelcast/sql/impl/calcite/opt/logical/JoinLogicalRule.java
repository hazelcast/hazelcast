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

package com.hazelcast.sql.impl.calcite.opt.logical;

import com.google.common.collect.Lists;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
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

import java.util.ArrayList;
import java.util.List;

/**
 * Convert abstract join to logical join. Optional filter pullout is performed for equi-joins.
 */

public final class JoinLogicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new JoinLogicalRule();

    private JoinLogicalRule() {
        super(
            OptUtils.single(LogicalJoin.class, Convention.NONE),
            RelFactories.LOGICAL_BUILDER,
            JoinLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);

        RelNode leftInput = OptUtils.toLogicalInput(join.getLeft());
        RelNode rightInput = OptUtils.toLogicalInput(join.getRight());

        List<Integer> leftKeys = new ArrayList<>(1);
        List<Integer> rightKeys = new ArrayList<>(1);
        List<Boolean> filterNulls = new ArrayList<>(1);

        // This operations tries to extract conjunctive equi-join component from the join condition. This component
        // is split into above defined collections: column indexes of the left input, column indexes of the right
        // input, and equi-join type (equality or IS NOT DISTINCT FROM). Returned condition is a remainder of
        // original condition which is not a part of equi-join. Examples:
        // 1) "A.a = B.b" is converted to {index(a in A)}, {index(b in B)}, {true} and returned remainder is "true".
        // 2) "A.a1 = B.b1 AND A.a2 = B.b2" => {index(a1 in A), index(a2 in A)}, {index(b1 in B), index(b2 in B)},
        // {true, true}, remainder is "true"
        // 3) "A.a1 = B.b AND A.a2 > 5" => {index(a1 in A)}, {index(b in B)}, {true}, remainder is "A.a2 > 5"
        // 4) "A.a1 = B.b OR A.a2 > 5" => {}, {}, {}, remainder is "A.a1 = B.b OR A.a2 > 5", because this is a
        // disjunction
        // Fundamental significance of this split operation is that equi-join component could be used to determine
        // the collocation of two inputs. If no equi-join is extracted, then no collocation is possible unless
        // one of the inputs is ReplicatedMap and it is not on the "right outer" side.
        RexNode filterCondition = RelOptUtil.splitJoinCondition(
            leftInput,
            rightInput,
            join.getCondition(),
            leftKeys,
            rightKeys,
            filterNulls
        );

        // TODO: Convert LEFT/RIGHT OUTER JOIN to INNER JOIN in case corresponding side doesn't allow nulls.
        // TODO: E.g. "A LEFT OUTER JOIN B on A.a = B.b1 WHERE B.b2 = ?" is equivalent to
        // TODO: "A INNER JOIN B on A.a = B.b1 WHERE B.b2 = ?" because if a row from A doesn't have a matching row in B,
        // TODO: then resulting tuple will have all attributes of B set to NULL and "B.b2 = ?" will filter out this
        // TODO: tuple anyway.

        RelNode transform;

        if (join.getJoinType() == JoinRelType.INNER && !leftKeys.isEmpty()) {
            assert leftKeys.size() == rightKeys.size();

            // Perform filter pullout for equi-join. This separated filter could be used for further optimizations,
            // such as filter push-down to scan.
            RexNode joinCondition = createEquiJoinCondition(
                leftInput,
                rightInput,
                leftKeys,
                rightKeys,
                filterNulls
            );

            transform = new JoinLogicalRel(
                join.getCluster(),
                OptUtils.toLogicalConvention(join.getTraitSet()),
                leftInput,
                rightInput,
                joinCondition,
                join.getJoinType(),
                leftKeys,
                rightKeys
            );

            // Split operation returns "true" as a remainder if the whole condition is an equi-join, e.g. "A.a = B.b".
            // Filter is not needed in this case.
            if (!filterCondition.isAlwaysTrue()) {
                transform = new FilterLogicalRel(
                    join.getCluster(),
                    OptUtils.toLogicalConvention(join.getTraitSet()),
                    transform,
                    filterCondition
                );
            }
        } else {
            // If we failed to extract an equi-join component or join type is not INNER JOIN, then continue with
            // pessimistic approach.
            transform = new JoinLogicalRel(
                join.getCluster(),
                OptUtils.toLogicalConvention(join.getTraitSet()),
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

        for (int i = 0; i < leftKeys.size(); i++) {
            int leftKeyIndex = leftKeys.get(i);
            int rightKeyIndex = rightKeys.get(i);

            SqlOperator operator = filterNulls.get(i)
                ? SqlStdOperatorTable.EQUALS : SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;

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
