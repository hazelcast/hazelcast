/*
 * Copyright 2023 Hazelcast Inc.
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
import com.hazelcast.jet.sql.impl.HazelcastPhysicalScan;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.cost.Cost;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableIntList;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public class JoinNestedLoopPhysicalRel extends JoinPhysicalRel {
    private JoinInfo modifiedJoinInfo;

    JoinNestedLoopPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType
    ) {
        super(cluster, traitSet, left, right, condition, joinType);
    }

    public RexNode rightFilter() {
        return ((HazelcastPhysicalScan) getRight()).filter();
    }

    public List<RexNode> rightProjection() {
        return ((HazelcastPhysicalScan) getRight()).projection();
    }

    @Override
    public JoinInfo analyzeCondition() {
        if (modifiedJoinInfo != null) {
            return modifiedJoinInfo;
        }
        JoinInfo joinInfo = super.analyzeCondition();
        if (getRight().getTable() == null) {
            return joinInfo;
        }
        List<Integer> leftKeys = joinInfo.leftKeys.toIntegerList();
        List<Integer> rightKeys = joinInfo.rightKeys.toIntegerList();
        HazelcastTable table = OptUtils.extractHazelcastTable(getRight());
        RexBuilder rexBuilder = getCluster().getRexBuilder();

        List<RexNode> additionalNonEquiConditions = new ArrayList<>();
        for (int i = 0; i < rightKeys.size(); i++) {
            Integer rightKeyIndex = rightKeys.get(i);
            RexNode rightExpr = table.getProjects().get(rightKeyIndex);
            if (rightExpr instanceof RexInputRef) {
                rightKeys.set(i, ((RexInputRef) rightExpr).getIndex());
            } else {
                // Offset the indices in rightExp by the width of the left row
                rightExpr = rightExpr.accept(new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        return rexBuilder.makeInputRef(inputRef.getType(),
                                inputRef.getIndex() + getLeft().getRowType().getFieldCount());
                    }
                });
                additionalNonEquiConditions.add(rexBuilder.makeCall(HazelcastSqlOperatorTable.EQUALS,
                        rexBuilder.makeInputRef(getLeft(), leftKeys.get(i)),
                        rightExpr));
                leftKeys.remove(i);
                rightKeys.remove(i);
                i--;
            }
        }

        modifiedJoinInfo = new ModifiedJoinInfo(ImmutableIntList.copyOf(leftKeys),
                ImmutableIntList.copyOf(rightKeys),
                ImmutableList.<RexNode>builder()
                        .addAll(joinInfo.nonEquiConditions)
                        .addAll(additionalNonEquiConditions).build());
        return modifiedJoinInfo;
    }

    @Override
    public <V> V accept(CreateDagVisitor<V> visitor) {
        return visitor.onNestedLoopJoin(this);
    }

    /**
     * Cost calculation of Nested Loop Join relation.
     * NLJ algorithm is simple algorithm, where for each left row we are traversing right row set.
     * Speaking of cost estimation, we are accounting the next properties:
     * - both processed and produced row cou is estimated an L * R, because for each left row we probe full right row set;
     * - for CPU cost estimation, we multiply row count by cost of row comparison.
     * The perfect assumption also must include memory and IO cost estimation, and also selectivity for a right row set.
     */
    @Override
    @Nullable
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final double leftRowCount = mq.getRowCount(left);
        final double rightRowCount = mq.getRowCount(right);
        if (Double.isInfinite(leftRowCount) || Double.isInfinite(rightRowCount)) {
            return planner.getCostFactory().makeInfiniteCost();
        }

        RelOptCost rightCost = planner.getCost(getRight(), mq);
        if (rightCost == null) {
            return planner.getCostFactory().makeInfiniteCost();
        }

        // TODO: introduce selectivity estimator, but ATM we taking the worst case scenario : selectivity = 1.0.
        double rowsEstimate = leftRowCount * /* TODO: selectivity */ rightRowCount;
        double cpuEstimate = Math.max(1.0, rowsEstimate - 1) * Cost.JOIN_ROW_CMP_MULTIPLIER;

        return planner.getCostFactory().makeCost(rowsEstimate, cpuEstimate, 0);
    }

    @Override
    public Join copy(
            RelTraitSet traitSet,
            RexNode conditionExpr,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone
    ) {
        return new JoinNestedLoopPhysicalRel(getCluster(), traitSet, left, right, getCondition(), joinType);
    }

    protected static class ModifiedJoinInfo extends JoinInfo {
        protected ModifiedJoinInfo(ImmutableIntList leftKeys, ImmutableIntList rightKeys,
                                   ImmutableList<RexNode> nonEquiConditions) {
            super(leftKeys, rightKeys, nonEquiConditions);
        }
    }
}
