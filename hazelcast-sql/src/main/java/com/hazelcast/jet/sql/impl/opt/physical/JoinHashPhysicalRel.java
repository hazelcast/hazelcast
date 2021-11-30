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

import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

public class JoinHashPhysicalRel extends JoinPhysicalRel {

    JoinHashPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType
    ) {
        super(cluster, traitSet, left, right, condition, joinType);
    }

    public JetJoinInfo joinInfo(QueryParameterMetadata parameterMetadata) {
        int[] leftKeys = analyzeCondition().leftKeys.toIntArray();
        int[] rightKeys = analyzeCondition().rightKeys.toIntArray();

        Expression<Boolean> nonEquiCondition = filter(
                schema(parameterMetadata),
                analyzeCondition().getRemaining(getCluster().getRexBuilder()),
                parameterMetadata
        );

        Expression<Boolean> condition = filter(schema(parameterMetadata), getCondition(), parameterMetadata);

        return new JetJoinInfo(getJoinType(), leftKeys, rightKeys, nonEquiCondition, condition);
    }

//    @Override
//    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
//        double rowCount = mq.getRowCount(this);
//
//        // Joins can be flipped, and for many algorithms, both versions are viable
//        // and have the same cost. To make the results stable between versions of
//        // the planner, make one of the versions slightly more expensive.
//        switch (joinType) {
//            case SEMI:
//            case ANTI:
//                // SEMI and ANTI join cannot be flipped
//                break;
//            case RIGHT:
//                rowCount = RelMdUtil.addEpsilon(rowCount);
//                break;
//            default:
//                if (RelNodes.COMPARATOR.compare(left, right) > 0) {
//                    rowCount = RelMdUtil.addEpsilon(rowCount);
//                }
//        }
//
//        // Cheaper if the smaller number of rows is coming from the LHS.
//        // Model this by adding L log L to the cost.
//        final double rightRowCount = right.estimateRowCount(mq);
//        final double leftRowCount = left.estimateRowCount(mq);
//        if (Double.isInfinite(leftRowCount)) {
//            rowCount = leftRowCount;
//        } else {
//            rowCount += Util.nLogN(leftRowCount);
//        }
//        if (Double.isInfinite(rightRowCount)) {
//            rowCount = rightRowCount;
//        } else {
//            rowCount += rightRowCount;
//        }
//        if (isSemiJoin()) {
//            return planner.getCostFactory().makeCost(rowCount, 0, 0).multiplyBy(.01d);
//        } else {
//            return planner.getCostFactory().makeCost(rowCount, 0, 0);
//        }
//    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onHashJoin(this);
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
        return new JoinHashPhysicalRel(getCluster(), traitSet, left, right, conditionExpr, joinType);
    }
}
