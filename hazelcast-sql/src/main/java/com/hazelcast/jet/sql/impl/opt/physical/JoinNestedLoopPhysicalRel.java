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
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.CollectionUtil.toIntArray;
import static java.util.Arrays.asList;

public class JoinNestedLoopPhysicalRel extends JoinPhysicalRel {

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

    public Expression<Boolean> rightFilter(QueryParameterMetadata parameterMetadata) {
        return ((FullScanPhysicalRel) getRight()).filter(parameterMetadata);
    }

    public List<Expression<?>> rightProjection(QueryParameterMetadata parameterMetadata) {
        return ((FullScanPhysicalRel) getRight()).projection(parameterMetadata);
    }

    public JetJoinInfo joinInfo(QueryParameterMetadata parameterMetadata) {
        List<Integer> leftKeys = analyzeCondition().leftKeys.toIntegerList();
        List<Integer> rightKeys = analyzeCondition().rightKeys.toIntegerList();
        HazelcastTable table = OptUtils.extractHazelcastTable(getRight());
        RexBuilder rexBuilder = getCluster().getRexBuilder();

        List<RexNode> additionalNonEquiConditions = new ArrayList<>();
        for (int i = 0; i < rightKeys.size(); i++) {
            Integer rightKeyIndex = rightKeys.get(i);
            RexNode rightExpr = table.getProjects().get(rightKeyIndex);
            if (rightExpr instanceof RexInputRef) {
                rightKeys.set(i, ((RexInputRef) rightExpr).getIndex());
            } else {
                // offset the indices in rightExp by the width of left row
                rightExpr = rightExpr.accept(new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        return rexBuilder.makeInputRef(
                                inputRef.getType(),
                                inputRef.getIndex() + getLeft().getRowType().getFieldCount()
                        );
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

        Expression<Boolean> nonEquiCondition = filter(
                schema(parameterMetadata),
                RexUtil.composeConjunction(rexBuilder, asList(
                        analyzeCondition().getRemaining(rexBuilder),
                        RexUtil.composeConjunction(rexBuilder, additionalNonEquiConditions))),
                parameterMetadata);

        Expression<Boolean> condition = filter(schema(parameterMetadata), getCondition(), parameterMetadata);

        return new JetJoinInfo(getJoinType(), toIntArray(leftKeys), toIntArray(rightKeys), nonEquiCondition, condition);
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onNestedLoopJoin(this);
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
}
