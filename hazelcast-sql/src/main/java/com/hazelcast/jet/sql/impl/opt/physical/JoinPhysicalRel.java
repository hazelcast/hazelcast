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

import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public abstract class JoinPhysicalRel extends Join implements PhysicalRel {

    public JoinPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType
    ) {
        super(cluster, traitSet, emptyList(), left, right, condition, emptySet(), joinType);
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        PlanNodeSchema leftSchema = ((PhysicalRel) getLeft()).schema(parameterMetadata);
        PlanNodeSchema rightSchema = ((PhysicalRel) getRight()).schema(parameterMetadata);
        return PlanNodeSchema.combine(leftSchema, rightSchema);
    }

    public JetJoinInfo joinInfo(QueryParameterMetadata parameterMetadata) {
        JoinInfo joinInfo = analyzeCondition();
        int[] leftKeys = joinInfo.leftKeys.toIntArray();
        int[] rightKeys = joinInfo.rightKeys.toIntArray();
        RexNode predicate = RexUtil.composeConjunction(getCluster().getRexBuilder(), joinInfo.nonEquiConditions);
        Expression<Boolean> nonEquiCondition = filter(schema(parameterMetadata), predicate, parameterMetadata);
        Expression<Boolean> condition = filter(schema(parameterMetadata), getCondition(), parameterMetadata);
        return new JetJoinInfo(getJoinType(), leftKeys, rightKeys, nonEquiCondition, condition);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("conditionType",
                !analyzeCondition().rightKeys.isEmpty() ? "equiJoin" : "nonEquiJoin");
    }
}
