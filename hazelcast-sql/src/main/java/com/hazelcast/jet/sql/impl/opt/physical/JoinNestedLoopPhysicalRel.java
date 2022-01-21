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
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public class JoinNestedLoopPhysicalRel extends Join implements PhysicalRel {

    JoinNestedLoopPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType
    ) {
        super(cluster, traitSet, emptyList(), left, right, condition, emptySet(), joinType);
    }

    public Expression<Boolean> rightFilter(QueryParameterMetadata parameterMetadata) {
        return ((FullScanPhysicalRel) getRight()).filter(parameterMetadata);
    }

    public List<Expression<?>> rightProjection(QueryParameterMetadata parameterMetadata) {
        return ((FullScanPhysicalRel) getRight()).projection(parameterMetadata);
    }

    public JetJoinInfo joinInfo(QueryParameterMetadata parameterMetadata) {
        int[] leftKeys = analyzeCondition().leftKeys.toIntArray();
        int[] rightKeys = getKeysFromRightScan();

        Expression<Boolean> nonEquiCondition = filter(
                schema(parameterMetadata),
                analyzeCondition().getRemaining(getCluster().getRexBuilder()),
                parameterMetadata
        );

        Expression<Boolean> condition = filter(schema(parameterMetadata), getCondition(), parameterMetadata);

        return new JetJoinInfo(getJoinType(), leftKeys, rightKeys, nonEquiCondition, condition);
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        PlanNodeSchema leftSchema = ((PhysicalRel) getLeft()).schema(parameterMetadata);
        PlanNodeSchema rightSchema = ((PhysicalRel) getRight()).schema(parameterMetadata);
        return PlanNodeSchema.combine(leftSchema, rightSchema);
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

    private int[] getKeysFromRightScan() {
        HazelcastTable table = getRight().getTable().unwrap(HazelcastTable.class);
        List<Integer> projects = table.getProjects();
        int[] rightKeys = Arrays.stream(analyzeCondition().rightKeys.toIntArray()).map(projects::get).toArray();
        return rightKeys;
    }
}
