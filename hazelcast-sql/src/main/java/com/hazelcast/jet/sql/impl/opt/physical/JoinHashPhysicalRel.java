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

import com.hazelcast.jet.sql.impl.opt.cost.Cost;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

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

    @Override
    public <V> V accept(CreateDagVisitor<V> visitor) {
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

    /**
     * Cost calculation of Hash Join relation. It does not rely on children cost.
     * <p>
     * Hash Join algorithm is more advanced join algorithm, where it builds a hash table for left row set, and then
     * compare each row from the right side
     * Speaking of cost estimation, we are accounting the next properties:
     * - produced row count is estimated as L * R, with assumption that the JOIN predicate has maximum selectivity.
     * - processed row count is estimated as L + R, because we traverse both sides once per JOIN.
     * - CPU cost estimation is a processed row multiplied by cost to build a hash table, and left and right rows comparison.
     * <p>
     * The perfect assumption also must include memory (what is important in case of hash table) and IO cost estimation,
     * as well as a selectivity for a right row set.
     */
    @Override
    @Nullable
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double leftRowCount = mq.getRowCount(getLeft());
        double rightRowCount = mq.getRowCount(getRight());
        // TODO: introduce selectivity estimator, but ATM we taking the worst case scenario : selectivity = 1.0.
        double producedRowCount = leftRowCount * /* TODO: selectivity */ rightRowCount;
        double processedRowCount = leftRowCount + /* TODO: selectivity */ rightRowCount;

        double cpu = Cost.HASH_JOIN_MULTIPLIER * processedRowCount * Cost.JOIN_ROW_CMP_MULTIPLIER;

        return planner.getCostFactory().makeCost(producedRowCount, cpu, 0.);
    }
}
