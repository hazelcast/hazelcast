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

package com.hazelcast.sql.impl.calcite.opt;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;

/**
 * Common class for join relations.
 */
public abstract class AbstractJoinRel extends Join {
    /** Indexes of left keys with equi-join conditions. */
    protected final List<Integer> leftKeys;

    /** Indexes of right keys with equi-join conditions. */
    protected final List<Integer> rightKeys;

    protected AbstractJoinRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        List<Integer> leftKeys,
        List<Integer> rightKeys
    ) {
        super(cluster, traitSet, ImmutableList.of(), left, right, condition, Collections.emptySet(), joinType);

        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
    }

    public List<Integer> getLeftKeys() {
        return leftKeys;
    }

    public List<Integer> getRightKeys() {
        return rightKeys;
    }

    public boolean hasEquiJoinKeys() {
        return !leftKeys.isEmpty();
    }

    // TODO: VO: Revisit this carefully when it is time to implement join. For now it is just copied from Calcite with the
    //  exception to CPU that is adjusted to row count.
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (isSemiJoin()) {
            return planner.getCostFactory().makeTinyCost();
        }

        double rowCount = mq.getRowCount(this);
        double cpu = rowCount;

        return planner.getCostFactory().makeCost(rowCount, cpu, 0);
    }
}
