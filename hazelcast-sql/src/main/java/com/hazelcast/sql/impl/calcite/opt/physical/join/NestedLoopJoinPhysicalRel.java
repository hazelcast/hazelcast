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

package com.hazelcast.sql.impl.calcite.opt.physical.join;

import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PhysicalRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Nested loop join.
 */
public class NestedLoopJoinPhysicalRel extends AbstractJoinPhysicalRel implements PhysicalRel {
    public NestedLoopJoinPhysicalRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        List<Integer> leftKeys,
        List<Integer> rightKeys
    ) {
        super(cluster, traitSet, left, right, condition, joinType, leftKeys, rightKeys);
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
        return new NestedLoopJoinPhysicalRel(
            getCluster(),
            traitSet,
            left,
            right,
            condition,
            joinType,
            leftKeys,
            rightKeys
        );
    }

    @Override
    protected void visitAfterInputs(PhysicalRelVisitor visitor) {
        visitor.onNestedLoopJoin(this);
    }

    @Override
    public final RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw);
    }
}
