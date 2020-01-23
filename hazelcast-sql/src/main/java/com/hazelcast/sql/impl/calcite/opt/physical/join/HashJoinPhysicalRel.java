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
 * Hash join.
 */
public class HashJoinPhysicalRel extends AbstractJoinPhysicalRel implements PhysicalRel {
    /** Left hash keys.. */
    private final List<Integer> leftHashKeys;

    /** Right hash keys. */
    private final List<Integer> rightHashKeys;

    public HashJoinPhysicalRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        List<Integer> leftKeys,
        List<Integer> rightKeys,
        List<Integer> leftHashKeys,
        List<Integer> rightHashKeys
    ) {
        super(cluster, traitSet, left, right, condition, joinType, leftKeys, rightKeys);

        this.leftHashKeys = leftHashKeys;
        this.rightHashKeys = rightHashKeys;
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
        return new HashJoinPhysicalRel(
            getCluster(),
            traitSet,
            left,
            right,
            condition,
            joinType,
            leftKeys,
            rightKeys,
            leftHashKeys,
            rightHashKeys
        );
    }

    public List<Integer> getLeftHashKeys() {
        return leftHashKeys;
    }

    public List<Integer> getRightHashKeys() {
        return rightHashKeys;
    }

    @Override
    public final RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);

        return pw.item("leftHashKeys", leftHashKeys).item("rightHashKeys", rightHashKeys);
    }

    @Override
    protected void visitAfterInputs(PhysicalRelVisitor visitor) {
        visitor.onHashJoin(this);
    }
}
