/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.physical.rel.join;

import com.hazelcast.sql.impl.calcite.common.AbstractJoinRel;
import com.hazelcast.sql.impl.calcite.physical.rel.PhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.PhysicalRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Common class for all physical joins.
 */
public abstract class AbstractJoinPhysicalRel extends AbstractJoinRel implements PhysicalRel {
    public AbstractJoinPhysicalRel(
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
    public void visit(PhysicalRelVisitor visitor) {
        // Visit inputs in right-left order, so that they are retrieved later from the stack in left-right order.
        ((PhysicalRel) right).visit(visitor);
        ((PhysicalRel) left).visit(visitor);

        visitAfterInputs(visitor);
    }

    /**
     * Visit the join after the inputs are processed.
     *
     * @param visitor Visitor.
     */
    protected abstract void visitAfterInputs(PhysicalRelVisitor visitor);
}
