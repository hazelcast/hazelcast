/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.JoinLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.Collection;

import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;

public final class JoinPhysicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new JoinPhysicalRule();

    private JoinPhysicalRule() {
        super(
                operand(JoinLogicalRel.class, LOGICAL, some(operand(RelNode.class, any()), operand(RelNode.class, any()))),
                JoinPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        JoinLogicalRel logicalJoin = call.rel(0);

        JoinRelType joinType = logicalJoin.getJoinType();
        assert joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT;

        RelNode physicalLeft = OptUtils.toPhysicalInput(logicalJoin.getLeft());
        RelNode physicalRight = OptUtils.toPhysicalInput(logicalJoin.getRight());

        Collection<RelNode> lefts = OptUtils.extractPhysicalRelsFromSubset(physicalLeft);
        Collection<RelNode> rights = OptUtils.extractPhysicalRelsFromSubset(physicalRight);
        for (RelNode left : lefts) {
            for (RelNode right : rights) {
                RelNode rel = new JoinNestedLoopPhysicalRel(
                        logicalJoin.getCluster(),
                        OptUtils.toPhysicalConvention(logicalJoin.getTraitSet()),
                        left,
                        right,
                        logicalJoin.getCondition(),
                        logicalJoin.getJoinType()
                );
                call.transformTo(rel);
            }
        }
    }
}
