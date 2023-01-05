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

package com.hazelcast.jet.sql.impl.opt.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

public class CorrelateLogicalRel extends JoinLogicalRel implements LogicalRel {

    private final CorrelationId correlationId;

    CorrelateLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            CorrelationId correlationId,
            RelNode right,
            JoinRelType joinType
    ) {
        super(cluster, traitSet, left, right,
                // TODO: should Correlate be subclass of Join/JoinLogicalRel? It is not in Calcite.
                // Correlate does not have condition on Join level.
                // We have some validations for JoinLogicalRel that should apply to CorrelateLogicalRel
                // but they match only exact class not the subclass.
                cluster.getRexBuilder().makeLiteral(true),
                joinType);
        this.correlationId = correlationId;
    }

    public CorrelationId getCorrelationId() {
        return correlationId;
    }

    @Override
    public final Join copy(
            RelTraitSet traitSet,
            RexNode condition,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone
    ) {
        return new CorrelateLogicalRel(getCluster(), traitSet, left, correlationId, right, joinType);
    }
}
