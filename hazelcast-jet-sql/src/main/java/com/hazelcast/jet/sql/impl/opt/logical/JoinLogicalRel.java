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

package com.hazelcast.jet.sql.impl.opt.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public class JoinLogicalRel extends Join implements LogicalRel {

    JoinLogicalRel(
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
    public final Join copy(
            RelTraitSet traitSet,
            RexNode condition,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone
    ) {
        return new JoinLogicalRel(getCluster(), traitSet, left, right, condition, joinType);
    }
}
