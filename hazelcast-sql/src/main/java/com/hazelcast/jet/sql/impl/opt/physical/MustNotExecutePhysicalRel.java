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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A RelNode that is used to replace another RelNode, if a rule determines that
 * the rel it matched cannot be executed without some transformation. It is used
 * to avoid throwing directly from the rule, but unlike {@link ShouldNotExecuteRel},
 * it should interrupt execution unconditionally.
 * That's why this rel has zero cost.
 */
public class MustNotExecutePhysicalRel extends ShouldNotExecuteRel {
    public MustNotExecutePhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelDataType type,
            String exceptionMessage) {
        super(cluster, traitSet, type, exceptionMessage);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }
}
