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

import com.hazelcast.sql.impl.calcite.opt.cost.CostUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

/**
 * Base class for filters.
 */
public abstract class AbstractFilterRel extends Filter implements HazelcastRelNode {
    public AbstractFilterRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        RexNode condition
    ) {
        super(cluster, traits, input, condition);
    }

    @Override
    public final RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw);
    }

    @Override
    public final RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double inputRows = mq.getRowCount(getInput());

        double rows = CostUtils.adjustFilteredRowCount(inputRows, mq.getSelectivity(this, condition));
        double cpu = inputRows;

        return planner.getCostFactory().makeCost(rows, cpu, 0);
    }
}
