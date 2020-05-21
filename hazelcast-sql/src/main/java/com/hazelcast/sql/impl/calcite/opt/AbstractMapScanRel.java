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
import com.hazelcast.sql.impl.schema.map.AbstractMapTable;
import com.hazelcast.sql.impl.schema.map.ReplicatedMapTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Base class for map scans.
 */
public abstract class AbstractMapScanRel extends AbstractScanRel implements HazelcastRelNode {
    /** Filter. */
    protected final RexNode filter;

    public AbstractMapScanRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        List<Integer> projects,
        RexNode filter
    ) {
        super(cluster, traitSet, table, projects);

        this.filter = filter;
    }

    public List<Integer> getProjects() {
        return projects != null ? projects : identity();
    }

    public RexNode getFilter() {
        return filter;
    }

    public AbstractMapTable getMap() {
        return getTableUnwrapped().getTarget();
    }

    public boolean isReplicated() {
        return getMap() instanceof ReplicatedMapTable;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("filter", filter, filter != null);
    }

    @Override
    public final double estimateRowCount(RelMetadataQuery mq) {
        double rowCount = super.estimateRowCount(mq);

        if (filter != null) {
            double selectivity = mq.getSelectivity(this, filter);

            rowCount = rowCount * selectivity;
        }

        return rowCount;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // 1. Get cost of the scan itself. For replicated map cost is multiplied by the number of nodes.
        RelOptCost scanCost = super.computeSelfCost(planner, mq);

        if (isReplicated()) {
            scanCost = scanCost.multiplyBy(getMemberCount());
        }

        // 2. Get cost of the project taking in count filter and number of expressions. Project never produces IO.
        double filterRowCount = scanCost.getRows();

        if (filter != null) {
            double filterSelectivity = mq.getSelectivity(this, filter);

            filterRowCount = filterRowCount * filterSelectivity;
        }

        int expressionCount = getProjects().size();

        double projectCpu = CostUtils.adjustProjectCpu(filterRowCount * expressionCount, true);

        // 3. Finally, return sum of both scan and project.
        RelOptCost totalCost = planner.getCostFactory().makeCost(
            filterRowCount,
            scanCost.getCpu() + projectCpu,
            scanCost.getIo()
        );

        return totalCost;
    }
}
