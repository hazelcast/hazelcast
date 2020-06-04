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
public abstract class AbstractMapScanRel extends AbstractScanRel {
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

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).itemIf("filter", filter, filter != null);
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
        return computeSelfCost(
            planner,
            mq,
            table.getRowCount(),
            filter,
            getProjects().size(),
            CostUtils.TABLE_SCAN_CPU_MULTIPLIER
        );
    }

    protected RelOptCost computeSelfCost(
        RelOptPlanner planner,
        RelMetadataQuery mq,
        double scanRowCount,
        RexNode filter,
        int projectCount,
        double costMultiplier
    ) {
        // 1. Get cost of the scan itself. For replicated map cost is multiplied by the number of nodes.
        double scanCpu = scanRowCount;

        // 2. Get cost of the filter, if any.
        double filterRowCount;
        double filterCpu;

        if (filter != null) {
            filterRowCount = CostUtils.adjustFilteredRowCount(scanRowCount, mq.getSelectivity(this, filter));
            filterCpu = CostUtils.adjustCpuForConstrainedScan(scanCpu);
        } else {
            filterRowCount = scanRowCount;
            filterCpu = 0;
        }

        // 3. Get cost of the project taking in count filter and number of expressions. Project never produces IO.
        double projectCpu = CostUtils.adjustCpuForConstrainedScan(CostUtils.getProjectCpu(filterRowCount, projectCount));

        // 4. Finally, return sum of both scan and project.
        return planner.getCostFactory().makeCost(
            filterRowCount,
            (scanCpu + filterCpu + projectCpu) * costMultiplier,
            0
        );
    }
}
