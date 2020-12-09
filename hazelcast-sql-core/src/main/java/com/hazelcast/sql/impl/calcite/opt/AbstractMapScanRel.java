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
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.AbstractMapTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * Base class for map scans.
 */
public abstract class AbstractMapScanRel extends AbstractScanRel {
    public AbstractMapScanRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table
    ) {
        super(cluster, traitSet, table);
    }

    public AbstractMapTable getMap() {
        return getTableUnwrapped().getTarget();
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        HazelcastTable table0 = getTableUnwrapped();

        return computeSelfCost(
            planner,
            table0.getTotalRowCount(),
            CostUtils.TABLE_SCAN_CPU_MULTIPLIER,
            table0.getFilter() != null,
            table.getRowCount(),
            table0.getProjects().size()
        );
    }

    protected RelOptCost computeSelfCost(
        RelOptPlanner planner,
        double scanRowCount,
        double scanCostMultiplier,
        boolean hasFilter,
        double filterRowCount,
        int projectCount
    ) {
        // 1. Get cost of the scan itself.
        double scanCpu = scanRowCount * scanCostMultiplier;

        // 2. Get cost of the filter, if any.
        double filterCpu = hasFilter ? CostUtils.adjustCpuForConstrainedScan(scanCpu) : 0;

        // 3. Get cost of the project taking into account the filter and number of expressions. Project never produces IO.
        double projectCpu = CostUtils.adjustCpuForConstrainedScan(CostUtils.getProjectCpu(filterRowCount, projectCount));

        // 4. Finally, return sum of both scan and project.
        return planner.getCostFactory().makeCost(
            filterRowCount,
            scanCpu + filterCpu + projectCpu,
            0
        );
    }
}
