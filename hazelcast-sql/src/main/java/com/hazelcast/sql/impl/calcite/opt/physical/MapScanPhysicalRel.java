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

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.calcite.cost.CostUtils;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PhysicalRelVisitor;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Physical scan over partitioned map.
 * <p>
 * Traits:
 * <ul>
 *     <li><b>Collation</b>: empty, as map is not sorted</li>
 *     <li><b>Distribution</b>: PARTITIONED</li>
 * </ul>
 */
public class MapScanPhysicalRel extends AbstractMapScanPhysicalRel {
    public MapScanPhysicalRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        List<Integer> projects,
        RexNode filter
    ) {
        super(cluster, traitSet, table, projects, filter);
    }

    @Override
    public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MapScanPhysicalRel(getCluster(), traitSet, getTable(), projects, filter);
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        visitor.onMapScan(this);
    }

    // TODO: Dedup with logical scan
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // 1. Get cost of the scan itself. For replicated map cost is multiplied by the number of nodes.
        RelOptCost scanCost = super.computeSelfCost(planner, mq);

        if (table.unwrap(HazelcastTable.class).isReplicated()) {
            scanCost = scanCost.multiplyBy(getHazelcastCluster().getMemberCount());
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
