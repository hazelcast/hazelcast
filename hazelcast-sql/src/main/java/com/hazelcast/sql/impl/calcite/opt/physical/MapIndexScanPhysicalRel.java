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

import com.hazelcast.sql.impl.calcite.opt.AbstractMapScanRel;
import com.hazelcast.sql.impl.calcite.opt.cost.CostUtils;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PhysicalRelVisitor;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Map index scan operator.
 */
public class MapIndexScanPhysicalRel extends AbstractMapScanRel implements PhysicalRel {
    /** Target index. */
    private final MapTableIndex index;

    /** Index filter. */
    private final IndexFilter indexFilter;

    /** Remainder filter. */
    private final RexNode remainderFilter;

    public MapIndexScanPhysicalRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        List<Integer> projects,
        MapTableIndex index,
        IndexFilter indexFilter,
        RexNode remainderFilter,
        RexNode originalFilter
    ) {
        super(cluster, traitSet, table, projects, originalFilter);

        this.index = index;
        this.indexFilter = indexFilter;
        this.remainderFilter = remainderFilter;
    }

    public MapTableIndex getIndex() {
        return index;
    }

    public IndexFilter getIndexFilter() {
        return indexFilter;
    }

    public RexNode getRemainderFilter() {
        return remainderFilter;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MapIndexScanPhysicalRel(
            getCluster(),
            traitSet,
            getTable(),
            projects,
            index,
            indexFilter,
            remainderFilter,
            filter
        );
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        visitor.onMapIndexScan(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
           .item("index", index)
           .item("indexFilter", indexFilter)
           .item("remainderFilter", remainderFilter);
    }

    // TODO: Dedup with logical scan
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // 1. Get cost of the scan itself. For replicated map cost is multiplied by the number of nodes.
        RelOptCost scanCost = super.computeSelfCost(planner, mq);

        if (isReplicated()) {
            scanCost = scanCost.multiplyBy(getMemberCount());
        }

        // 2. Get cost of the project taking in count filter and number of expressions. Project never produces IO.
        double filterSelectivity = mq.getSelectivity(this, filter);
        double filterRowCount = scanCost.getRows() * filterSelectivity;

        int expressionCount = getProjects().size();

        double projectCpu = CostUtils.adjustProjectCpu(filterRowCount * expressionCount, true);

        // 3. Finally, return sum of both scan and project. Note that we decrease the cost of the scan by selectivity factor.
        RelOptCost totalCost = planner.getCostFactory().makeCost(
            filterRowCount,
            scanCost.getCpu() * filterSelectivity + projectCpu,
            scanCost.getIo()
        );

        return totalCost;
    }
}
