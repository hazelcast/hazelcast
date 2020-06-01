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

    private final MapTableIndex index;
    private final IndexFilter indexFilter;
    private final RexNode indexExp;
    private final RexNode remainderExp;

    public MapIndexScanPhysicalRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        List<Integer> projects,
        MapTableIndex index,
        IndexFilter indexFilter,
        RexNode indexExp,
        RexNode remainderExp,
        RexNode originalFilter
    ) {
        super(cluster, traitSet, table, projects, originalFilter);

        this.index = index;
        this.indexFilter = indexFilter;
        this.indexExp = indexExp;
        this.remainderExp = remainderExp;
    }

    public MapTableIndex getIndex() {
        return index;
    }

    public IndexFilter getIndexFilter() {
        return indexFilter;
    }

    public RexNode getRemainderExp() {
        return remainderExp;
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
            indexExp,
            remainderExp,
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
           .item("indexExp", indexExp)
           .item("remainderExp", remainderExp);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double scanRowCount = table.getRowCount();

        if (indexFilter == null) {
            // This is an index scan. It is costlier than normal scan due to an indirection between index records and data
            // records. Nevertheless, it might be useful due to collation provided by the index itself.
            return computeSelfCost(
                planner,
                mq,
                scanRowCount,
                filter,
                getProjects().size(),
                CostUtils.INDEX_SCAN_CPU_MULTIPLIER
            );
        } else {
            // This is an index lookup. We scan records matching index filter, then apply the remainder filter on the result.
            scanRowCount = CostUtils.adjustFilteredRowCount(scanRowCount, mq.getSelectivity(this, filter));

            return computeSelfCost(
                planner,
                mq,
                scanRowCount,
                remainderExp,
                getProjects().size(),
                CostUtils.INDEX_SCAN_CPU_MULTIPLIER
            );
        }
    }
}
