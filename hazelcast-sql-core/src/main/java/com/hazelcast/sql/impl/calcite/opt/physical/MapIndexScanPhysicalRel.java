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

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.calcite.opt.AbstractMapScanRel;
import com.hazelcast.sql.impl.calcite.opt.cost.CostUtils;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PhysicalRelVisitor;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.calcite.rel.RelFieldCollation.Direction;


import java.util.ArrayList;
import java.util.List;

/**
 * Map index scan operator.
 */
public class MapIndexScanPhysicalRel extends AbstractMapScanRel implements PhysicalRel {

    private final MapTableIndex index;
    private final IndexFilter indexFilter;
    private final List<QueryDataType> converterTypes;
    private final RexNode indexExp;
    private final RexNode remainderExp;

    public MapIndexScanPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            MapTableIndex index,
            IndexFilter indexFilter,
            List<QueryDataType> converterTypes,
            RexNode indexExp,
            RexNode remainderExp
    ) {
        super(cluster, traitSet, table);

        this.index = index;
        this.indexFilter = indexFilter;
        this.converterTypes = converterTypes;
        this.indexExp = indexExp;
        this.remainderExp = remainderExp;
    }

    public MapTableIndex getIndex() {
        return index;
    }

    public IndexFilter getIndexFilter() {
        return indexFilter;
    }

    public List<QueryDataType> getConverterTypes() {
        return converterTypes;
    }

    public RexNode getRemainderExp() {
        return remainderExp;
    }

    public List<Boolean> getAscs() {
        RelCollation collation = getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
        assert collation != null;
        int size = collation.getFieldCollations().size();

        List<Boolean> ascs = new ArrayList<>(size);
        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
            Boolean asc = fieldCollation.getDirection() == Direction.ASCENDING ? TRUE : FALSE;
            ascs.add(asc);
        }
        return ascs;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MapIndexScanPhysicalRel(
                getCluster(),
                traitSet,
                getTable(),
                index,
                indexFilter,
                converterTypes,
                indexExp,
                remainderExp
        );
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        visitor.onMapIndexScan(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("index", index.getName())
            .item("indexExp", indexExp)
            .item("remainderExp", remainderExp);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double rowCount = table.getRowCount();

        if (indexExp != null) {
            rowCount = CostUtils.adjustFilteredRowCount(rowCount, RelMdUtil.guessSelectivity(indexExp));
        }

        if (remainderExp != null) {
            rowCount = CostUtils.adjustFilteredRowCount(rowCount, RelMdUtil.guessSelectivity(remainderExp));
        }

        return rowCount;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Get the number of rows being scanned. This is either the whole index (scan), or only part of the index (lookup)
        double scanRowCount = table.getRowCount();

        if (indexExp != null) {
            scanRowCount = CostUtils.adjustFilteredRowCount(scanRowCount, RelMdUtil.guessSelectivity(indexExp));
        }

        // Get the number of rows that we expect after the remainder filter is applied.
        boolean hasFilter = remainderExp != null;

        double filterRowCount = scanRowCount;

        if (hasFilter) {
            filterRowCount = CostUtils.adjustFilteredRowCount(filterRowCount, RelMdUtil.guessSelectivity(remainderExp));
        }

        return computeSelfCost(
                planner,
                scanRowCount,
                CostUtils.indexScanCpuMultiplier(index.getType()),
                hasFilter,
                filterRowCount,
                getTableUnwrapped().getProjects().size()
        );
    }
}
