/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.FullScan;
import com.hazelcast.jet.sql.impl.opt.cost.CostUtils;
import com.hazelcast.sql.impl.expression.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import javax.annotation.Nullable;
import java.util.List;

public class FullScanLogicalRel extends FullScan implements LogicalRel {

    public FullScanLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            @Nullable Expression<?> lagExpression,
            int watermarkedColumnIndex
    ) {
        super(cluster, traitSet, table, lagExpression, watermarkedColumnIndex);
    }

    @Override
    public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new FullScanLogicalRel(getCluster(), traitSet, getTable(), lagExpression(), watermarkedColumnIndex());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Prefer scans with smaller number of results/expressions.
        // This is needed to ensure that UPDATE scans use only key column
        // TODO: make this calculation consistent with FullScanPhysicalRel cost
        RelOptCost baseCost = super.computeSelfCost(planner, mq);
        if (table.getRowType().getFieldCount() > 1) {
            return CostUtils.preferSingleExpressionScan(baseCost);
        }
        return baseCost;
    }
}
