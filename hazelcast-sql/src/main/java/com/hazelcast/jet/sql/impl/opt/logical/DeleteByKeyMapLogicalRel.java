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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

public class DeleteByKeyMapLogicalRel extends AbstractRelNode implements LogicalRel {

    private final PartitionedMapTable table;
    private final RexNode primaryKeyCondition;

    protected DeleteByKeyMapLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            PartitionedMapTable table,
            RexNode primaryKeyCondition
    ) {
        super(cluster, traitSet);

        this.table = table;
        this.primaryKeyCondition = primaryKeyCondition;
    }

    public PartitionedMapTable table() {
        return table;
    }

    public RexNode primaryKeyCondition() {
        return primaryKeyCondition;
    }

    @Override
    public RelDataType deriveRowType() {
        return RelOptUtil.createDmlRowType(SqlKind.DELETE, getCluster().getTypeFactory());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(this);
        return planner.getCostFactory().makeCost(rowCount, 1, 1);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw
                .item("table", table.getSqlName())
                .item("primaryKeyCondition", primaryKeyCondition);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DeleteByKeyMapLogicalRel(getCluster(), traitSet, table, primaryKeyCondition);
    }
}
