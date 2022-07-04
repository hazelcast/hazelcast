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

import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
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

    private final RelOptTable table;
    private final RexNode keyCondition;

    DeleteByKeyMapLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            RexNode keyCondition
    ) {
        super(cluster, traitSet);

        assert table.unwrap(HazelcastTable.class).getTarget() instanceof PartitionedMapTable;

        this.table = table;
        this.keyCondition = keyCondition;
    }

    public RelOptTable table() {
        return table;
    }

    public RexNode keyCondition() {
        return keyCondition;
    }

    @Override
    public RelDataType deriveRowType() {
        return RelOptUtil.createDmlRowType(SqlKind.DELETE, getCluster().getTypeFactory());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // zero as not starting any job
        return planner.getCostFactory().makeZeroCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw
                .item("table", table.getQualifiedName())
                .item("keyCondition", keyCondition);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DeleteByKeyMapLogicalRel(getCluster(), traitSet, table, keyCondition);
    }
}
