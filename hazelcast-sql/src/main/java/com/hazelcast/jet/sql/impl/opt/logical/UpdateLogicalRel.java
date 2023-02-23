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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;

import static org.apache.calcite.rel.core.TableModify.Operation.UPDATE;

public class UpdateLogicalRel extends TableModify implements LogicalRel {

    private final RexNode predicate;

    UpdateLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            CatalogReader catalogReader,
            RelNode input,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened,
            RexNode predicate
    ) {
        super(cluster, traitSet, table, catalogReader, input, UPDATE, updateColumnList, sourceExpressionList, flattened);
        this.predicate = predicate;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeHugeCost();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new UpdateLogicalRel(
                getCluster(),
                traitSet,
                getTable(),
                getCatalogReader(),
                sole(inputs),
                getUpdateColumnList(),
                getSourceExpressionList(),
                isFlattened(),
                predicate
        );
    }

    public RexNode getPredicate() {
        return predicate;
    }
}
