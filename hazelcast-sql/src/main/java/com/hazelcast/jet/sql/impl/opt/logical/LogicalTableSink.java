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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;

import java.util.List;

public class LogicalTableSink extends TableModify {

    public LogicalTableSink(TableModify modify) {
        this(
                modify.getCluster(),
                modify.getTraitSet(),
                modify.getTable(),
                modify.getCatalogReader(),
                modify.getInput(),
                modify.isFlattened()
        );

        assert modify.getOperation() == Operation.INSERT;
    }

    private LogicalTableSink(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            Prepare.CatalogReader schema,
            RelNode input,
            boolean flattened
    ) {
        super(cluster, traitSet, table, schema, input, Operation.INSERT, null, null, flattened);
    }

    @Override
    public LogicalTableSink copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new LogicalTableSink(getCluster(), traitSet, getTable(), getCatalogReader(), sole(inputs), isFlattened());
    }
}
