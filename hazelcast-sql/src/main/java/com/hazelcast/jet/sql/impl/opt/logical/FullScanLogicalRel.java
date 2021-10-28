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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;

import javax.annotation.Nullable;
import java.util.List;

import static java.util.Collections.emptyList;

public class FullScanLogicalRel extends TableScan implements LogicalRel {

    private final FunctionEx<ExpressionEvalContext, EventTimePolicy<Object[]>> eventTimePolicyProvider;

    FullScanLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table
    ) {
        this(cluster, traitSet, table, null);
    }

    FullScanLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<Object[]>> eventTimePolicyProvider
    ) {
        super(cluster, traitSet, emptyList(), table);

        this.eventTimePolicyProvider = eventTimePolicyProvider;
    }

    @Nullable
    public FunctionEx<ExpressionEvalContext, EventTimePolicy<Object[]>> eventTimePolicyProvider() {
        return eventTimePolicyProvider;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .itemIf("eventTimePolicyProvider", eventTimePolicyProvider, eventTimePolicyProvider != null);
    }

    @Override
    public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new FullScanLogicalRel(getCluster(), traitSet, getTable(), eventTimePolicyProvider);
    }
}
