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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;

import java.util.List;

public class WatermarkLogicalRel extends SingleRel implements LogicalRel {

    private final FunctionEx<ExpressionEvalContext, EventTimePolicy<Object[]>> eventTimePolicyProvider;

    WatermarkLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            FunctionEx<ExpressionEvalContext, EventTimePolicy<Object[]>> eventTimePolicyProvider
    ) {
        super(cluster, traits, input);

        this.eventTimePolicyProvider = eventTimePolicyProvider;
    }

    public FunctionEx<ExpressionEvalContext, EventTimePolicy<Object[]>> eventTimePolicyProvider() {
        return eventTimePolicyProvider;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("eventTimePolicyProvider", eventTimePolicyProvider);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new WatermarkLogicalRel(getCluster(), traitSet, sole(inputs), eventTimePolicyProvider);
    }
}
