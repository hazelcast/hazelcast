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

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

/**
 * Planner rule that matches VALUES-based {@link PartitionedMapTable} SINK.
 * <p>For example,</p>
 * <blockquote><code>SINK INTO map VALUES (1, '1')</code></blockquote>
 * <p>
 * Such SINK is translated to optimized, direct key(s) {@code IMap} operation
 * which does not involve starting any job.
 */
public final class SinkMapLogicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new SinkMapLogicalRule();

    private SinkMapLogicalRule() {
        super(
                operandJ(
                        SinkLogicalRel.class, LOGICAL, sink -> !OptUtils.requiresJob(sink)
                                && OptUtils.hasTableType(sink, PartitionedMapTable.class),
                        operand(ValuesLogicalRel.class, none())
                ),
                SinkMapLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        SinkLogicalRel logicalSink = call.rel(0);
        ValuesLogicalRel logicalValues = call.rel(1);

        SinkMapLogicalRel rel = new SinkMapLogicalRel(
                logicalSink.getCluster(),
                OptUtils.toLogicalConvention(logicalSink.getTraitSet()),
                logicalSink.getTable(),
                logicalValues.values()
        );
        call.transformTo(rel);
    }
}
