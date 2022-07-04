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
 * Planner rule that matches single row, VALUES-based {@link PartitionedMapTable}
 * INSERT.
 * <p>For example,</p>
 * <blockquote><code>INSERT INTO map VALUES (1, '1')</code></blockquote>
 * <p>
 * Such INSERT is translated to optimized, direct key {@code IMap} operation
 * which does not involve starting any job.
 */
public final class InsertMapLogicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new InsertMapLogicalRule();

    private InsertMapLogicalRule() {
        super(
                operandJ(
                        InsertLogicalRel.class, LOGICAL, insert -> !OptUtils.requiresJob(insert)
                                && OptUtils.hasTableType(insert, PartitionedMapTable.class),
                        operand(ValuesLogicalRel.class, none())
                ),
                InsertMapLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        InsertLogicalRel logicalInsert = call.rel(0);
        ValuesLogicalRel logicalValues = call.rel(1);

        if (logicalValues.size() == 1) {
            InsertMapLogicalRel rel = new InsertMapLogicalRel(
                    logicalInsert.getCluster(),
                    OptUtils.toLogicalConvention(logicalInsert.getTraitSet()),
                    logicalInsert.getTable(),
                    logicalValues.values().get(0)
            );
            call.transformTo(rel);
        }
    }
}
