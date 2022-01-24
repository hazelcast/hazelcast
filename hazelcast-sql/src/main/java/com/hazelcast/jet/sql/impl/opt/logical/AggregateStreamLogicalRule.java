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
import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMetadataQuery;
import com.hazelcast.jet.sql.impl.opt.metadata.WatermarkedFields;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.RelNode;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

public final class AggregateStreamLogicalRule extends RelRule<Config> {

    private static final Config RULE_CONFIG = Config.EMPTY
            .withDescription(AggregateStreamLogicalRule.class.getSimpleName())
            .withOperandSupplier(b0 -> b0.operand(AggregateLogicalRel.class)
                    .trait(LOGICAL)
                    .predicate(OptUtils::isUnbounded)
                    .anyInputs()
            );

    private AggregateStreamLogicalRule() {
        super(RULE_CONFIG);
    }

    @SuppressWarnings("checkstyle:DeclarationOrder")
    public static final RelOptRule INSTANCE = new AggregateStreamLogicalRule();

    @Override
    public void onMatch(RelOptRuleCall call) {
        AggregateLogicalRel aggr = call.rel(0);

        // TODO besides watermark order, we can also use normal collation
        RelNode input = aggr.getInput();
        HazelcastRelMetadataQuery query = OptUtils.metadataQuery(input);
        WatermarkedFields watermarkedFields = query.extractWatermarkedFields(input);
        if (watermarkedFields == null || watermarkedFields.findFirst(aggr.getGroupSet()) == null) {
            // no watermarked field by which we're grouping found
            call.transformTo(new NoExecuteRel(aggr.getCluster(), aggr.getTraitSet(), aggr.getRowType(),
                    "Streaming aggregation must be grouped by field with watermark order (IMPOSE_ORDER function)"));
        }
    }
}
