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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.AggregateLogicalRel;
import com.hazelcast.sql.impl.row.JetSqlRow;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.immutables.value.Value;

import java.util.Collection;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

@Value.Enclosing
final class AggregateBatchPhysicalRule extends AggregateAbstractPhysicalRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableAggregateBatchPhysicalRule.Config.builder()
                .description(AggregateBatchPhysicalRule.class.getSimpleName())
                .operandSupplier(b0 -> b0.operand(AggregateLogicalRel.class)
                        .trait(LOGICAL)
                        .predicate(OptUtils::isBounded)
                        .inputs(b1 -> b1.operand(RelNode.class).anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new AggregateBatchPhysicalRule(this);
        }
    }

    private AggregateBatchPhysicalRule(Config config) {
        super(config);
    }

    @SuppressWarnings("checkstyle:DeclarationOrder")
    static final RelOptRule INSTANCE = new AggregateBatchPhysicalRule(Config.DEFAULT);

    @Override
    public void onMatch(RelOptRuleCall call) {
        AggregateLogicalRel logicalAggregate = call.rel(0);
        RelNode input = logicalAggregate.getInput();

        assert logicalAggregate.getGroupType() == Group.SIMPLE;

        RelNode convertedInput = OptUtils.toPhysicalInput(input);
        Collection<RelNode> transformedInputs = OptUtils.extractPhysicalRelsFromSubset(convertedInput);
        for (RelNode transformedInput : transformedInputs) {
            call.transformTo(transform(logicalAggregate, transformedInput));
        }
    }

    private RelNode transform(AggregateLogicalRel logicalAggregate, RelNode physicalInput) {
        return logicalAggregate.getGroupSet().cardinality() == 0
                ? toAggregate(logicalAggregate, physicalInput)
                : toAggregateByKey(logicalAggregate, physicalInput);
    }

    private static RelNode toAggregate(AggregateLogicalRel logicalAggregate, RelNode physicalInput) {
        AggregateOperation<?, JetSqlRow> aggrOp = aggregateOperation(
                physicalInput.getRowType(),
                logicalAggregate.getGroupSet(),
                logicalAggregate.getAggCallList()
        );

        if (logicalAggregate.containsDistinctCall()) {
            return new AggregatePhysicalRel(
                    physicalInput.getCluster(),
                    physicalInput.getTraitSet(),
                    physicalInput,
                    logicalAggregate.getGroupSet(),
                    logicalAggregate.getGroupSets(),
                    logicalAggregate.getAggCallList(),
                    aggrOp
            );
        } else {
            RelNode rel = new AggregateAccumulatePhysicalRel(
                    physicalInput.getCluster(),
                    physicalInput.getTraitSet(),
                    physicalInput,
                    aggrOp
            );

            return new AggregateCombinePhysicalRel(
                    rel.getCluster(),
                    rel.getTraitSet(),
                    rel,
                    logicalAggregate.getGroupSet(),
                    logicalAggregate.getGroupSets(),
                    logicalAggregate.getAggCallList(),
                    aggrOp
            );
        }
    }

    private static RelNode toAggregateByKey(AggregateLogicalRel logicalAggregate, RelNode physicalInput) {
        AggregateOperation<?, JetSqlRow> aggrOp = aggregateOperation(
                physicalInput.getRowType(),
                logicalAggregate.getGroupSet(),
                logicalAggregate.getAggCallList()
        );

        if (logicalAggregate.containsDistinctCall()) {
            return new AggregateByKeyPhysicalRel(
                    physicalInput.getCluster(),
                    physicalInput.getTraitSet(),
                    physicalInput,
                    logicalAggregate.getGroupSet(),
                    logicalAggregate.getGroupSets(),
                    logicalAggregate.getAggCallList(),
                    aggrOp
            );
        } else {
            RelNode rel = new AggregateAccumulateByKeyPhysicalRel(
                    physicalInput.getCluster(),
                    physicalInput.getTraitSet(),
                    physicalInput,
                    logicalAggregate.getGroupSet(),
                    aggrOp
            );

            return new AggregateCombineByKeyPhysicalRel(
                    rel.getCluster(),
                    rel.getTraitSet(),
                    rel,
                    logicalAggregate.getGroupSet(),
                    logicalAggregate.getGroupSets(),
                    logicalAggregate.getAggCallList(),
                    aggrOp
            );
        }
    }
}
