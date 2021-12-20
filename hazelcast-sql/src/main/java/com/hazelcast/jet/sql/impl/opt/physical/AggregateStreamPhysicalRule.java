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
import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMetadataQuery;
import com.hazelcast.jet.sql.impl.opt.metadata.WindowProperties;
import com.hazelcast.sql.impl.QueryException;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;

import java.util.Collection;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static java.util.Objects.requireNonNull;

final class AggregateStreamPhysicalRule extends AggregateAbstractPhysicalRule {

    static final RelOptRule INSTANCE = new AggregateStreamPhysicalRule();

    private AggregateStreamPhysicalRule() {
        super(
                operand(
                        AggregateLogicalRel.class,
                        LOGICAL,
                        AggregateStreamPhysicalRule::matches,
                        some(operand(RelNode.class, any()))
                ),
                AggregateStreamPhysicalRule.class.getSimpleName()
        );
    }

    private static boolean matches(AggregateLogicalRel logicalAggregate) {
        if (!OptUtils.isUnbounded(logicalAggregate)) {
            return false;
        }

        Collection<RelNode> logicalInputs = OptUtils.extractLogicalRelsFromSubset(logicalAggregate.getInput());
        for (RelNode logicalInput : logicalInputs) {
            if (extractWindowProperty(logicalAggregate, logicalInput) == null) {
                throw QueryException.error("Streaming aggregation must be grouped by window_start/window_end");
            }
        }

        return true;
    }

    @Override
    protected RelNode optimize(AggregateLogicalRel logicalAggregate, RelNode physicalInput) {
        WindowProperties.WindowProperty windowProperty = requireNonNull(extractWindowProperty(logicalAggregate, physicalInput));
        return toSlidingAggregateByKey(logicalAggregate, physicalInput, windowProperty);
    }

    private static WindowProperties.WindowProperty extractWindowProperty(
            AggregateLogicalRel logicalAggregate,
            RelNode input
    ) {
        HazelcastRelMetadataQuery query = OptUtils.metadataQuery(input);
        WindowProperties windowProperties = query.extractWindowProperties(input);
        if (windowProperties == null) {
            return null;
        }
        return windowProperties.findFirst(logicalAggregate.getGroupSet().asList());
    }

    private static RelNode toSlidingAggregateByKey(
            AggregateLogicalRel logicalAggregate,
            RelNode physicalInput,
            WindowProperties.WindowProperty windowProperty
    ) {
        AggregateOperation<?, Object[]> aggrOp = aggregateOperation(
                physicalInput.getRowType(),
                logicalAggregate.getGroupSet(),
                logicalAggregate.getAggCallList()
        );

        if (logicalAggregate.containsDistinctCall()) {
            return new SlidingWindowAggregateByKeyPhysicalRel(
                    physicalInput.getCluster(),
                    physicalInput.getTraitSet(),
                    physicalInput,
                    logicalAggregate.getGroupSet(),
                    logicalAggregate.getGroupSets(),
                    logicalAggregate.getAggCallList(),
                    aggrOp,
                    windowProperty
            );
        } else {
            RelNode rel = new SlidingWindowAggregateAccumulateByKeyPhysicalRel(
                    physicalInput.getCluster(),
                    physicalInput.getTraitSet(),
                    physicalInput,
                    logicalAggregate.getGroupSet(),
                    aggrOp,
                    windowProperty
            );

            return new SlidingWindowAggregateCombineByKeyPhysicalRel(
                    rel.getCluster(),
                    rel.getTraitSet(),
                    rel,
                    logicalAggregate.getGroupSet(),
                    logicalAggregate.getGroupSets(),
                    logicalAggregate.getAggCallList(),
                    aggrOp,
                    windowProperty
            );
        }
    }
}
