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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.AggregateLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;

/**
 * This is a way to handle cases when the aggregation isn't implemented by
 * replacing it with {@link StreamAggregateMisusePhysicalRel}, which has huge cost.
 * If no other rule replaces the aggregation with some executable relation,
 * the error will be thrown to the user.
 * <p>
 * Currently, there's only {@link AggregateSlidingWindowPhysicalRule} that
 * handles watermarked, windowed streaming aggregation.
 */
public final class StreamAggregateConverterRule extends ConverterRule {

    public static final RelOptRule INSTANCE = new StreamAggregateConverterRule();

    private StreamAggregateConverterRule() {
        super(
                AggregateLogicalRel.class,
                OptUtils::isUnbounded,
                LOGICAL,
                PHYSICAL,
                StreamAggregateConverterRule.class.getSimpleName()
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        AggregateLogicalRel agg = (AggregateLogicalRel) rel;
        return new StreamAggregateMisusePhysicalRel(
                agg.getCluster(),
                OptUtils.toPhysicalConvention(agg.getTraitSet()),
                agg.getHints(),
                OptUtils.toPhysicalInput(agg.getInput()),
                agg.getGroupSet(),
                agg.getGroupSets(),
                agg.getAggCallList()
        );
    }
}

