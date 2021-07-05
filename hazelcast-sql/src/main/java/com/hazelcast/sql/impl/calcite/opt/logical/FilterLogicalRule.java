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

package com.hazelcast.sql.impl.calcite.opt.logical;

import com.hazelcast.sql.impl.calcite.opt.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;

/**
 * Converts abstract filter to logical filter.
 */
public final class FilterLogicalRule extends ConverterRule {
    public static final RelOptRule INSTANCE = new FilterLogicalRule();

    private FilterLogicalRule() {
        super(LogicalFilter.class, Convention.NONE, HazelcastConventions.LOGICAL, FilterLogicalRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalFilter filter = (LogicalFilter) rel;
        RelNode input = filter.getInput();

        return new FilterLogicalRel(
                filter.getCluster(),
                OptUtils.toLogicalConvention(filter.getTraitSet()),
                OptUtils.toLogicalInput(input),
                filter.getCondition()
        );
    }
}
