/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.opt.logical;

import com.hazelcast.sql.impl.calcite.opt.AbstractFilterRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

/**
 * Logical filter.
 */
public class FilterLogicalRel extends AbstractFilterRel implements LogicalRel {
    public FilterLogicalRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        RexNode condition
    ) {
        super(cluster, traits, input, condition);
    }

    @Override
    public final Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new FilterLogicalRel(getCluster(), traitSet, input, condition);
    }
}
