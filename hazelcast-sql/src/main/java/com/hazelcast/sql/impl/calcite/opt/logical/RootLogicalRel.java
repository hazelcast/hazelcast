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

import com.hazelcast.sql.impl.calcite.opt.AbstractRootRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * Logical root node. Installed on top of logical relational tree manually at the end of logical optimization phase.
 */
public class RootLogicalRel extends AbstractRootRel implements LogicalRel, com.hazelcast.jet.sql.impl.opt.logical.LogicalRel {
    public RootLogicalRel(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    @Override
    public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new RootLogicalRel(getCluster(), traitSet, sole(inputs));
    }
}
