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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;

import java.util.List;

public class UnionLogicalRel extends Union implements LogicalRel {

    UnionLogicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelNode> inputs,
            boolean all
    ) {
        super(cluster, traits, inputs, all);
    }

    @Override
    public final Union copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new UnionLogicalRel(getCluster(), traitSet, inputs, all);
    }
}
