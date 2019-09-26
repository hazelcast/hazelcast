/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.physical.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.List;

/**
 * Exchange which takes data from several nodes and merges them into unsorted stream on a single node.
 * <p>
 * Traits:
 * <ul>
 *     <li><b>Collation</b>: empty, as resulting input consists of unordered batches from different nodes</li>
 *     <li><b>Distribution</b>: SINGLETON</li>
 * </ul>
 */
public class SingletonExchangePhysicalRel extends SingleRel implements PhysicalRel {
    public SingletonExchangePhysicalRel(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new SingletonExchangePhysicalRel(getCluster(), traitSet, sole(inputs));
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        ((PhysicalRel) input).visit(visitor);

        visitor.onSingletonExchange(this);
    }
}
