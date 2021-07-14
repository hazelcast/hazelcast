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

package com.hazelcast.sql.impl.calcite.opt.physical.exchange;

import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

/**
 * Base class for exchanges.
 */
public abstract class AbstractExchangePhysicalRel extends SingleRel implements PhysicalRel {
    public AbstractExchangePhysicalRel(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);

        assert getMemberCount() > 1 : "Exchange should not be instantiated on single-member topology.";
    }
}
