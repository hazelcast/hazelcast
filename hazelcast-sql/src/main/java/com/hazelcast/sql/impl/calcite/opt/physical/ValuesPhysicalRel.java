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

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.google.common.collect.ImmutableList;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PhysicalRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

public class ValuesPhysicalRel extends Values implements PhysicalRel {
    public ValuesPhysicalRel(
            RelOptCluster cluster,
            RelDataType rowType,
            ImmutableList<ImmutableList<RexLiteral>> tuples,
            RelTraitSet traits
    ) {
        super(cluster, rowType, tuples, traits);
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        visitor.onValues(this);
    }
}
