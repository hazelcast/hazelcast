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

import com.hazelcast.jet.core.Vertex;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.List;

public class WatermarkPhysicalRel extends SingleRel implements PhysicalRel {

    WatermarkPhysicalRel(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        // marker rel, schema should not be ever needed
        throw new UnsupportedOperationException();
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onWatermark(this);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new WatermarkPhysicalRel(getCluster(), traitSet, sole(inputs));
    }
}
