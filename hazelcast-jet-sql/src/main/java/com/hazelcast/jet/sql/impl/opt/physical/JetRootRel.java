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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.calcite.opt.AbstractRootRel;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

public class JetRootRel extends AbstractRootRel implements PhysicalRel {

    private final Address initiatorAddress;
    private final QueryId queryId;

    public JetRootRel(RelNode input, Address initiatorAddress, QueryId queryId) {
        super(input.getCluster(), RelTraitSet.createEmpty(), input);

        this.initiatorAddress = initiatorAddress;
        this.queryId = queryId;
    }

    public Address getInitiatorAddress() {
        return initiatorAddress;
    }

    public QueryId getQueryId() {
        return queryId;
    }

    @Override
    public PlanNodeSchema schema() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onRoot(this);
    }
}
