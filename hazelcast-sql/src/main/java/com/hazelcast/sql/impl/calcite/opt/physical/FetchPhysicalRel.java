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

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PhysicalRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * A node to perform limit/offset operations.
 */
// TODO: Implement fetch-pushdown rule. The idea is to limit the number of returned rows as early as possible. This op
//  optimization is applicable to nodes which doesn't change the number of rows. E.g.: project, semi-join.
public class FetchPhysicalRel extends SingleRel implements PhysicalRel {
    private final RexNode fetch;
    private final RexNode offset;

    public FetchPhysicalRel(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode fetch, RexNode offset) {
        super(cluster, traits, input);

        this.fetch = fetch;
        this.offset = offset;
    }

    public RexNode getFetch() {
        return fetch;
    }

    public RexNode getOffset() {
        return offset;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new FetchPhysicalRel(getCluster(), traitSet, sole(inputs), fetch, offset);
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        ((PhysicalRel) input).visit(visitor);

        visitor.onFetch(this);
    }

    @Override
    public final RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);

        return pw.item("fetch", fetch).item("offset", offset);
    }
}
