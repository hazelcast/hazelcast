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
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PhysicalRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Exchange which merges sorted input streams from several nodes into a single sorted stream on a single node.
 * <p>
 * Traits:
 * <ul>
 *     <li><b>Collation</b>: derived from the input, never empty</li>
 *     <li><b>Distribution</b>: ROOT</li>
 * </ul>
 */
public class SortMergeExchangePhysicalRel extends AbstractExchangePhysicalRel {

    private final RelCollation collation;
    private final RexNode fetch;
    private final RexNode offset;

    public SortMergeExchangePhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RelCollation collation,
            RexNode fetch,
            RexNode offset
    ) {
        super(cluster, traitSet, input);

        this.collation = collation;
        this.fetch = fetch;
        this.offset = offset;
    }

    public RelCollation getCollation() {
        return collation;
    }

    public RexNode getFetch() {
        return fetch;
    }

    public RexNode getOffset() {
        return offset;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new SortMergeExchangePhysicalRel(getCluster(), traitSet, sole(inputs), collation, fetch, offset);
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        ((PhysicalRel) input).visit(visitor);

        visitor.onSortMergeExchange(this);
    }

    @Override
    public final RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);

        return pw.item("collation", collation.getFieldCollations())
                 .item("fetch", fetch).item("offset", offset);
    }
}
