/*
 * Copyright 2023 Hazelcast Inc.
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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class DeleteLogicalRel extends AbstractRelNode implements LogicalRel {

    private final RelOptTable table;
    private final CatalogReader catalogReader;
    private final RelNode input;
    private final boolean flattened;
    private final RexNode predicate;

    protected DeleteLogicalRel(
            @Nonnull RelOptCluster cluster,
            @Nonnull RelTraitSet traitSet,
            @Nonnull RelOptTable table,
            @Nonnull Prepare.CatalogReader catalogReader,
            @Nullable RelNode input,
            boolean flattened,
            @Nullable RexNode predicate
    ) {
        super(cluster, traitSet);
        this.table = table;
        this.catalogReader = catalogReader;
        this.input = input;
        this.flattened = flattened;
        this.predicate = predicate;
    }

    @Nonnull
    @Override
    public RelOptTable getTable() {
        return table;
    }

    @Nonnull
    public CatalogReader getCatalogReader() {
        return catalogReader;
    }

    @Nullable
    public RelNode getInput() {
        return input;
    }

    @Override
    public List<RelNode> getInputs() {
        return input != null ? Collections.singletonList(input) : Collections.emptyList();
    }

    public boolean isFlattened() {
        return flattened;
    }

    @Nullable
    public RexNode getPredicate() {
        return predicate;
    }

    @Override
    public RelDataType deriveRowType() {
        return RelOptUtil.createDmlRowType(SqlKind.DELETE, getCluster().getTypeFactory());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // If we have input, each row has to be deleted separately,
        // so we get cost of scan + cost of delete.
        // If there is no input, the delete still has to executed in the connected system
        // which will also have cost proportional to number of rows.
        // Version with no input will be preferred because of smaller total cost.
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DeleteLogicalRel(getCluster(), traitSet, getTable(), getCatalogReader(), sole(inputs), isFlattened(),
                predicate);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelWriter w = super.explainTerms(pw);
        if (input != null) {
            w.input("input", getInput());
        }
        return w
                .item("table", table.getQualifiedName())
                .item("flattened", flattened)
                .itemIf("predicate", predicate, predicate != null);

    }
}
