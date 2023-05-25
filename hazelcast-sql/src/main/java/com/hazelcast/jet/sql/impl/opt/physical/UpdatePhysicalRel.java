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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class UpdatePhysicalRel extends AbstractRelNode implements PhysicalRel {

    private final RelOptTable table;
    private final CatalogReader catalogReader;
    private RelNode input;
    private final List<String> updateColumnList;
    private final List<RexNode> sourceExpressionList;
    private final boolean flattened;
    private final RexNode predicate;

    UpdatePhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            Prepare.CatalogReader catalogReader,
            RelNode input,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened,
            RexNode predicate
    ) {
        super(cluster, traitSet);
        this.table = table;
        this.catalogReader = catalogReader;
        this.input = input;
        this.updateColumnList = updateColumnList;
        this.sourceExpressionList = sourceExpressionList;
        this.flattened = flattened;
        this.predicate = predicate;
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public RelOptTable getTable() {
        return table;
    }

    public CatalogReader getCatalogReader() {
        return catalogReader;
    }

    public RelNode getInput() {
        return input;
    }

    @Override
    public List<RelNode> getInputs() {
        return input != null ? Collections.singletonList(input) : Collections.emptyList();
    }

    @Override
    public void replaceInput(int ordinalInParent, RelNode rel) {
        assert input != null;
        assert ordinalInParent == 0;
        this.input = rel;
        recomputeDigest();
    }

    public List<String> getUpdateColumnList() {
        return updateColumnList;
    }

    public List<RexNode> getSourceExpressionList() {
        return sourceExpressionList;
    }

    public boolean isFlattened() {
        return flattened;
    }

    public RexNode getPredicate() {
        return predicate;
    }

    @Override
    public RelDataType deriveRowType() {
        return RelOptUtil.createDmlRowType(SqlKind.UPDATE, getCluster().getTypeFactory());
    }

    @Override
    public <V> V accept(CreateDagVisitor<V> visitor) {
        return visitor.onUpdate(this);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new UpdatePhysicalRel(
                getCluster(),
                traitSet,
                getTable(),
                getCatalogReader(),
                sole(inputs),
                getUpdateColumnList(),
                getSourceExpressionList(),
                isFlattened(),
                predicate
        );
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelWriter w = super.explainTerms(pw);
        if (input != null) {
            w.input("input", getInput());
        }
        return w
                .item("table", table.getQualifiedName())
                .item("updateColumnList", updateColumnList)
                .item("sourceExpressionList", sourceExpressionList)
                .item("flattened", flattened)
                .itemIf("predicate", predicate, predicate != null);
    }
}
