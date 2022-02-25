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
import com.hazelcast.jet.sql.impl.opt.FieldCollation;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.stream.Collectors;

public class SortPhysicalRel extends Sort implements PhysicalRel {

    private final boolean requiresSort;

    SortPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            RelCollation collation,
            RexNode offset,
            RexNode fetch,
            RelDataType rowType,
            boolean requiresSort
    ) {
        super(cluster, traits, input, collation, offset, fetch);
        this.rowType = rowType;

        this.requiresSort = requiresSort;
    }

    public boolean requiresSort() {
        return requiresSort;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    // Copy of org.apache.calcite.rel.core.Sort.computeSelfCost, but also takes our requiresSort flag into account
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double offsetValue = Util.first(doubleValue(offset), 0d);

        double inCount = mq.getRowCount(input);
        Double fetchValue = doubleValue(fetch);
        double readCount;

        if (fetchValue == null) {
            readCount = inCount;
        } else if (fetchValue <= 0) {
            // Case 1. Read zero rows from input, therefore CPU cost is zero.
            return planner.getCostFactory().makeCost(inCount, 0, 0);
        } else {
            readCount = Math.min(inCount, offsetValue + fetchValue);
        }

        double bytesPerRow = (3 + getRowType().getFieldCount()) * 4;

        double cpu;
        if (collation.getFieldCollations().isEmpty() || !requiresSort) { // Here is the change
            // Case 2. If sort keys are empty, CPU cost is cheaper because we are just
            // stepping over the first "readCount" rows, rather than sorting all
            // "inCount" them. (Presumably we are applying FETCH and/or OFFSET,
            // otherwise this Sort is a no-op.)
            cpu = readCount * bytesPerRow;
        } else {
            // Case 3. Read and sort all "inCount" rows, keeping "readCount" in the
            // sort data structure at a time.
            cpu = Util.nLogM(inCount, readCount) * bytesPerRow;
        }
        return planner.getCostFactory().makeCost(readCount, cpu, 0);
    }

    public List<FieldCollation> getCollations() {
        return getCollation().getFieldCollations()
                .stream().map(FieldCollation::new).collect(Collectors.toList());
    }

    public Expression<?> fetch(QueryParameterMetadata parameterMetadata) {
        PlanNodeSchema schema = schema(parameterMetadata);
        RexVisitor<Expression<?>> visitor = OptUtils.createRexToExpressionVisitor(schema, parameterMetadata);
        return fetch.accept(visitor);
    }

    public Expression<?> offset(QueryParameterMetadata parameterMetadata) {
        PlanNodeSchema schema = schema(parameterMetadata);
        RexVisitor<Expression<?>> visitor = OptUtils.createRexToExpressionVisitor(schema, parameterMetadata);
        return offset.accept(visitor);
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        return OptUtils.schema(rowType);
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onSort(this);
    }

    @Override
    public final RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("requiresSort", requiresSort);
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new SortPhysicalRel(getCluster(), traitSet, input, collation, offset, fetch, rowType, requiresSort);
    }

    private static @Nullable Double doubleValue(@Nullable RexNode r) {
        return r instanceof RexLiteral
                ? ((RexLiteral) r).getValueAs(Double.class)
                : null;
    }
}
