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

import com.hazelcast.config.IndexType;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.opt.FieldCollation;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.cost.CostUtils;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;

import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.stream.Collectors.toList;

/**
 * Map index scan operator.
 */
public class IndexScanMapPhysicalRel extends TableScan implements PhysicalRel {

    private final MapTableIndex index;
    private final IndexFilter indexFilter;
    private final RexNode indexExp;
    private final RexNode remainderExp;

    public IndexScanMapPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            MapTableIndex index,
            IndexFilter indexFilter,
            RexNode indexExp,
            RexNode remainderExp
    ) {
        super(cluster, traitSet, table);

        this.index = index;
        this.indexFilter = indexFilter;
        this.indexExp = indexExp;
        this.remainderExp = remainderExp;
    }

    public MapTableIndex getIndex() {
        return index;
    }

    public IndexFilter getIndexFilter() {
        return indexFilter;
    }

    public RexNode getRemainderExp() {
        return remainderExp;
    }

    public ComparatorEx<JetSqlRow> getComparator() {
        if (index.getType() == IndexType.SORTED) {
            RelCollation relCollation = getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
            List<FieldCollation> collations = relCollation.getFieldCollations().stream()
                    .map(FieldCollation::new)
                    .collect(toList());
            return ExpressionUtil.comparisonFn(collations);
        } else {
            return null;
        }
    }

    public boolean isDescending() {
        boolean descending = false;
        RelCollation relCollation = getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

        // Take first direction as main direction.
        // In case of different directions Scan + Sort relations combination should be used.
        if (!relCollation.getFieldCollations().isEmpty()) {
            descending = relCollation.getFieldCollations().get(0).getDirection().isDescending();
        }

        return descending;
    }

    public Expression<Boolean> filter(QueryParameterMetadata parameterMetadata) {
        PlanNodeSchema schema = OptUtils.schema(getTable());
        return filter(schema, remainderExp, parameterMetadata);
    }

    public List<Expression<?>> projection(QueryParameterMetadata parameterMetadata) {
        PlanNodeSchema schema = OptUtils.schema(getTable());

        HazelcastTable table = getTable().unwrap(HazelcastTable.class);
        return project(schema, table.getProjects(), parameterMetadata);
    }

    public HazelcastTable getTableUnwrapped() {
        return table.unwrap(HazelcastTable.class);
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        List<QueryDataType> fieldTypes = toList(projection(parameterMetadata), Expression::getType);
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onMapIndexScan(this);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double rowCount = table.getRowCount();

        if (indexExp != null) {
            rowCount = CostUtils.adjustFilteredRowCount(rowCount, RelMdUtil.guessSelectivity(indexExp));
        }

        if (remainderExp != null) {
            rowCount = CostUtils.adjustFilteredRowCount(rowCount, RelMdUtil.guessSelectivity(remainderExp));
        }

        return rowCount;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Get the number of rows being scanned. This is either the whole index (scan), or only part of the index (lookup)
        double scanRowCount = table.getRowCount();

        if (indexExp != null) {
            scanRowCount = CostUtils.adjustFilteredRowCount(scanRowCount, RelMdUtil.guessSelectivity(indexExp));
        }

        // Get the number of rows that we expect after the remainder filter is applied.
        boolean hasFilter = remainderExp != null;
        double filterRowCount = scanRowCount;

        if (hasFilter) {
            filterRowCount = CostUtils.adjustFilteredRowCount(filterRowCount, RelMdUtil.guessSelectivity(remainderExp));
        }

        return computeSelfCost(
                planner,
                scanRowCount,
                CostUtils.indexScanCpuMultiplier(index.getType()),
                hasFilter,
                filterRowCount,
                getTableUnwrapped().getProjects().size()
        );
    }

    private static RelOptCost computeSelfCost(
            RelOptPlanner planner,
            double scanRowCount,
            double scanCostMultiplier,
            boolean hasFilter,
            double filterRowCount,
            int projectCount
    ) {
        // 1. Get cost of the scan itself.
        double scanCpu = scanRowCount * scanCostMultiplier;

        // 2. Get cost of the filter, if any.
        double filterCpu = hasFilter ? CostUtils.adjustCpuForConstrainedScan(scanCpu) : 0;

        // 3. Get cost of the project taking into account the filter and number of expressions. Project never produces IO.
        double projectCpu = CostUtils.adjustCpuForConstrainedScan(CostUtils.getProjectCpu(filterRowCount, projectCount));

        // 4. Finally, return sum of both scan and project.
        return planner.getCostFactory().makeCost(
                filterRowCount,
                scanCpu + filterCpu + projectCpu,
                0
        );
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("index", index.getName())
                .item("indexExp", indexExp)
                .item("remainderExp", remainderExp);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IndexScanMapPhysicalRel(getCluster(), traitSet, getTable(), index, indexFilter, indexExp, remainderExp);
    }
}
