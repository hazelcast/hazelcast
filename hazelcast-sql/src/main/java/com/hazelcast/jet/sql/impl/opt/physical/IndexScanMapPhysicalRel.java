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

import com.hazelcast.config.IndexType;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.HazelcastPhysicalScan;
import com.hazelcast.jet.sql.impl.opt.FieldCollation;
import com.hazelcast.jet.sql.impl.opt.cost.CostUtils;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
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
public class IndexScanMapPhysicalRel extends TableScan implements HazelcastPhysicalScan {

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
            RelCollation relCollation = getTraitSet().getCollation();
            assert relCollation != null;
            List<FieldCollation> collations;
            if (relCollation != RelCollations.EMPTY) {
                collations = FieldCollation.convertCollation(relCollation.getFieldCollations());
            } else {
                // order is not forced, use default comparator for the index columns
                collations = index.getFieldOrdinals().stream()
                        .map(i -> new FieldCollation(i, RelFieldCollation.Direction.ASCENDING,
                                RelFieldCollation.NullDirection.UNSPECIFIED))
                        .collect(toList());
            }
            return ExpressionUtil.comparisonFn(collations);
        } else {
            return null;
        }
    }

    public boolean isDescending() {
        boolean descending = false;
        RelCollation relCollation = getTraitSet().getCollation();

        // Take first direction as main direction.
        // In case of different directions Scan + Sort relations combination should be used.
        if (!relCollation.getFieldCollations().isEmpty()) {
            descending = relCollation.getFieldCollations().get(0).getDirection().isDescending();
        }

        return descending;
    }

    public boolean requiresSort() {
        return index.getType() == IndexType.SORTED && getTraitSet().getCollation() != RelCollations.EMPTY;
    }


    @Override
    public RexNode filter() {
        return remainderExp;
    }

    @Override
    public List<RexNode> projection() {
        return getTable().unwrap(HazelcastTable.class).getProjects();
    }

    public HazelcastTable getTableUnwrapped() {
        return table.unwrap(HazelcastTable.class);
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        List<QueryDataType> fieldTypes = toList(projection(), rexNode -> HazelcastTypeUtils.toHazelcastType(rexNode.getType()));
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public <V> V accept(CreateDagVisitor<V> visitor) {
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
                requiresSort(),
                getTableUnwrapped().getProjects().size()
        );
    }

    private static RelOptCost computeSelfCost(
            RelOptPlanner planner,
            double scanRowCount,
            double scanCostMultiplier,
            boolean hasFilter,
            double filterRowCount,
            boolean requiresSort,
            int projectCount
    ) {
        // 1. Get cost of the scan itself.
        double scanCpu = scanRowCount * scanCostMultiplier;

        // 2. Get cost of the filter, if any.
        double filterCpu = hasFilter ? CostUtils.adjustCpuForConstrainedScan(scanCpu) : 0;

        // 3. Get cost of the project taking into account the filter and number of expressions. Project never produces IO.
        double projectCpu = CostUtils.adjustCpuForConstrainedScan(CostUtils.getProjectCpu(filterRowCount, projectCount));

        // 4. Cost of merging the rows into a sorted stream, it depends on the number of resulting rows and their size.
        // Note that no full sorting is performed, sorted row streams are only merged.
        double sortCpu = requiresSort ? CostUtils.INDEX_SCAN_CPU_MULTIPLIER_SORTED_ORDER_REQUIRED * projectCpu : 0;

        // 5. Finally, return sum of both scan and project.
        return planner.getCostFactory().makeCost(
                filterRowCount,
                scanCpu + filterCpu + projectCpu + sortCpu,
                0
        );
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("index", index.getName())
                .item("indexExp", indexExp)
                .item("remainderExp", remainderExp)
                .itemIf("requiresSort", requiresSort(), requiresSort());
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IndexScanMapPhysicalRel(getCluster(), traitSet, getTable(), index, indexFilter, indexExp, remainderExp);
    }


    /**
     * Return copy of this RelNode without collation - without guaranteed order.
     * <p>
     * Sorted index scan can work in two modes because it is distributed scatter-gather operation:
     * <ol>
     *     <li>with guaranteed order which is defined by index columns. This mode requires gathering
     *     all results before further processing to guarantee the order</li>
     *     <li>without guaranteed order, if the order is not needed. This mode does not require
     *     gathering results so should be much more performant.</li>
     * </ol>
     * Some operations do not care about the order of input data (e.g. aggregations) so they can
     * benefit from this mode of operation.
     */
    public RelNode withoutCollation() {
        return copy(traitSet.replace(RelCollations.EMPTY), getInputs());
    }
}
