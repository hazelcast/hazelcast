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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.opt.FullScan;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.cost.CostUtils;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;
import java.util.List;

import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.impl.opt.cost.CostUtils.TABLE_SCAN_CPU_MULTIPLIER;

public class FullScanPhysicalRel extends FullScan implements PhysicalRel {

    FullScanPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider,
            int watermarkedColumnIndex

    ) {
        super(cluster, traitSet, table, eventTimePolicyProvider, watermarkedColumnIndex);
    }

    public Expression<Boolean> filter(QueryParameterMetadata parameterMetadata) {
        PlanNodeSchema schema = OptUtils.schema(getTable());

        RexNode filter = getTable().unwrap(HazelcastTable.class).getFilter();

        return filter(schema, filter, parameterMetadata);
    }

    public List<Expression<?>> projection(QueryParameterMetadata parameterMetadata) {
        PlanNodeSchema schema = OptUtils.schema(getTable());

        HazelcastTable table = getTable().unwrap(HazelcastTable.class);

        return project(schema, table.getProjects(), parameterMetadata);
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        List<QueryDataType> fieldTypes = toList(projection(parameterMetadata), Expression::getType);
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onFullScan(this);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        HazelcastTable table = getTable().unwrap(HazelcastTable.class);
        double totalRowCount = table.getStatistic().getRowCount() != null
                ? table.getTotalRowCount()
                : getTable().getRowCount();

        double filterRowCount = totalRowCount;

        if (table.getFilter() != null) {
            filterRowCount = CostUtils.adjustFilteredRowCount(totalRowCount, RelMdUtil.guessSelectivity(table.getFilter()));
        }

        return computeSelfCost(
                planner,
                totalRowCount,
                table.getFilter() != null,
                filterRowCount,
                table.getProjects().size()
        );
    }

    private static RelOptCost computeSelfCost(
            RelOptPlanner planner,
            double scanRowCount,
            boolean hasFilter,
            double filterRowCount,
            int projectCount
    ) {
        // 1. Get cost of the scan itself.
        double scanCpu = scanRowCount * TABLE_SCAN_CPU_MULTIPLIER;

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
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new FullScanPhysicalRel(getCluster(), traitSet, getTable(), eventTimePolicyProvider(),
                watermarkedColumnIndex());
    }
}
