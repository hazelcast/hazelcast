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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.List;

/**
 * Logical rule that pushes down a {@link Filter} into a {@link TableScan} to allow for constrained scans.
 * See {@link HazelcastTable} for more information about constrained scans.
 * <p>
 * Before:
 * <pre>
 * LogicalFilter[filter=exp1]
 *     LogicalScan[table[filter=exp2]]
 * </pre>
 * After:
 * <pre>
 * LogicalScan[table[filter=exp1 AND exp2]]
 * </pre>
 */
public final class FilterIntoScanLogicalRule extends RelOptRule {

    public static final FilterIntoScanLogicalRule INSTANCE = new FilterIntoScanLogicalRule();

    private FilterIntoScanLogicalRule() {
        super(
                operand(LogicalFilter.class, operand(LogicalTableScan.class, none())),
                RelFactories.LOGICAL_BUILDER,
                FilterIntoScanLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        TableScan scan = call.rel(1);

        HazelcastTable originalTable = OptUtils.extractHazelcastTable(scan);

        // Remap the condition to the original TableScan columns.
        RexNode newCondition = remapCondition(originalTable, filter.getCondition());

        // Compose the conjunction with the old filter if needed.
        RexNode originalCondition = originalTable.getFilter();

        if (originalCondition != null) {
            List<RexNode> nodes = new ArrayList<>(2);
            nodes.add(originalCondition);
            nodes.add(newCondition);

            newCondition = RexUtil.composeConjunction(scan.getCluster().getRexBuilder(), nodes, true);
        }

        // Create a scan with a new filter.
        LogicalTableScan newScan = OptUtils.createLogicalScan(
                scan,
                originalTable.withFilter(newCondition)
        );

        call.transformTo(newScan);
    }

    /**
     * Remaps the column indexes referenced in the {@code Filter} to match the original indexed used by {@code TableScan}.
     * <p>
     * Consider the following query: "SELECT f1, f0 FROM t WHERE f0 > ?" for the table {@code t[f0, f1]}
     * <p>
     * The original tree before optimization:
     * <pre>
     * LogicalFilter[$1>?]                                  // f0 is referenced as $1
     *   LogicalProject[$1, $0]                             // f1, f0
     *     LogicalScan[table=t[projects=[0, 1]]]            // f0, f1
     * </pre>
     * After project pushdown:
     * <pre>
     * LogicalFilter[$1>?]                                  // f0 is referenced as $1
     *   LogicalScan[table=t[projects=[1, 0]]]              // f1, f0
     * </pre>
     * After filter pushdown:
     * <pre>
     * LogicalScan[table=t[projects=[1, 0], filter=[$0>?]]] // f0 is referenced as $0
     * </pre>
     *
     * @param originalHazelcastTable  The original table from the {@code TableScan} before the pushdown
     * @param originalFilterCondition The original condition from the {@code Filter}.
     * @return New condition that is going to be pushed down to a {@code TableScan}.
     */
    private static RexNode remapCondition(HazelcastTable originalHazelcastTable, RexNode originalFilterCondition) {
        List<Integer> projects = originalHazelcastTable.getProjects();

        Mapping mapping = Mappings.source(projects, originalHazelcastTable.getOriginalFieldCount());

        return RexUtil.apply(mapping, originalFilterCondition);
    }
}
