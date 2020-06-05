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

package com.hazelcast.sql.impl.calcite.opt.logical;

import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.schema.HazelcastRelOptTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
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

public final class FilterIntoScanLogicalRule extends RelOptRule {
    public static final FilterIntoScanLogicalRule INSTANCE = new FilterIntoScanLogicalRule();

    private FilterIntoScanLogicalRule() {
        super(
            operand(LogicalFilter.class,
                operandJ(LogicalTableScan.class, null, OptUtils::isHazelcastTable, none())),
            RelFactories.LOGICAL_BUILDER,
            FilterIntoScanLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        TableScan scan = call.rel(1);

        HazelcastRelOptTable originalRelTable = (HazelcastRelOptTable) scan.getTable();
        HazelcastTable originalHazelcastTable = OptUtils.getHazelcastTable(scan);

        List<Integer> projects = originalHazelcastTable.getProjects();

        // Remap the new filter to the original table fields.
        // E.g. the filter [$0 IS NULL] for the projected table [2, 3] is converted to [$2 IS NULL].
        Mapping mapping = Mappings.source(projects, originalHazelcastTable.getOriginalFieldCount());

        RexNode newCondition = RexUtil.apply(mapping, filter.getCondition());

        // Compose the conjunction with the old filter if needed.
        RexNode originalCondition = originalHazelcastTable.getFilter();

        if (originalCondition != null) {
            List<RexNode> nodes = new ArrayList<>(2);
            nodes.add(originalCondition);
            nodes.add(newCondition);

            newCondition = RexUtil.composeConjunction(scan.getCluster().getRexBuilder(), nodes, true);
        }

        // Create a scan with a new filter.
        HazelcastTable newHazelcastTable = originalHazelcastTable.withFilter(newCondition);

        LogicalTableScan newScan = OptUtils.createLogicalScanWithNewTable(scan, originalRelTable, newHazelcastTable);

        call.transformTo(newScan);
    }
}
