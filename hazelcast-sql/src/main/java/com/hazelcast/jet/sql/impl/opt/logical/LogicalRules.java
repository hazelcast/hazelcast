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

import com.hazelcast.sql.impl.calcite.opt.logical.FilterIntoScanLogicalRule;
import com.hazelcast.sql.impl.calcite.opt.logical.ProjectIntoScanLogicalRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule.FilterIntoJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public final class LogicalRules {

    private LogicalRules() {
    }

    public static RuleSet getRuleSet() {
        return RuleSets.ofList(
                // Filter rules
                FilterLogicalRule.INSTANCE,
                FilterMergeRule.INSTANCE,
                FilterProjectTransposeRule.INSTANCE,
                FilterIntoScanLogicalRule.INSTANCE,
                FilterAggregateTransposeRule.INSTANCE,
                FilterIntoJoinRule.FILTER_ON_JOIN,
                ReduceExpressionsRule.FILTER_INSTANCE,

                // Project rules
                ProjectLogicalRule.INSTANCE,
                ProjectMergeRule.INSTANCE,
                ProjectRemoveRule.INSTANCE,
                ProjectFilterTransposeRule.INSTANCE,
                ProjectIntoScanLogicalRule.INSTANCE,

                // Scan rules
                FullScanLogicalRule.INSTANCE,
                FullFunctionScanLogicalRules.SPECIFIC_FUNCTION_INSTANCE,
                FullFunctionScanLogicalRules.DYNAMIC_FUNCTION_INSTANCE,

                // Aggregate rules
                AggregateLogicalRule.INSTANCE,

                // Sort rules
                SortLogicalRule.INSTANCE,

                // Join rules
                JoinLogicalRule.INSTANCE,
                JoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER,
                ReduceExpressionsRule.JOIN_INSTANCE,

                // Union rules
                UnionLogicalRule.INSTANCE,

                // Value rules
                ValuesLogicalRules.CONVERT_INSTANCE,
                ValuesLogicalRules.FILTER_INSTANCE,
                ValuesLogicalRules.PROJECT_INSTANCE,
                ValuesLogicalRules.PROJECT_FILTER_INSTANCE,
                ValuesLogicalRules.UNION_INSTANCE,

                // DML rules
                InsertLogicalRule.INSTANCE,
                SinkLogicalRule.INSTANCE,
                UpdateLogicalRules.INSTANCE,
                UpdateLogicalRules.NOOP_INSTANCE,
                DeleteLogicalRule.INSTANCE,

                UpdateByKeyMapLogicalRule.INSTANCE,
                DeleteByKeyMapLogicalRule.INSTANCE,

                // Miscellaneous
                PruneEmptyRules.PROJECT_INSTANCE,
                PruneEmptyRules.FILTER_INSTANCE
        );
    }
}
