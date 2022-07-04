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

import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public final class LogicalRules {

    private LogicalRules() {
    }

    public static RuleSet getRuleSet() {
        return RuleSets.ofList(
                // Scan rules
                FullScanLogicalRule.INSTANCE,
                FunctionLogicalRules.SPECIFIC_FUNCTION_INSTANCE,
                FunctionLogicalRules.DYNAMIC_FUNCTION_INSTANCE,

                // Calc rules
                CalcLogicalRule.INSTANCE,
                CalcIntoScanLogicalRule.INSTANCE,
                CalcMergeRule.INSTANCE,
                CoreRules.CALC_REMOVE,
                CoreRules.CALC_REDUCE_EXPRESSIONS,
                // We need it to transpose RIGHT JOIN to the LEFT JOIN
                CoreRules.PROJECT_TO_CALC,
                SlidingWindowCalcSplitLogicalRule.STREAMING_FILTER_TRANSPOSE,

                // Windowing rules
                WatermarkRules.IMPOSE_ORDER_INSTANCE,
                WatermarkRules.WATERMARK_INTO_SCAN_INSTANCE,
                FunctionLogicalRules.WINDOW_FUNCTION_INSTANCE,
                SlidingWindowDropLateItemsMergeRule.INSTANCE,

                // Aggregate rules
                AggregateLogicalRule.INSTANCE,

                // Sort rules
                SortLogicalRule.INSTANCE,

                // Join rules
                JoinLogicalRule.INSTANCE,
                CoreRules.JOIN_REDUCE_EXPRESSIONS,
//                STREAMING_JOIN_TRANSPOSE,

                // Union rules
                PruneEmptyRules.UNION_INSTANCE,
                CoreRules.UNION_REMOVE,
                CoreRules.UNION_PULL_UP_CONSTANTS,
                UnionDropLateItemsTransposeRule.INSTANCE,
                UnionLogicalRule.INSTANCE,

                // Value rules
                ValuesLogicalRules.CONVERT_INSTANCE,
                ValuesLogicalRules.CALC_INSTANCE,
                ValuesLogicalRules.UNION_INSTANCE,

                // DML rules
                TableModifyLogicalRule.INSTANCE,
                InsertLogicalRule.INSTANCE,
                SinkLogicalRule.INSTANCE,
                UpdateLogicalRules.SCAN_INSTANCE,
                UpdateLogicalRules.VALUES_INSTANCE,
                DeleteLogicalRule.INSTANCE,

                // imap-by-key access optimization rules
                SelectByKeyMapLogicalRules.INSTANCE,
                InsertMapLogicalRule.INSTANCE,
                SinkMapLogicalRule.INSTANCE,
                UpdateByKeyMapLogicalRule.INSTANCE,
                DeleteByKeyMapLogicalRule.INSTANCE
        );
    }
}
