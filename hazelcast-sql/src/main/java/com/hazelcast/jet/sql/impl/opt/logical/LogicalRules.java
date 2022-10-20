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

import com.hazelcast.jet.sql.impl.opt.common.CalcIntoScanRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public final class LogicalRules {

    private LogicalRules() {
    }

    public static RuleSet getRuleSet() {
        return RuleSets.ofList(
                // We need it to transpose RIGHT JOIN to the LEFT JOIN
//                CoreRules.PROJECT_TO_CALC,
                CalcReduceExprRule.INSTANCE,

                // Scan rules
                FullScanLogicalRule.INSTANCE,
                FunctionLogicalRules.SPECIFIC_FUNCTION_INSTANCE,
                FunctionLogicalRules.DYNAMIC_FUNCTION_INSTANCE,

                // Watermark rules
                WatermarkRules.IMPOSE_ORDER_INSTANCE,
                WatermarkRules.WATERMARK_INTO_SCAN_INSTANCE,

                // Windowing rules
                FunctionLogicalRules.WINDOW_FUNCTION_INSTANCE,

                // Value rules
                ValuesLogicalRules.CONVERT_INSTANCE,
                ValuesLogicalRules.CALC_INSTANCE,
                ValuesLogicalRules.UNION_INSTANCE,

                // Union rules
                PruneEmptyRules.UNION_INSTANCE,
                CoreRules.UNION_REMOVE,
                CoreRules.UNION_PULL_UP_CONSTANTS,
                UnionLogicalRule.INSTANCE,

                // Calc rules
                CalcLogicalRule.INSTANCE,
                CoreRules.CALC_REMOVE,
                CalcMergeRule.INSTANCE,
                CalcIntoScanRule.INSTANCE,

                // Transposition rules
                SlidingWindowCalcSplitLogicalRule.STREAMING_FILTER_TRANSPOSE,
                CalcDropLateItemsTransposeRule.INSTANCE,
                UnionDropLateItemsTransposeRule.INSTANCE,
                SlidingWindowDropLateItemsMergeRule.INSTANCE,

                // Aggregate rules
                AggregateLogicalRule.INSTANCE,

                // Join rules
                JoinLogicalRule.INSTANCE,
//                STREAMING_JOIN_TRANSPOSE,

                // Sort rules
                SortLogicalRule.INSTANCE,

                // DML rules
                InsertLogicalRule.INSTANCE,
                SinkLogicalRule.INSTANCE,
                UpdateLogicalRules.SCAN_INSTANCE,
                UpdateLogicalRules.VALUES_INSTANCE,
                DeleteLogicalRule.INSTANCE,
                TableModifyLogicalRule.INSTANCE,

                // imap-by-key access optimization rules
                InsertMapLogicalRule.INSTANCE,
                SinkMapLogicalRule.INSTANCE,
                UpdateByKeyMapLogicalRule.INSTANCE,
                DeleteByKeyMapLogicalRule.INSTANCE
        );
    }
}
