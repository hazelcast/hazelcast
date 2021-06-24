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

package com.hazelcast.sql.impl.calcite.opt.logical;

import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

/**
 * Logical optimization rules.
 */
public final class LogicalRules {
    private LogicalRules() {
        // No-op.
    }

    public static RuleSet getRuleSet() {
        return RuleSets.ofList(
                // Filter rules.
                FilterMergeRule.INSTANCE,
                FilterProjectTransposeRule.INSTANCE,
                FilterIntoScanLogicalRule.INSTANCE,

                // Project rules.
                ProjectMergeRule.INSTANCE,
                ProjectRemoveRule.INSTANCE,
                ProjectFilterTransposeRule.INSTANCE,
                ProjectJoinTransposeRule.INSTANCE,
                ProjectIntoScanLogicalRule.INSTANCE,

                // Values rules
                PruneEmptyRules.PROJECT_INSTANCE,
                PruneEmptyRules.FILTER_INSTANCE,

                // Converter rules
                MapScanLogicalRule.INSTANCE,
                FilterLogicalRule.INSTANCE,
                ProjectLogicalRule.INSTANCE,
                ValuesLogicalRule.INSTANCE,

                SortLogicalRule.INSTANCE
        );
    }
}
