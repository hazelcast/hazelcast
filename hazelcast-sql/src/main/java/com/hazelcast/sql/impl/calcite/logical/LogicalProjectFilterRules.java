/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.logical;

import com.hazelcast.sql.impl.calcite.logical.rule.FilterIntoScanLogicalRule;
import com.hazelcast.sql.impl.calcite.logical.rule.ProjectIntoScanLogicalRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;

/**
 * Logical optimization rules for projects and filters.
 */
public final class LogicalProjectFilterRules {
    /** Rule which merges two nearby filters together. */
    public static final FilterMergeRule FILTER_MERGE_RULE = FilterMergeRule.INSTANCE;

    /** Rule which moves filter past project. */
    // TODO: Disallow "item" pushdown
    public static final FilterProjectTransposeRule FILTER_PROJECT_TRANSPOSE_RULE = FilterProjectTransposeRule.INSTANCE;

    /** Rule which merges filter and scan. */
    public static final FilterIntoScanLogicalRule FILTER_INTO_SCAN_RULE = FilterIntoScanLogicalRule.INSTANCE;

    /** Rule which removes unnecessary projects. */
    public static final ProjectRemoveRule PROJECT_REMOVE_RULE = ProjectRemoveRule.INSTANCE;

    /** Rule which moves project past filter. */
    // TODO: Disallow "item" pushdown
    public static final ProjectFilterTransposeRule PROJECT_FILTER_TRANSPOSE_RULE = ProjectFilterTransposeRule.INSTANCE;

    /** Rule which moves project past join. */
    public static final ProjectJoinTransposeRule PROJECT_JOIN_TRANSPOSE_RULE = ProjectJoinTransposeRule.INSTANCE;

    /** Rule which merges project and scan. */
    public static final ProjectIntoScanLogicalRule PROJECT_INTO_SCAN_RULE = ProjectIntoScanLogicalRule.INSTANCE;

    private LogicalProjectFilterRules() {
        // No-op.
    }
}
