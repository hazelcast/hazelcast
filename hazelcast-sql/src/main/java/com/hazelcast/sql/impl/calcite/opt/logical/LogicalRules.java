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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.util.ArrayList;
import java.util.List;

/**
 * Logical optimization rules.
 */
public final class LogicalRules {
    /** Rule which attempts to push down the filter to a join condition. */
    private static final FilterJoinRule.FilterIntoJoinRule FILTER_PULL_RULE =
        new FilterJoinRule.FilterIntoJoinRule(true, RelFactories.LOGICAL_BUILDER, FilterPredicate.INSTANCE);

    /** Rule which attempts to push down the filter from the join condition. */
    private static final FilterJoinRule.JoinConditionPushRule CONDITION_PUSH_RULE =
        new FilterJoinRule.JoinConditionPushRule(RelFactories.LOGICAL_BUILDER, FilterPredicate.INSTANCE);

    /** Rule which moves expressions out of join condition: {Join <- Node} -> {Join <- Project <- Node}. */
    private static final JoinPushExpressionsRule EXPRESSIONS_PUSH_RULE = JoinPushExpressionsRule.INSTANCE;

    /** Rule which merges two nearby filters together. */
    private static final FilterMergeRule FILTER_MERGE_RULE = FilterMergeRule.INSTANCE;

    /** Rule which moves filter past project. */
    // TODO: Disallow "item" pushdown
    private static final FilterProjectTransposeRule FILTER_PROJECT_TRANSPOSE_RULE = FilterProjectTransposeRule.INSTANCE;

    /** Rule which merges filter and scan. */
    private static final FilterIntoScanLogicalRule FILTER_INTO_SCAN_RULE = FilterIntoScanLogicalRule.INSTANCE;

    /** Rule which removes unnecessary projects. */
    private static final ProjectRemoveRule PROJECT_REMOVE_RULE = ProjectRemoveRule.INSTANCE;

    /** Rule which moves project past filter. */
    // TODO: Disallow "item" pushdown
    private static final ProjectFilterTransposeRule PROJECT_FILTER_TRANSPOSE_RULE = ProjectFilterTransposeRule.INSTANCE;

    /** Rule which moves project past join. */
    private static final ProjectJoinTransposeRule PROJECT_JOIN_TRANSPOSE_RULE = ProjectJoinTransposeRule.INSTANCE;

    /** Rule which merges project and scan. */
    private static final ProjectIntoScanLogicalRule PROJECT_INTO_SCAN_RULE = ProjectIntoScanLogicalRule.INSTANCE;

    /** Rule which creates a semi-join from the Project-Join-Aggregate. */
    private static final SemiJoinRule SEMI_JOIN_PROJECT_RULE = SemiJoinRule.PROJECT;

    /** Rule which creates a semi-join from the Project-Join. */
    private static final SemiJoinRule SEMI_JOIN_JOIN_RULE = SemiJoinRule.JOIN;

    private LogicalRules() {
        // No-op.
    }

    public static RuleSet getConvertRuleSet() {
        return RuleSets.ofList(
            MapScanLogicalRule.INSTANCE,
            FilterLogicalRule.INSTANCE,
            ProjectLogicalRule.INSTANCE,
            AggregateLogicalRule.INSTANCE,
            SortLogicalRule.INSTANCE,
            JoinLogicalRule.INSTANCE
        );
    }

    public static RuleSet getRuleSet() {
        return RuleSets.ofList(
            // Join optimization rules.
            FILTER_PULL_RULE,
            CONDITION_PUSH_RULE,
            EXPRESSIONS_PUSH_RULE,

            // Filter and project rules.
            FILTER_MERGE_RULE,
            FILTER_PROJECT_TRANSPOSE_RULE,
            FILTER_INTO_SCAN_RULE,
            // TODO: ProjectMergeRule: https://jira.apache.org/jira/browse/CALCITE-2223
            PROJECT_FILTER_TRANSPOSE_RULE,
            PROJECT_JOIN_TRANSPOSE_RULE,
            PROJECT_REMOVE_RULE,
            PROJECT_INTO_SCAN_RULE,

            // TODO: Aggregate rules

            SEMI_JOIN_PROJECT_RULE,
            SEMI_JOIN_JOIN_RULE,

            // Convert Calcite node into Hazelcast nodes.
            // TODO: Should we extend converter here instead (see Flink)?
            MapScanLogicalRule.INSTANCE,
            FilterLogicalRule.INSTANCE,
            ProjectLogicalRule.INSTANCE,
            AggregateLogicalRule.INSTANCE,
            SortLogicalRule.INSTANCE,
            JoinLogicalRule.INSTANCE

            // TODO: Transitive closures: (a.a=b.b) AND (a=1) -> (a.a=b.b) AND (a=1) AND (b=1) -> pushdown to two tables, not one
        );
    }

    /**
     * Predicate to get only those filters which are eligible for {Filter -> Join} and {Join -> new Filter} pushdowns.
     */
    private static final class FilterPredicate implements  FilterJoinRule.Predicate {
        /** SIngleton instance. */
        private static final FilterPredicate INSTANCE = new FilterPredicate();

        @Override
        public boolean apply(Join join, JoinRelType joinType, RexNode exp) {
            // Filter could be pushed down only to INNER joins. To observe why this is so, consider two tables
            // r(r_attr) and s(s_attr), where both attributes are not null.
            // Query 1: This query may return r_attr with NULL value, if match is not found:
            // SELECT r_attr, s_attr FROM r LEFT JOIN s ON r.r_attr = s.s_attr
            // Query 2: This query is not equivalent to the Query 1, since it never returns NULL value for r_attr:
            // SELECT r_attr, s_attr FROM r LEFT JOIN s ON 1=1 WHERE r.r_attr = s.s_attr
            if (joinType != JoinRelType.INNER) {
                return true;
            }

            List<RexNode> tmpLeftKeys = new ArrayList<>();
            List<RexNode> tmpRightKeys = new ArrayList<>();
            List<RelDataTypeField> sysFields = new ArrayList<>();
            List<Integer> filterNulls = new ArrayList<>();

            RexNode remaining = RelOptUtil.splitJoinCondition(sysFields, join.getLeft(), join.getRight(),
                exp, tmpLeftKeys, tmpRightKeys, filterNulls, null);

            // This result will be "always true" if there is no disjunctions.
            return remaining.isAlwaysTrue();
        }
    }
}
