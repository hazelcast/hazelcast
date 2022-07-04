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

package com.hazelcast.jet.sql.impl.parse;

import com.hazelcast.jet.sql.impl.HazelcastSqlToRelConverter;
import com.hazelcast.jet.sql.impl.opt.logical.CalcMergeRule;
import com.hazelcast.jet.sql.impl.schema.HazelcastViewExpander;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import javax.annotation.Nullable;

/**
 * Converts a parse tree into a relational tree.
 */
public class QueryConverter {
    public static final SqlToRelConverter.Config CONFIG;

    /**
     * Whether to expand subqueries. When set to {@code false}, subqueries are left as is in the form of
     * {@link org.apache.calcite.rex.RexSubQuery}. Otherwise they are expanded into {@link org.apache.calcite.rel.core.Correlate}
     * instances.
     * Do not enable this because you may run into https://issues.apache.org/jira/browse/CALCITE-3484. Instead, subquery
     * elimination rules are executed during logical planning. In addition, resulting plans are slightly better that those
     * produced by "expand" flag.
     */
    private static final boolean EXPAND = false;

    /**
     * Whether to trim unused fields. The trimming is needed after subquery elimination.
     */
    private static final boolean TRIM_UNUSED_FIELDS = true;

    /**
     * Increase the maximum number of elements in the RHS to convert the IN operator to a sequence of OR comparisons.
     */
    private static final int HAZELCAST_IN_ELEMENTS_THRESHOLD = 10_000;

    static {
        CONFIG = SqlToRelConverter.config()
                .withExpand(EXPAND)
                .withInSubQueryThreshold(HAZELCAST_IN_ELEMENTS_THRESHOLD)
                .withTrimUnusedFields(TRIM_UNUSED_FIELDS);
    }

    private final SqlValidator validator;
    private final Prepare.CatalogReader catalogReader;
    private final RelOptCluster cluster;
    private final HazelcastViewExpander viewExpander;

    public QueryConverter(SqlValidator validator, Prepare.CatalogReader catalogReader, HazelcastRelOptCluster cluster) {
        this.validator = validator;
        this.catalogReader = catalogReader;
        this.cluster = cluster;
        this.viewExpander = new HazelcastViewExpander(validator, catalogReader, cluster);
    }

    public QueryConvertResult convert(SqlNode node) {
        SqlToRelConverter converter = createSqlToRelConverter();

        // 1. Perform initial conversion.
        RelRoot root = converter.convertQuery(node, false, true);

        // 2. Remove subquery expressions, converting them to Correlate nodes.
        RelNode relNoSubqueries = performUnconditionalRewrites(root.project());

        // 3. Perform decorrelation, i.e. rewrite a nested loop where the right side depends on the value of the left side,
        // to a variation of joins, semijoins and aggregations, which could be executed much more efficiently.
        // See "Unnesting Arbitrary Queries", Thomas Neumann and Alfons Kemper.
        RelNode result = converter.decorrelate(node, relNoSubqueries);

        // 4. The side effect of subquery rewrite and decorrelation in Apache Calcite is a number of unnecessary fields,
        // primarily in projections. This steps removes unused fields from the tree.
        //
        // Due to a (possible) Calcite bug, we're not doing it if there are nested EXISTS calls.
        // The bug is likely in decorrelation which produces LogicalAggregate with 0 output columns.
        if (!hasNestedExists(root.rel)) {
            result = converter.trimUnusedFields(true, result);
        }

        // 5. Transform projects and filters to Calc.
        result = transformProjectAndFilterIntoCalc(result);

        // 6. Collect original field names.
        return new QueryConvertResult(result, Pair.right(root.fields));
    }

    public RelNode convertView(SqlNode node) {
        HazelcastSqlToRelConverter sqlToRelConverter = createSqlToRelConverter();

        final RelRoot root = sqlToRelConverter.convertQuery(node, true, true);
        final RelRoot root2 = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));

        final RelBuilder relBuilder = QueryConverter.CONFIG.getRelBuilderFactory().create(cluster, null);
        RelRoot root3 = root2.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
        return root3.project();
    }

    private HazelcastSqlToRelConverter createSqlToRelConverter() {
        return new HazelcastSqlToRelConverter(viewExpander, validator, catalogReader, cluster,
                StandardConvertletTable.INSTANCE, QueryConverter.CONFIG);
    }

    /**
     * Initial query optimization step. It includes
     * <ul>
     * <li>
     *  Correlated subqueries elimination, converting them to various forms of joins.
     *  It is used instead of "expand" flag due to bugs in Calcite (see {@link #EXPAND}).
     * </li>
     * <li>
     *  Transformation of distinct UNION to UNION ALL, merging the neighboring UNION relations.
     * </li>
     *
     * </ul>
     *
     * @param rel Initial relation.
     * @return Resulting relation.
     */
    private static RelNode performUnconditionalRewrites(RelNode rel) {
        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();

        // Correlated subqueries elimination rules
        hepProgramBuilder.addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE);
        hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE);
        hepProgramBuilder.addRuleInstance(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);

        // Union optimization rules
        hepProgramBuilder.addRuleInstance(CoreRules.UNION_MERGE);
        hepProgramBuilder.addRuleInstance(CoreRules.UNION_TO_DISTINCT);

        HepPlanner planner = new HepPlanner(
                hepProgramBuilder.build(),
                Contexts.empty(),
                true,
                null,
                RelOptCostImpl.FACTORY
        );

        planner.setRoot(rel);
        return planner.findBestExp();
    }

    /**
     * Second unconditional query optimization step. It includes
     * <ul>
     * <li>
     *  Transformation of {@link Project} and {@link Filter} relations to {@link Calc}
     * </li>
     * </ul>
     *
     * @param rel Initial relation.
     * @return Resulting relation.
     */
    private static RelNode transformProjectAndFilterIntoCalc(RelNode rel) {
        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();

        // Filter rules
        hepProgramBuilder.addRuleInstance(CoreRules.FILTER_MERGE);
        hepProgramBuilder.addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE);
        hepProgramBuilder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
        hepProgramBuilder.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);
        hepProgramBuilder.addRuleInstance(PruneEmptyRules.FILTER_INSTANCE);

        // Project rules
        hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_MERGE);
        hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_REMOVE);
        hepProgramBuilder.addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE);

        // Join rules
        hepProgramBuilder.addRuleInstance(CoreRules.JOIN_REDUCE_EXPRESSIONS);
        hepProgramBuilder.addRuleInstance(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER);

        // Calc rules
        hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_TO_CALC);
        hepProgramBuilder.addRuleInstance(CoreRules.FILTER_TO_CALC);
        hepProgramBuilder.addRuleInstance(CalcMergeRule.INSTANCE);
        hepProgramBuilder.addRuleInstance(CoreRules.CALC_REMOVE);

        // TODO: [sasha] Move more rules to unconditionally rewrite rel tree.
        HepPlanner planner = new HepPlanner(
                hepProgramBuilder.build(),
                Contexts.empty(),
                true,
                null,
                RelOptCostImpl.FACTORY
        );

        planner.setRoot(rel);
        return planner.findBestExp();
    }

    private static boolean hasNestedExists(RelNode root) {
        class NestedExistsFinder extends RelVisitor {
            private boolean found;
            private int depth;

            @Override
            public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                if (node instanceof LogicalFilter) {
                    RexSubQuery exists = getExists((LogicalFilter) node);
                    if (exists != null) {
                        found |= depth > 0;
                        depth++;
                        go(exists.rel);
                        depth--;
                    }
                }
                super.visit(node, ordinal, parent);
            }

            private boolean find() {
                go(root);
                return found;
            }

            private RexSubQuery getExists(LogicalFilter filter) {
                RexSubQuery[] existsSubQuery = {null};

                filter.getCondition().accept(new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitSubQuery(RexSubQuery subQuery) {
                        if (subQuery.getKind() == SqlKind.EXISTS) {
                            existsSubQuery[0] = subQuery;
                        }
                        return super.visitSubQuery(subQuery);
                    }
                });

                return existsSubQuery[0];
            }
        }

        return new NestedExistsFinder().find();
    }
}
