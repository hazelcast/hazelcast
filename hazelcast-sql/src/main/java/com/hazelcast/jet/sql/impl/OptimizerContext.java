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

package com.hazelcast.jet.sql.impl;

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.sql.impl.opt.cost.CostFactory;
import com.hazelcast.jet.sql.impl.opt.distribution.DistributionTraitDef;
import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMdBoundedness;
import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMdRowCount;
import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMdWatermarkedFields;
import com.hazelcast.jet.sql.impl.parse.QueryConvertResult;
import com.hazelcast.jet.sql.impl.parse.QueryConverter;
import com.hazelcast.jet.sql.impl.parse.QueryParseResult;
import com.hazelcast.jet.sql.impl.parse.QueryParser;
import com.hazelcast.jet.sql.impl.schema.HazelcastCalciteCatalogReader;
import com.hazelcast.jet.sql.impl.schema.HazelcastSchema;
import com.hazelcast.jet.sql.impl.schema.HazelcastSchemaUtils;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.schema.IMapResolver;
import com.hazelcast.sql.impl.schema.SqlCatalog;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.HazelcastRootCalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RuleSet;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * Optimizer context which holds the whole environment for the given optimization session.
 * Should not be re-used between optimization sessions.
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public final class OptimizerContext {

    private static final ThreadLocal<OptimizerContext> THREAD_CONTEXT = new ThreadLocal<>();

    private static final RelMetadataProvider METADATA_PROVIDER = ChainedRelMetadataProvider.of(ImmutableList.of(
            HazelcastRelMdRowCount.SOURCE,
            HazelcastRelMdBoundedness.SOURCE,
            HazelcastRelMdWatermarkedFields.SOURCE,
            DefaultRelMetadataProvider.INSTANCE
    ));

    private static final CalciteConnectionConfig CONNECTION_CONFIG = CalciteConfiguration.DEFAULT.toConnectionConfig();

    private final HazelcastRelOptCluster cluster;
    private final QueryParser parser;
    private final QueryConverter converter;
    private final QueryPlanner planner;

    private final Deque<String> viewExpansionStack = new ArrayDeque<>();

    private OptimizerContext(
            HazelcastRelOptCluster cluster,
            QueryParser parser,
            QueryConverter converter,
            QueryPlanner planner
    ) {
        this.cluster = cluster;
        this.parser = parser;
        this.converter = converter;
        this.planner = planner;
    }

    /**
     * Create the optimization context.
     *
     * @param searchPaths Search paths to support "current schema" feature.
     * @param memberCount Number of member that is important for distribution-related rules and converters.
     * @return Context.
     */
    public static OptimizerContext create(
            SqlCatalog schema,
            List<List<String>> searchPaths,
            List<Object> arguments,
            int memberCount,
            IMapResolver iMapResolver
    ) {
        // Resolve tables.
        HazelcastSchema rootSchema = HazelcastSchemaUtils.createRootSchema(schema);

        return create(rootSchema, searchPaths, arguments, memberCount, iMapResolver);
    }

    public static OptimizerContext create(
            HazelcastSchema rootSchema,
            List<List<String>> schemaPaths,
            List<Object> arguments,
            int memberCount,
            IMapResolver iMapResolver
    ) {
        DistributionTraitDef distributionTraitDef = new DistributionTraitDef(memberCount);

        Prepare.CatalogReader catalogReader = createCatalogReader(rootSchema, schemaPaths);
        HazelcastSqlValidator validator = new HazelcastSqlValidator(catalogReader, arguments, iMapResolver);
        VolcanoPlanner volcanoPlanner = createPlanner(distributionTraitDef);
        HazelcastRelOptCluster cluster = createCluster(volcanoPlanner, distributionTraitDef);

        QueryParser parser = new QueryParser(validator);
        QueryConverter converter = new QueryConverter(validator, catalogReader, cluster);
        QueryPlanner planner = new QueryPlanner(volcanoPlanner);

        return new OptimizerContext(cluster, parser, converter, planner);
    }

    public static void setThreadContext(OptimizerContext context) {
        THREAD_CONTEXT.set(context);
    }

    public static OptimizerContext getThreadContext() {
        return THREAD_CONTEXT.get();
    }

    /**
     * Parse SQL statement.
     *
     * @param sql SQL string.
     * @return SQL tree.
     */
    public QueryParseResult parse(String sql) {
        return parser.parse(sql);
    }

    /**
     * Perform initial conversion of an SQL tree to a relational tree.
     *
     * @param node Query parse result.
     * @return Relational tree.
     */
    public QueryConvertResult convert(SqlNode node) {
        return converter.convert(node);
    }

    public RelNode convertView(SqlNode node) {
        return converter.convertView(node);
    }

    /**
     * Apply the given rules to the node.
     *
     * @param node     Node.
     * @param rules    Rules.
     * @param traitSet Required trait set.
     * @return Optimized node.
     */
    public RelNode optimize(RelNode node, RuleSet rules, RelTraitSet traitSet) {
        return planner.optimize(node, rules, traitSet);
    }

    public void setParameterMetadata(QueryParameterMetadata parameterMetadata) {
        cluster.setParameterMetadata(parameterMetadata);
    }

    public void setRequiresJob(boolean requiresJob) {
        cluster.setRequiresJob(requiresJob);
    }

    private static Prepare.CatalogReader createCatalogReader(HazelcastSchema rootSchema, List<List<String>> searchPaths) {
        assert searchPaths != null;

        return new HazelcastCalciteCatalogReader(
                new HazelcastRootCalciteSchema(rootSchema),
                searchPaths,
                HazelcastTypeFactory.INSTANCE,
                CONNECTION_CONFIG);
    }

    private static VolcanoPlanner createPlanner(DistributionTraitDef distributionTraitDef) {
        VolcanoPlanner planner = new VolcanoPlanner(
                CostFactory.INSTANCE,
                Contexts.of(CONNECTION_CONFIG)
        );

        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        planner.addRelTraitDef(distributionTraitDef);

        return planner;
    }

    private static HazelcastRelOptCluster createCluster(
            VolcanoPlanner planner,
            DistributionTraitDef distributionTraitDef
    ) {
        HazelcastRelOptCluster cluster = HazelcastRelOptCluster.create(
                planner,
                HazelcastRexBuilder.INSTANCE,
                distributionTraitDef
        );

        // Wire up custom metadata providers.
        cluster.setMetadataProvider(JaninoRelMetadataProvider.of(METADATA_PROVIDER));

        return cluster;
    }

    public Deque<String> getViewExpansionStack() {
        return viewExpansionStack;
    }
}
