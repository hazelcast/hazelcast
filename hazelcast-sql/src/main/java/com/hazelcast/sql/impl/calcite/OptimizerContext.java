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

package com.hazelcast.sql.impl.calcite;

import com.google.common.collect.ImmutableList;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.opt.QueryPlanner;
import com.hazelcast.sql.impl.calcite.opt.cost.CostFactory;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTraitDef;
import com.hazelcast.sql.impl.calcite.opt.metadata.HazelcastRelMdRowCount;
import com.hazelcast.sql.impl.calcite.parse.QueryConvertResult;
import com.hazelcast.sql.impl.calcite.parse.QueryConverter;
import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.calcite.parse.QueryParser;
import com.hazelcast.sql.impl.calcite.schema.HazelcastCalciteCatalogReader;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchemaUtils;
import com.hazelcast.sql.impl.calcite.validate.HazelcastJetSqlConformance;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlConformance;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
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
import org.apache.calcite.tools.RuleSet;

import java.util.List;

/**
 * Optimizer context which holds the whole environment for the given optimization session.
 * Should not be re-used between optimization sessions.
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public final class OptimizerContext {

    private static final RelMetadataProvider METADATA_PROVIDER = ChainedRelMetadataProvider.of(ImmutableList.of(
            HazelcastRelMdRowCount.SOURCE,
            DefaultRelMetadataProvider.INSTANCE
    ));

    private static final CalciteConnectionConfig CONNECTION_CONFIG = CalciteConfiguration.DEFAULT.toConnectionConfig();

    private final HazelcastRelOptCluster cluster;
    private final QueryParser parser;
    private final QueryConverter converter;
    private final QueryPlanner planner;

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
            SqlBackend sqlBackend
    ) {
        // Resolve tables.
        HazelcastSchema rootSchema = HazelcastSchemaUtils.createRootSchema(schema);

        return create(rootSchema, searchPaths, arguments, memberCount, sqlBackend);
    }

    public static OptimizerContext create(
            HazelcastSchema rootSchema,
            List<List<String>> schemaPaths,
            List<Object> arguments,
            int memberCount,
            SqlBackend sqlBackend
    ) {
        DistributionTraitDef distributionTraitDef = new DistributionTraitDef(memberCount);

        HazelcastSqlConformance conformance = HazelcastJetSqlConformance.INSTANCE;
        HazelcastTypeFactory typeFactory = HazelcastTypeFactory.INSTANCE;
        Prepare.CatalogReader catalogReader = createCatalogReader(typeFactory, CONNECTION_CONFIG, rootSchema, schemaPaths);
        VolcanoPlanner volcanoPlanner = createPlanner(CONNECTION_CONFIG, distributionTraitDef);
        HazelcastRelOptCluster cluster = createCluster(volcanoPlanner, typeFactory, distributionTraitDef);

        QueryParser parser = new QueryParser(typeFactory, catalogReader, conformance, arguments, sqlBackend);
        QueryConverter converter = new QueryConverter(catalogReader, cluster);
        QueryPlanner planner = new QueryPlanner(volcanoPlanner);

        return new OptimizerContext(cluster, parser, converter, planner);
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
     * @param parseResult Query parse result.
     * @return Relational tree.
     */
    public QueryConvertResult convert(QueryParseResult parseResult) {
        return converter.convert(parseResult);
    }

    public void setParameterMetadata(QueryParameterMetadata parameterMetadata) {
        cluster.setParameterMetadata(parameterMetadata);
    }


    public void setRequiresJob(boolean requiresJob) {
        cluster.setRequiresJob(requiresJob);
    }

    // For unit testing only
    public HazelcastRelOptCluster getCluster() {
        return cluster;
    }

    // For unit testing only
    public Prepare.CatalogReader getCatalogReader() {
        return converter.getCatalogReader();
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

    private static Prepare.CatalogReader createCatalogReader(
            HazelcastTypeFactory typeFactory,
            CalciteConnectionConfig config,
            HazelcastSchema rootSchema,
            List<List<String>> searchPaths
    ) {
        assert searchPaths != null;

        return new HazelcastCalciteCatalogReader(
                new HazelcastRootCalciteSchema(rootSchema),
                searchPaths,
                typeFactory,
                config
        );
    }

    private static VolcanoPlanner createPlanner(CalciteConnectionConfig config, DistributionTraitDef distributionTraitDef) {
        VolcanoPlanner planner = new VolcanoPlanner(
                CostFactory.INSTANCE,
                Contexts.of(config)
        );

        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        planner.addRelTraitDef(distributionTraitDef);

        return planner;
    }

    private static HazelcastRelOptCluster createCluster(
            VolcanoPlanner planner,
            HazelcastTypeFactory typeFactory,
            DistributionTraitDef distributionTraitDef
    ) {
        HazelcastRelOptCluster cluster = HazelcastRelOptCluster.create(
                planner,
                new HazelcastRexBuilder(typeFactory),
                distributionTraitDef
        );

        // Wire up custom metadata providers.
        cluster.setMetadataProvider(JaninoRelMetadataProvider.of(METADATA_PROVIDER));

        return cluster;
    }
}
