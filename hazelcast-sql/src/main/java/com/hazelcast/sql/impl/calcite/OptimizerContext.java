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

package com.hazelcast.sql.impl.calcite;

import com.google.common.collect.ImmutableList;
import com.hazelcast.sql.impl.calcite.opt.QueryPlanner;
import com.hazelcast.sql.impl.calcite.opt.cost.CostFactory;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTraitDef;
import com.hazelcast.sql.impl.calcite.opt.metadata.HazelcastRelMdRowCount;
import com.hazelcast.sql.impl.calcite.parse.CasingConfiguration;
import com.hazelcast.sql.impl.calcite.parse.QueryConvertResult;
import com.hazelcast.sql.impl.calcite.parse.QueryConverter;
import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.calcite.parse.QueryParser;
import com.hazelcast.sql.impl.calcite.schema.HazelcastCalciteCatalogReader;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchemaUtils;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlConformance;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.schema.TableResolver;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.HazelcastRootCalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
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

    private static final CalciteConnectionConfig CONNECTION_CONFIG = CasingConfiguration.DEFAULT.toConnectionConfig();

    private final QueryParser parser;
    private final QueryConverter converter;
    private final QueryPlanner planner;

    private OptimizerContext(
        QueryParser parser,
        QueryConverter converter,
        QueryPlanner planner
    ) {
        this.parser = parser;
        this.converter = converter;
        this.planner = planner;
    }

    /**
     * Create the optimization context.
     *
     * @param tableResolvers Resolver to collect information about tables.
     * @param currentSearchPaths Search paths to support "current schema" feature.
     * @param memberCount Number of member that is important for distribution-related rules and converters.
     * @return Context.
     */
    public static OptimizerContext create(
        List<TableResolver> tableResolvers,
        List<List<String>> currentSearchPaths,
        int memberCount
    ) {
        // Prepare search paths.
        List<List<String>> searchPaths = HazelcastSchemaUtils.prepareSearchPaths(currentSearchPaths, tableResolvers);

        // Resolve tables.
        HazelcastSchema rootSchema = HazelcastSchemaUtils.createRootSchema(tableResolvers);

        return create(rootSchema, searchPaths, memberCount);
    }

    public static OptimizerContext create(
        HazelcastSchema rootSchema,
        List<List<String>> schemaPaths,
        int memberCount
    ) {
        DistributionTraitDef distributionTraitDef = new DistributionTraitDef(memberCount);

        JavaTypeFactory typeFactory = new HazelcastTypeFactory();
        Prepare.CatalogReader catalogReader = createCatalogReader(typeFactory, CONNECTION_CONFIG, rootSchema, schemaPaths);
        SqlValidator validator = createValidator(typeFactory, catalogReader);
        VolcanoPlanner volcanoPlanner = createPlanner(CONNECTION_CONFIG, distributionTraitDef);
        HazelcastRelOptCluster cluster = createCluster(volcanoPlanner, typeFactory, distributionTraitDef);

        QueryParser parser = new QueryParser(validator);
        QueryConverter converter = new QueryConverter(catalogReader, validator, cluster);
        QueryPlanner planner = new QueryPlanner(volcanoPlanner);

        return new OptimizerContext(parser, converter, planner);
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
     * @param node SQL tree.
     * @return Relational tree.
     */
    public QueryConvertResult convert(SqlNode node) {
        return converter.convert(node);
    }

    /**
     * Apply the given rules to the node.
     *
     * @param node Node.
     * @param rules Rules.
     * @param traitSet Required trait set.
     * @return Optimized node.
     */
    public RelNode optimize(RelNode node, RuleSet rules, RelTraitSet traitSet) {
        return planner.optimize(node, rules, traitSet);
    }

    private static Prepare.CatalogReader createCatalogReader(
        JavaTypeFactory typeFactory,
        CalciteConnectionConfig config,
        HazelcastSchema rootSchema,
        List<List<String>> schemaPaths
    ) {
        return new HazelcastCalciteCatalogReader(
            new HazelcastRootCalciteSchema(rootSchema),
            schemaPaths,
            typeFactory,
            config
        );
    }

    private static SqlValidator createValidator(JavaTypeFactory typeFactory, Prepare.CatalogReader catalogReader) {
        SqlOperatorTable opTab = ChainedSqlOperatorTable.of(
            HazelcastSqlOperatorTable.instance(),
            SqlStdOperatorTable.instance()
        );

        return new HazelcastSqlValidator(
            opTab,
            catalogReader,
            typeFactory,
            HazelcastSqlConformance.INSTANCE
        );
    }

    private static VolcanoPlanner createPlanner(CalciteConnectionConfig config, DistributionTraitDef distributionTraitDef) {
        VolcanoPlanner planner = new VolcanoPlanner(
            CostFactory.INSTANCE,
            Contexts.of(config)
        );

        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(distributionTraitDef);

        return planner;
    }

    private static HazelcastRelOptCluster createCluster(
        VolcanoPlanner planner,
        JavaTypeFactory typeFactory,
        DistributionTraitDef distributionTraitDef
    ) {
        HazelcastRelOptCluster cluster = HazelcastRelOptCluster.create(
            planner,
            new RexBuilder(typeFactory),
            distributionTraitDef
        );

        // Wire up custom metadata providers.
        cluster.setMetadataProvider(JaninoRelMetadataProvider.of(METADATA_PROVIDER));

        return cluster;
    }
}
