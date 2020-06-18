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
import com.hazelcast.sql.impl.JetSqlBackend;
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
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlConformance;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.schema.TableResolver;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.HazelcastRootCalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.RuleSet;

import javax.annotation.Nullable;
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

    // for testing purposes only
    private final SqlValidator validator;

    private OptimizerContext(
        QueryParser parser,
        QueryConverter converter,
        QueryPlanner planner,
        SqlValidator validator
    ) {
        this.parser = parser;
        this.converter = converter;
        this.planner = planner;
        this.validator = validator;
    }

    // for testing purposes only
    public SqlValidator getValidator() {
        return validator;
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
        JetSqlBackend jetSqlBackend,
        List<TableResolver> tableResolvers,
        List<List<String>> currentSearchPaths,
        int memberCount
    ) {
        // Prepare search paths.
        List<List<String>> searchPaths = HazelcastSchemaUtils.prepareSearchPaths(currentSearchPaths, tableResolvers);

        // Resolve tables.
        HazelcastSchema rootSchema = HazelcastSchemaUtils.createRootSchema(tableResolvers);

        return create(jetSqlBackend, rootSchema, searchPaths, memberCount);
    }

    public static OptimizerContext create(
        @Nullable JetSqlBackend jetSqlBackend,
        HazelcastSchema rootSchema,
        List<List<String>> schemaPaths,
        int memberCount
    ) {
        DistributionTraitDef distributionTraitDef = new DistributionTraitDef(memberCount);

        RelDataTypeFactory typeFactory = HazelcastTypeFactory.INSTANCE;
        Prepare.CatalogReader catalogReader = createCatalogReader(typeFactory, CONNECTION_CONFIG, rootSchema, schemaPaths);
        SqlValidator validator = createValidator(jetSqlBackend, typeFactory, catalogReader);
        VolcanoPlanner volcanoPlanner = createPlanner(CONNECTION_CONFIG, distributionTraitDef);
        HazelcastRelOptCluster cluster = createCluster(volcanoPlanner, typeFactory, distributionTraitDef);

        QueryParser parser = new QueryParser(validator);
        QueryConverter converter = new QueryConverter(catalogReader, validator, cluster);
        QueryPlanner planner = new QueryPlanner(volcanoPlanner);

        return new OptimizerContext(parser, converter, planner, validator);
    }

    /**
     * Parse SQL statement.
     *
     * @param sql SQL string.
     * @param jetBackendPresent True if Jet is on classpath and able to run queries.
     * @return SQL tree.
     */
    public QueryParseResult parse(String sql, boolean jetBackendPresent) {
        return parser.parse(sql, jetBackendPresent);
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
        RelDataTypeFactory typeFactory,
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

    private static SqlValidator createValidator(JetSqlBackend jetSqlBackend, RelDataTypeFactory typeFactory,
                                                CatalogReader catalogReader) {
        SqlOperatorTable opTab = ChainedSqlOperatorTable.of(
            HazelcastSqlOperatorTable.instance(),
            SqlStdOperatorTable.instance()
        );

        if (jetSqlBackend != null) {
            return (SqlValidator) jetSqlBackend.createValidator(
                opTab,
                catalogReader,
                typeFactory,
                HazelcastSqlConformance.INSTANCE
            );
        } else {
            return new HazelcastSqlValidator(
                opTab,
                catalogReader,
                typeFactory,
                HazelcastSqlConformance.INSTANCE
            );
        }
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
        RelDataTypeFactory typeFactory,
        DistributionTraitDef distributionTraitDef
    ) {
        HazelcastRexBuilder rexBuilder = new HazelcastRexBuilder(typeFactory);
        HazelcastRelOptCluster cluster = HazelcastRelOptCluster.create(
            planner,
            rexBuilder,
            distributionTraitDef
        );

        // Wire up custom metadata providers.
        cluster.setMetadataProvider(JaninoRelMetadataProvider.of(METADATA_PROVIDER));

        return cluster;
    }
}
