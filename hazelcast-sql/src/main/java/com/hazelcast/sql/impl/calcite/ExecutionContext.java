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
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.cost.CostFactory;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTraitDef;
import com.hazelcast.sql.impl.calcite.opt.logical.LogicalRel;
import com.hazelcast.sql.impl.calcite.opt.logical.LogicalRules;
import com.hazelcast.sql.impl.calcite.opt.logical.RootLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.metadata.HazelcastRelMdRowCount;
import com.hazelcast.sql.impl.calcite.opt.physical.FilterPhysicalRule;
import com.hazelcast.sql.impl.calcite.opt.physical.MapScanPhysicalRule;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ProjectPhysicalRule;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRule;
import com.hazelcast.sql.impl.calcite.opt.physical.SortPhysicalRule;
import com.hazelcast.sql.impl.calcite.opt.physical.agg.AggregatePhysicalRule;
import com.hazelcast.sql.impl.calcite.opt.physical.join.JoinPhysicalRule;
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
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.HazelcastRootCalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
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
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.util.List;
import java.util.Properties;

/**
 * Execution context which holds the whole environment for the given execution session.
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public final class ExecutionContext {

    private static final RelMetadataProvider METADATA_PROVIDER = ChainedRelMetadataProvider.of(ImmutableList.of(
        HazelcastRelMdRowCount.SOURCE,
        DefaultRelMetadataProvider.INSTANCE
    ));

    private static final CalciteConnectionConfig CONNECTION_CONFIG;

    private final QueryParser parser;
    private final QueryConverter converter;
    private final VolcanoPlanner planner;

    static {
        Properties connectionProperties = new Properties();

        connectionProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        connectionProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        connectionProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());

        CONNECTION_CONFIG = new CalciteConnectionConfigImpl(connectionProperties);
    }

    private ExecutionContext(
        QueryParser parser,
        QueryConverter converter,
        VolcanoPlanner planner
    ) {
        this.parser = parser;
        this.converter = converter;
        this.planner = planner;
    }

    public static ExecutionContext create(
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

    public static ExecutionContext create(
        HazelcastSchema rootSchema,
        List<List<String>> schemaPaths,
        int memberCount
    ) {
        DistributionTraitDef distributionTraitDef = new DistributionTraitDef(memberCount);

        JavaTypeFactory typeFactory = new HazelcastTypeFactory();
        Prepare.CatalogReader catalogReader = createCatalogReader(typeFactory, CONNECTION_CONFIG, rootSchema, schemaPaths);
        SqlValidator validator = createValidator(typeFactory, catalogReader);
        VolcanoPlanner planner = createPlanner(CONNECTION_CONFIG, distributionTraitDef);
        HazelcastRelOptCluster cluster = createCluster(planner, typeFactory, distributionTraitDef);

        QueryParser parser = new QueryParser(validator);
        QueryConverter converter = new QueryConverter(catalogReader, validator, cluster);

        return new ExecutionContext(parser, converter, planner);
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
    public RelNode convert(SqlNode node) {
        return converter.convert(node);
    }

    /**
     * Perform logical optimization.
     *
     * @param rel Original logical tree.
     * @return Optimized logical tree.
     */
    public LogicalRel optimizeLogical(RelNode rel) {
        RuleSet rules = LogicalRules.getRuleSet();
        Program program = Programs.of(rules);

        RelNode res = program.run(
            planner,
            rel,
            OptUtils.toLogicalConvention(rel.getTraitSet()),
            ImmutableList.of(),
            ImmutableList.of()
        );

        return new RootLogicalRel(res.getCluster(), res.getTraitSet(), res);
    }

    /**
     * Perform physical optimization. This is where proper access methods and algorithms for joins and aggregations are chosen.
     *
     * @param rel Optimized logical tree.
     * @return Optimized physical tree.
     */
    public PhysicalRel optimizePhysical(RelNode rel) {
        RuleSet rules = RuleSets.ofList(
            SortPhysicalRule.INSTANCE,
            RootPhysicalRule.INSTANCE,
            FilterPhysicalRule.INSTANCE,
            ProjectPhysicalRule.INSTANCE,
            MapScanPhysicalRule.INSTANCE,
            AggregatePhysicalRule.INSTANCE,
            JoinPhysicalRule.INSTANCE,

            new AbstractConverter.ExpandConversionRule(RelFactories.LOGICAL_BUILDER)
        );

        Program program = Programs.of(rules);

        RelNode res = program.run(
            planner,
            rel,
            OptUtils.toPhysicalConvention(rel.getTraitSet(), OptUtils.getDistributionDef(rel).getTraitRoot()),
            ImmutableList.of(),
            ImmutableList.of()
        );

        return (PhysicalRel) res;
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
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        planner.addRelTraitDef(distributionTraitDef);

        return planner;
    }

    private static HazelcastRelOptCluster createCluster(
        VolcanoPlanner planner,
        JavaTypeFactory typeFactory,
        DistributionTraitDef distributionTraitDef
    ) {
        // TODO: Use CachingRelMetadataProvider instead?
        RelMetadataProvider relMetadataProvider = JaninoRelMetadataProvider.of(METADATA_PROVIDER);

        HazelcastRelOptCluster cluster = HazelcastRelOptCluster.create(
            planner,
            new RexBuilder(typeFactory),
            distributionTraitDef
        );

        cluster.setMetadataProvider(relMetadataProvider);

        return cluster;
    }
}
