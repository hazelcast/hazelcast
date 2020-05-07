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
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.calcite.opt.cost.CostFactory;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTraitDef;
import com.hazelcast.sql.impl.calcite.opt.metadata.HazelcastRelMdRowCount;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchemaUtils;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.logical.LogicalRel;
import com.hazelcast.sql.impl.calcite.opt.logical.LogicalRules;
import com.hazelcast.sql.impl.calcite.opt.logical.RootLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.FilterPhysicalRule;
import com.hazelcast.sql.impl.calcite.opt.physical.MapScanPhysicalRule;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ProjectPhysicalRule;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRule;
import com.hazelcast.sql.impl.calcite.opt.physical.SortPhysicalRule;
import com.hazelcast.sql.impl.calcite.opt.physical.agg.AggregatePhysicalRule;
import com.hazelcast.sql.impl.calcite.opt.physical.join.JoinPhysicalRule;
import com.hazelcast.sql.impl.calcite.schema.HazelcastCalciteCatalogReader;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlConformance;
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
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.util.List;
import java.util.Properties;

/**
 * Optimizer context which holds the whole environment for the given optimization session.
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public final class OptimizerContext {
    public static final RelMetadataProvider METADATA_PROVIDER = ChainedRelMetadataProvider.of(ImmutableList.of(
        HazelcastRelMdRowCount.SOURCE,
        DefaultRelMetadataProvider.INSTANCE
    ));

    /**
     * Converter: whether to expand subqueries. When set to {@code false}, subqueries are left as is in the form of
     * {@link org.apache.calcite.rex.RexSubQuery}. Otherwise they are expanded into {@link org.apache.calcite.rel.core.Correlate}
     * instances.
     * Do not enable this because you may run into https://issues.apache.org/jira/browse/CALCITE-3484. Instead, subquery
     * elimination rules are executed during logical planning. In addition, resulting plans are slightly better that those
     * produced by "expand" flag.
     */
    private static final boolean CONVERTER_EXPAND = false;

    /** Converter: whether to trim unused fields. */
    private static final boolean CONVERTER_TRIM_UNUSED_FIELDS = true;

    /** Basic Calcite config. */
    private final VolcanoPlanner planner;

    /** SQL validator. */
    private final SqlValidator validator;

    /** SQL converter. */
    private final SqlToRelConverter sqlToRelConverter;

    private OptimizerContext(
        SqlValidator validator,
        SqlToRelConverter sqlToRelConverter,
        VolcanoPlanner planner
    ) {
        this.validator = validator;
        this.sqlToRelConverter = sqlToRelConverter;
        this.planner = planner;
    }

    public static OptimizerContext create(
        List<TableResolver> tableResolvers,
        List<List<String>> currentSearchPaths,
        int memberCount
    ) {
        // Prepare search paths.
        List<List<String>> searchPaths0 = HazelcastSchemaUtils.prepareSearchPaths(currentSearchPaths, tableResolvers);

        // Resolve tables.
        HazelcastSchema rootSchema = HazelcastSchemaUtils.createRootSchema(tableResolvers);

        return create(rootSchema, searchPaths0, memberCount);
    }

    public static OptimizerContext create(
        HazelcastSchema rootSchema,
        List<List<String>> schemaPaths,
        int memberCount
    ) {
        DistributionTraitDef distributionTraitDef = new DistributionTraitDef(memberCount);

        JavaTypeFactory typeFactory = new HazelcastTypeFactory();
        CalciteConnectionConfig connectionConfig = createConnectionConfig();
        Prepare.CatalogReader catalogReader = createCatalogReader(typeFactory, connectionConfig, rootSchema, schemaPaths);
        SqlValidator validator = createValidator(typeFactory, catalogReader);
        VolcanoPlanner planner = createPlanner(connectionConfig, distributionTraitDef);
        HazelcastRelOptCluster cluster = createCluster(planner, typeFactory, distributionTraitDef);
        SqlToRelConverter sqlToRelConverter = createSqlToRelConverter(catalogReader, validator, cluster);

        return new OptimizerContext(validator, sqlToRelConverter, planner);
    }

    /**
     * Parse SQL statement.
     *
     * @param sql SQL string.
     * @return SQL tree.
     */
    public SqlNode parse(String sql) {
        SqlNode node;

        try {
            SqlParser.ConfigBuilder parserConfig = SqlParser.configBuilder();

            // TODO: Tests for it!
            parserConfig.setCaseSensitive(true);
            parserConfig.setUnquotedCasing(Casing.UNCHANGED);
            parserConfig.setQuotedCasing(Casing.UNCHANGED);
            parserConfig.setConformance(HazelcastSqlConformance.INSTANCE);

            SqlParser parser = SqlParser.create(sql, parserConfig.build());

            node = parser.parseStmt();

            // TODO: Get column names through SqlSelect.selectList[i].toString() (and, possibly, origins?)
            return validator.validate(node);
        } catch (Exception e) {
            throw QueryException.error(SqlErrorCode.PARSING, e.getMessage(), e);
        }
    }

    /**
     * Perform initial conversion of an SQL tree to a relational tree.
     *
     * @param node SQL tree.
     * @return Relational tree.
     */
    public RelNode convert(SqlNode node) {
        // 1. Perform initial conversion.
        RelRoot root = sqlToRelConverter.convertQuery(node, false, true);

        // 2. Remove subquery expressions, converting them to Correlate nodes.
        RelNode relNoSubqueries = rewriteSubqueries(root.rel);

        // 3. Perform decorrelation, i.e. rewrite a nested loop where the right side depends on the value of the left side,
        // to a variation of joins, semijoins and aggregations, which could be executed much more efficiently.
        // See "Unnesting Arbitrary Queries", Thomas Neumann and Alfons Kemper.
        RelNode relDecorrelated = sqlToRelConverter.decorrelate(node, relNoSubqueries);

        // 4. The side effect of subquery rewrite and decorrelation in Apache Calcite is a number of unnecessary fields,
        // primarily in projections. This steps removes unused fields from the tree.
        RelNode relTrimmed = sqlToRelConverter.trimUnusedFields(true, relDecorrelated);

        return relTrimmed;
    }

    /**
     * Special substep of an initial query conversion which eliminates correlated subqueries, converting them to various forms
     * of joins. It is used instead of "expand" flag due to bugs in Calcite (see {@link #CONVERTER_EXPAND}).
     *
     * @param rel Initial relation.
     * @return Resulting relation.
     */
    private RelNode rewriteSubqueries(RelNode rel) {
        HepProgramBuilder hepPgmBldr = new HepProgramBuilder();

        hepPgmBldr.addRuleInstance(SubQueryRemoveRule.FILTER);
        hepPgmBldr.addRuleInstance(SubQueryRemoveRule.PROJECT);
        hepPgmBldr.addRuleInstance(SubQueryRemoveRule.JOIN);

        HepPlanner planner = new HepPlanner(hepPgmBldr.build(), Contexts.empty(), true, null, RelOptCostImpl.FACTORY);

        planner.setRoot(rel);

        return planner.findBestExp();
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

    public RelDataType getParameterRowType(SqlNode sqlNode) {
        return validator.getParameterRowType(sqlNode);
    }

    private static CalciteConnectionConfig createConnectionConfig() {
        Properties properties = new Properties();

        properties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        properties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        properties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());

        return new CalciteConnectionConfigImpl(properties);
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

    private static SqlToRelConverter createSqlToRelConverter(
        Prepare.CatalogReader catalogReader,
        SqlValidator validator,
        HazelcastRelOptCluster cluster
    ) {
        SqlToRelConverter.ConfigBuilder sqlToRelConfigBuilder = SqlToRelConverter.configBuilder()
            .withTrimUnusedFields(CONVERTER_TRIM_UNUSED_FIELDS)
            .withExpand(CONVERTER_EXPAND);

        return new SqlToRelConverter(
            null,
            validator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            sqlToRelConfigBuilder.build()
        );
    }
}
