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
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.cost.CostFactory;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTrait;
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
import com.hazelcast.sql.impl.calcite.parser.HazelcastSqlParser;
import com.hazelcast.sql.impl.calcite.schema.HazelcastCalciteCatalogReader;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTableStatistic;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlConformance;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.optimizer.OptimizerRuleCallTracker;
import com.hazelcast.sql.impl.schema.SchemaUtils;
import com.hazelcast.sql.impl.schema.Table;
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
import org.apache.calcite.schema.Schema;
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Execution context which holds the whole environment for the given execution session.
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public final class ExecutionContext {
    public static final RelMetadataProvider METADATA_PROVIDER = ChainedRelMetadataProvider.of(ImmutableList.of(
            HazelcastRelMdRowCount.SOURCE,
            DefaultRelMetadataProvider.INSTANCE
    ));

    /** Converter: whether to convert LogicalTableScan to some physical form immediately or not. We do not need this. */
    private static final boolean CONVERTER_CONVERT_TABLE_ACCESS = false;

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

    /** Thread-local execution config. */
    private static final ThreadLocal<ExecutionConfig> EXECUTION_CONFIG = new ThreadLocal<>();

    /** Execution config. */
    private final ExecutionConfig config;

    /** Cluster. */
    private final HazelcastRelOptCluster cluster;

    /** Basic Calcite config. */
    private final VolcanoPlanner planner;

    /** SQL validator. */
    private final SqlValidator validator;

    /** SQL converter. */
    private final SqlToRelConverter sqlToRelConverter;

    private ExecutionContext(
        ExecutionConfig config,
        SqlValidator validator,
        SqlToRelConverter sqlToRelConverter,
        HazelcastRelOptCluster cluster,
        VolcanoPlanner planner
    ) {
        this.config = config;
        this.validator = validator;
        this.sqlToRelConverter = sqlToRelConverter;
        this.cluster = cluster;
        this.planner = planner;
    }

    public static ExecutionContext create(
        List<TableResolver> tableResolvers,
        List<List<String>> currentSearchPaths,
        int memberCount
    ) {
        // Prepare search paths.
        List<List<String>> searchPaths = SchemaUtils.prepareSearchPaths(currentSearchPaths, tableResolvers);

        // Resolve tables.
        HazelcastSchema rootSchema = createRootSchema(tableResolvers);

        return create(rootSchema, searchPaths, memberCount, getExecutionConfig());
    }

    public static ExecutionContext create(
        HazelcastSchema rootSchema,
        List<List<String>> schemaPaths,
        int memberCount,
        ExecutionConfig config
    ) {
        if (config == null) {
            config = ExecutionConfig.builder().build();
        }

        JavaTypeFactory typeFactory = new HazelcastTypeFactory();
        CalciteConnectionConfig connectionConfig = createConnectionConfig();
        Prepare.CatalogReader catalogReader = createCatalogReader(typeFactory, connectionConfig, rootSchema, schemaPaths);
        SqlValidator validator = createValidator(typeFactory, catalogReader);
        VolcanoPlanner planner = createPlanner(connectionConfig);
        HazelcastRelOptCluster cluster = createCluster(planner, typeFactory, memberCount);
        SqlToRelConverter sqlToRelConverter = createSqlToRelConverter(catalogReader, validator, cluster);

        return new ExecutionContext(config, validator, sqlToRelConverter, cluster, planner);
    }

    /**
     * Parse SQL statement.
     *
     * @param sql SQL string.
     * @return SQL tree.
     */
    public SqlNode parse(String sql) {
        try {
            SqlParser.ConfigBuilder parserConfig = SqlParser.configBuilder();

            parserConfig.setParserFactory(HazelcastSqlParser.FACTORY);
            // TODO: Tests for it!
            parserConfig.setCaseSensitive(true);
            parserConfig.setUnquotedCasing(Casing.UNCHANGED);
            parserConfig.setQuotedCasing(Casing.UNCHANGED);
            parserConfig.setConformance(HazelcastSqlConformance.INSTANCE);

            SqlParser parser = SqlParser.create(sql, parserConfig.build());

            return parser.parseStmt();
        } catch (Exception e) {
            throw QueryException.error(SqlErrorCode.PARSING, e.getMessage(), e);
        }
    }

    /**
     * Validate SQL node.
     */
    public SqlNode validate(SqlNode node) {
        try {
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
    public PhysicalRel optimizePhysical(RelNode rel, OptimizerRuleCallTracker ruleCallTracker) {
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

        try {
            cluster.startPhysicalOptimization(ruleCallTracker);

            if (ruleCallTracker != null) {
                ruleCallTracker.onStart();
            }

            RelNode res = program.run(
                planner,
                rel,
                OptUtils.toPhysicalConvention(rel.getTraitSet(), DistributionTrait.ROOT_DIST),
                ImmutableList.of(),
                ImmutableList.of()
            );

            if (ruleCallTracker != null) {
                ruleCallTracker.onDone();
            }

            return (PhysicalRel) res;
        } finally {
            cluster.finishPhysicalOptimization();
        }
    }

    public RelDataType getParameterRowType(SqlNode sqlNode) {
        return validator.getParameterRowType(sqlNode);
    }

    public ExecutionConfig getConfig() {
        return config;
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

    private static VolcanoPlanner createPlanner(CalciteConnectionConfig config) {
        VolcanoPlanner planner = new VolcanoPlanner(
            CostFactory.INSTANCE,
            Contexts.of(config)
        );

        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(DistributionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        return planner;
    }

    private static HazelcastRelOptCluster createCluster(VolcanoPlanner planner, JavaTypeFactory typeFactory, int memberCount) {
        // TODO: Use CachingRelMetadataProvider instead?
        RelMetadataProvider relMetadataProvider = JaninoRelMetadataProvider.of(METADATA_PROVIDER);

        HazelcastRelOptCluster cluster = HazelcastRelOptCluster.create(planner, new RexBuilder(typeFactory), memberCount);
        cluster.setMetadataProvider(relMetadataProvider);

        return cluster;
    }

    private static SqlToRelConverter createSqlToRelConverter(
        Prepare.CatalogReader catalogReader,
        SqlValidator validator,
        HazelcastRelOptCluster cluster
    ) {
        SqlToRelConverter.ConfigBuilder sqlToRelConfigBuilder = SqlToRelConverter.configBuilder()
            //.withConvertTableAccess(CONVERTER_CONVERT_TABLE_ACCESS)
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

    private static ExecutionConfig getExecutionConfig() {
        ExecutionConfig res = EXECUTION_CONFIG.get();

        if (res != null) {
            EXECUTION_CONFIG.remove();
        } else {
            res = ExecutionConfig.builder().build();
        }

        return res;
    }

    public static void setExecutionConfig(ExecutionConfig executionConfig) {
        EXECUTION_CONFIG.set(executionConfig);
    }

    /**
     * Creates the top-level catalog containing the given child schema.
     *
     * @param schema Schema.
     * @return Catalog.
     */
    public static HazelcastSchema createCatalog(Schema schema) {
        return new HazelcastSchema(
            Collections.singletonMap(SchemaUtils.CATALOG, schema),
            Collections.emptyMap()
        );
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static HazelcastSchema createRootSchema(List<TableResolver> tableResolvers) {
        // Create tables.
        Map<String, Map<String, HazelcastTable>> tableMap = new HashMap<>();

        for (TableResolver tableResolver : tableResolvers) {
            Collection<Table> tables = tableResolver.getTables();

            if (tables == null || tables.isEmpty()) {
                continue;
            }

            for (Table table : tables) {
                HazelcastTable convertedTable = new HazelcastTable(
                    table,
                    new HazelcastTableStatistic(table.getStatistics().getRowCount())
                );

                Map<String, HazelcastTable> schemaTableMap =
                    tableMap.computeIfAbsent(table.getSchemaName(), (k) -> new HashMap<>());

                schemaTableMap.put(table.getName(), convertedTable);
            }
        }

        // Create schemas.
        Map<String, Schema> schemaMap = new HashMap<>();

        for (Map.Entry<String, Map<String, HazelcastTable>> schemaEntry : tableMap.entrySet()) {
            String schemaName = schemaEntry.getKey();
            Map schemaTables = schemaEntry.getValue();

            HazelcastSchema schema = new HazelcastSchema(Collections.emptyMap(), schemaTables);

            schemaMap.put(schemaName, schema);
        }

        HazelcastSchema rootSchema = new HazelcastSchema(schemaMap, Collections.emptyMap());

        return createCatalog(rootSchema);
    }
}
