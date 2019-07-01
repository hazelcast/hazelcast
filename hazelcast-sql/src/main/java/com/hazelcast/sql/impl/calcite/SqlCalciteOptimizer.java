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

package com.hazelcast.sql.impl.calcite;

import com.google.common.collect.ImmutableList;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.sql.impl.PhysicalPlan;
import com.hazelcast.sql.impl.SqlOptimizer;
import com.hazelcast.sql.impl.calcite.logical.rel.HazelcastRel;
import com.hazelcast.sql.impl.calcite.logical.rel.HazelcastRootRel;
import com.hazelcast.sql.impl.calcite.logical.rule.HazelcastFilterRule;
import com.hazelcast.sql.impl.calcite.logical.rule.HazelcastProjectIntoScanRule;
import com.hazelcast.sql.impl.calcite.logical.rule.HazelcastProjectRule;
import com.hazelcast.sql.impl.calcite.logical.rule.HazelcastSortRule;
import com.hazelcast.sql.impl.calcite.logical.rule.HazelcastTableScanRule;
import com.hazelcast.sql.impl.calcite.physical.distribution.HazelcastDistributionTrait;
import com.hazelcast.sql.impl.calcite.physical.distribution.HazelcastDistributionTraitDef;
import com.hazelcast.sql.impl.calcite.physical.rel.HazelcastPhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rule.HazelcastRootPhysicalRule;
import com.hazelcast.sql.impl.calcite.physical.rule.HazelcastSortPhysicalRule;
import com.hazelcast.sql.impl.calcite.physical.rule.HazelcastTableScanPhysicalRule;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.schema.Person;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.impl.LongSchemaVersion;
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

import java.util.Collections;
import java.util.Properties;

/**
 * Calcite-based SQL optimizer.
 */
public class SqlCalciteOptimizer implements SqlOptimizer {
    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Logger. */
    private final ILogger logger;

    public SqlCalciteOptimizer(NodeEngine nodeEngine, ILogger logger) {
        this.nodeEngine = nodeEngine;
        this.logger = logger;
    }

    @Override
    public PhysicalPlan prepare(String sql) {
        // 1. Prepare context.
        // TODO: Cache as much as possible.
        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
        CalciteConnectionConfig config = prepareConfig();
        Prepare.CatalogReader catalogReader = prepareCatalogReader(typeFactory, config);
        SqlValidator validator = prepareValidator(typeFactory, catalogReader);
        VolcanoPlanner planner = preparePlanner(config);
        SqlToRelConverter sqlToRelConverter = prepareSqlToRelConverter(typeFactory, catalogReader, validator, planner);

        // 2. Parse SQL string and validate it.
        SqlNode node = doParse(sql, validator);

        // 3. Convert to REL.
        RelNode rel = doConvertToRel(node, sqlToRelConverter);

        // 4. Perform logical heuristic optimization.
        HazelcastRel logicalRel = doOptimizeLogical(rel);

        // 5. Perform physical cost-based optimization.
        HazelcastPhysicalRel physicalRel = doOptimizePhysical(planner, logicalRel);

        // 6. Convert to executable plan.
        SqlCalcitePlanVisitor planVisitor = new SqlCalcitePlanVisitor();

        logicalRel.visitForPlan(planVisitor);

        return planVisitor.getPlan();
    }

    private CalciteConnectionConfig prepareConfig() {
        // TODO: Cache
        // TODO: Can we avoid using CalciteConnectionCalciteConnectionConfigImpl?
        Properties properties = new Properties();

        properties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        properties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        properties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());

        return new CalciteConnectionConfigImpl(properties);
    }

    private Prepare.CatalogReader prepareCatalogReader(JavaTypeFactory typeFactory, CalciteConnectionConfig config) {
        // TODO: Dynamic schema! See DynamicSchema and DynamicRootSchema in Drill
        CalciteSchema schema = CalciteSchema.createRootSchema(true);

        schema.add("persons", new HazelcastTable(typeFactory.createStructType(Person.class)));

        CalciteSchema rootSchema = schema.createSnapshot(new LongSchemaVersion(System.nanoTime()));

        // TODO: Our own implemenation of catalog reader! (see DrillCalciteCatalogReader)
        return new CalciteCatalogReader(
            rootSchema,
            Collections.emptyList(), // Default schema path.
            typeFactory,
            config
        );
    }

    private SqlValidator prepareValidator(JavaTypeFactory typeFactory, Prepare.CatalogReader catalogReader) {
        // TODO: Operator table which support only functions supported by Hazelcast.
        final SqlOperatorTable opTab = ChainedSqlOperatorTable.of(SqlStdOperatorTable.instance(), catalogReader);

        // TODO: Need our own validator, investigate interface
        return new SqlCalciteValidator(
            opTab,
            catalogReader,
            typeFactory,
            SqlCalciteConformance.INSTANCE
        );
    }

    private VolcanoPlanner preparePlanner(CalciteConnectionConfig config) {
        // TODO: Drill's SqlConverter.toRel - see how VolcanoPlanner is instantiated.
        final VolcanoPlanner planner = new VolcanoPlanner(
            null, // TODO: DrillCostBase.DrillCostFactory
            Contexts.of(config)
        );

        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(HazelcastDistributionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        return planner;
    }

    private SqlToRelConverter prepareSqlToRelConverter(
        JavaTypeFactory typeFactory,
        Prepare.CatalogReader catalogReader,
        SqlValidator validator,
        VolcanoPlanner planner
    ) {
        // TODO: See SqlConverter.SqlToRelConverterConfig
        final SqlToRelConverter.ConfigBuilder sqlToRelConfigBuilder =
            SqlToRelConverter.configBuilder()
                .withTrimUnusedFields(true)
                .withExpand(false)
                .withExplain(false)
                .withConvertTableAccess(false);

        return new SqlToRelConverter(
            null, // TODO: ViewExpander, see CalcitePrepareImpl which implements this interface
            validator,
            catalogReader,
            RelOptCluster.create(planner, new RexBuilder(typeFactory)), // TODO: See DrillRexBuilder
            StandardConvertletTable.INSTANCE,
            sqlToRelConfigBuilder.build()
        );
    }

    private SqlNode doParse(String sql, SqlValidator validator) {
        try {
            SqlParser.ConfigBuilder parserConfig = SqlParser.configBuilder();

            parserConfig.setUnquotedCasing(Casing.UNCHANGED);
            parserConfig.setConformance(SqlCalciteConformance.INSTANCE);

            // TODO: Can we cache it?
            SqlParser parser = SqlParser.create(sql, parserConfig.build());

            SqlNode node = parser.parseStmt();

            // TODO: Use SqlShuttle to look for unsupported query parts. See Drill's UnsupportedOperatorsVisitor.
            return validator.validate(node);
        }
        catch (Exception e) {
            // TODO: Throw proper parse exception.
            throw new HazelcastException("Failed to parse SQL: " + sql, e);
        }
    }

    private RelNode doConvertToRel(SqlNode node, SqlToRelConverter sqlToRelConverter) {
        RelRoot root = sqlToRelConverter.convertQuery(node, false, true);

        return root.rel;
    }

    private HazelcastRel doOptimizeLogical(RelNode rel) {
        // TODO: Rules to merge scan and project/filter
        // TODO: Rule to eliminate sorting if the source is already sorted.
        // TODO: Cache rules and posslbly the whole planner.

        HepProgramBuilder hepBuilder = new HepProgramBuilder();

        hepBuilder.addRuleInstance(new AbstractConverter.ExpandConversionRule(RelFactories.LOGICAL_BUILDER));

        hepBuilder.addRuleInstance(ProjectFilterTransposeRule.INSTANCE); // TODO: Remove once both merge routines are ready
        hepBuilder.addRuleInstance(HazelcastProjectIntoScanRule.INSTANCE);

        hepBuilder.addRuleInstance(HazelcastSortRule.INSTANCE);
        hepBuilder.addRuleInstance(HazelcastTableScanRule.INSTANCE);
        hepBuilder.addRuleInstance(HazelcastFilterRule.INSTANCE);
        hepBuilder.addRuleInstance(HazelcastProjectRule.INSTANCE);

        HepPlanner hepPlanner = new HepPlanner(
            hepBuilder.build()
        );

        hepPlanner.setRoot(rel);

        RelNode optimizedRel = hepPlanner.findBestExp();

        HazelcastRootRel res = new HazelcastRootRel(
            optimizedRel.getCluster(),
            optimizedRel.getTraitSet(),
            optimizedRel
        );

        return res;
    }

    /**
     * Optimize physical nodes.
     *
     * @param logicalRel Logical node.
     * @return Optimized physical node.
     */
    private HazelcastPhysicalRel doOptimizePhysical(VolcanoPlanner planner, HazelcastRel logicalRel) {
        // TODO: Cache rules
        RuleSet rules = RuleSets.ofList(
            HazelcastSortPhysicalRule.INSTANCE,
            HazelcastRootPhysicalRule.INSTANCE,
            HazelcastTableScanPhysicalRule.INSTANCE,
            new AbstractConverter.ExpandConversionRule(RelFactories.LOGICAL_BUILDER)
        );

        RelTraitSet traits = logicalRel.getTraitSet()
            .plus(HazelcastPhysicalRel.HAZELCAST_PHYSICAL)
            .plus(HazelcastDistributionTrait.SINGLETON);

        final Program program = Programs.of(rules);

        RelNode res = program.run(
            planner,
            logicalRel,
            traits,
            ImmutableList.of(),
            ImmutableList.of()
        );

        return (HazelcastPhysicalRel)res;
    }
}
