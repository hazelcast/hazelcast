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
import com.hazelcast.sql.impl.PhysicalPlan;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.sql.impl.SqlOptimizer;
import com.hazelcast.sql.impl.calcite.logical.rel.HazelcastRel;
import com.hazelcast.sql.impl.calcite.physical.distribution.HazelcastDistributionTrait;
import com.hazelcast.sql.impl.calcite.physical.distribution.HazelcastDistributionTraitDef;
import com.hazelcast.sql.impl.calcite.physical.rel.HazelcastPhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rule.HazelcastRootPhysicalRule;
import com.hazelcast.sql.impl.calcite.physical.rule.HazelcastSortPhysicalRule;
import com.hazelcast.sql.impl.calcite.physical.rule.HazelcastTableScanPhysicalRule;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.schema.Person;
import com.hazelcast.sql.impl.calcite.logical.rule.HazelcastFilterRule;
import com.hazelcast.sql.impl.calcite.logical.rule.HazelcastProjectIntoScanRule;
import com.hazelcast.sql.impl.calcite.logical.rule.HazelcastProjectRule;
import com.hazelcast.sql.impl.calcite.logical.rel.HazelcastRootRel;
import com.hazelcast.sql.impl.calcite.logical.rule.HazelcastSortRule;
import com.hazelcast.sql.impl.calcite.logical.rule.HazelcastTableScanRule;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.Casing;
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
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
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
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
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
        // 1. ==================== PARSE ====================
        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();

        // TODO: Dynamic schema! See DynamicSchema and DynamicRootSchema in Drill
        CalciteSchema schema = CalciteSchema.createRootSchema(true);

        schema.add("persons", new HazelcastTable(typeFactory.createStructType(Person.class)));

        CalciteSchema rootSchema = schema.createSnapshot(new LongSchemaVersion(System.nanoTime()));

        Properties properties = new Properties();

        properties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        properties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        properties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());

        CalciteConnectionConfigImpl config = new CalciteConnectionConfigImpl(properties);

        // TODO: Our own implemenation of catalog reader! (see DrillCalciteCatalogReader)
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
            rootSchema,
            Collections.emptyList(), // Default schema path.
            typeFactory,
            config
        );

        // TODO: Drill's SqlConverter.toRel - see how VolcanoPlanner is instantiated.
        final VolcanoPlanner planner = new VolcanoPlanner(
            null, // TODO: DrillCostBase.DrillCostFactory
            Contexts.of(config)
        );

        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        // planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
        planner.addRelTraitDef(HazelcastDistributionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        // TODO: Add various rules (see CalcitePrepareImpl.createPlanner)

        SqlRexConvertletTable rexConvertletTable = StandardConvertletTable.INSTANCE;

        SqlNode node = parse(sql);

        // 2. ==================== ANALYZE/VALIDATE ====================
        final SqlOperatorTable opTab0 = config.fun(SqlOperatorTable.class, SqlStdOperatorTable.instance());
        final SqlOperatorTable opTab = ChainedSqlOperatorTable.of(opTab0, catalogReader);

        // TODO: Need our own validator, investigate interface
        SqlValidator sqlValidator = new SqlCalciteValidator(
            opTab,
            catalogReader,
            typeFactory,
            config.conformance()
        );

        SqlNode validatedNode = sqlValidator.validate(node);

        // TODO: User SqlShuttle to look for unsupported query parts. See Drill's UnsupportedOperatorsVisitor.

        // 3. ==================== CONVERT TO LOGICAL TREE ====================

        // TODO: See SqlConverter.SqlToRelConverterConfig
        final SqlToRelConverter.ConfigBuilder sqlToRelConfigBuilder =
            SqlToRelConverter.configBuilder()
                .withTrimUnusedFields(true)
                .withExpand(false)
                .withExplain(false)
                .withConvertTableAccess(false);

        // TODO: See DrillRexBuilder
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
            null, // TODO: ViewExpander, see CalcitePrepareImpl which implements this interface
            sqlValidator,
            catalogReader,
            cluster,
            rexConvertletTable,
            sqlToRelConfigBuilder.build()
        );

        RelRoot root = sqlToRelConverter.convertQuery(validatedNode, false, true);

        // TODO: See PreProcessLogicalRel and DefaultSqlHandler.validateAndConvert

        System.out.println(">>> Converted REL: " + root);

        // 4. ==================== OPTIMIZE ====================
        HazelcastRel logicalRel = processLogical(root.rel);
        HazelcastPhysicalRel physicalRel = processPhysical(planner, logicalRel);

        // TODO: Use visitor to find unsupported operations!

        // TODO: See DefaultSqlHandler.convertToDrel
        // TODO: See DrillPushProjectIntoScanRule and ParquetPushDownFilter for how project/filter are merged into scan.

        // 5. ==================== DECOUPLE FROM CALCITE ====================
        SqlCalcitePlanVisitor planVisitor = new SqlCalcitePlanVisitor();

        logicalRel.visitForPlan(planVisitor);

        PhysicalPlan plan = planVisitor.getPlan();

        return plan;
    }

    private HazelcastRel processLogical(RelNode rel) {
        HepProgramBuilder hepBuilder = new HepProgramBuilder();

        // TODO: Rules to merge scan and project/filter
        // TODO: Rule to eliminate sorting if the source is already sorted.

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

        System.out.println(">>> Processed logical: " + res);

        return res;
    }

    /**
     * Optimize physical nodes.
     *
     * @param logicalRel Logical node.
     * @return Optimized physical node.
     */
    private HazelcastPhysicalRel processPhysical(VolcanoPlanner planner, HazelcastRel logicalRel) {
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

    /**
     * Parse SQL query and return SQL node.
     *
     * @param sql SQL.
     * @return Node.
     */
    private SqlNode parse(String sql) {
        try {
            SqlParser.ConfigBuilder parserConfig = SqlParser.configBuilder();

            parserConfig.setUnquotedCasing(Casing.UNCHANGED);
            parserConfig.setConformance(SqlCalciteConformance.INSTANCE);

            SqlParser parser = SqlParser.create(sql, parserConfig.build());

            return parser.parseStmt();
        }
        catch (Exception e) {
            // TODO: Throw proper parse exception.
            throw new HazelcastException("Failed to parse SQL: " + sql, e);
        }
    }
}
