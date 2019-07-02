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
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.sql.HazelcastSqlTransientException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryFragment;
import com.hazelcast.sql.impl.QueryPlan;
import com.hazelcast.sql.impl.SqlOptimizer;
import com.hazelcast.sql.impl.calcite.logical.rel.LogicalRel;
import com.hazelcast.sql.impl.calcite.logical.rel.RootLogicalRel;
import com.hazelcast.sql.impl.calcite.logical.rule.FilterLogicalRule;
import com.hazelcast.sql.impl.calcite.logical.rule.LogicalMapScanRule;
import com.hazelcast.sql.impl.calcite.logical.rule.ProjectIntoScanLogicalRule;
import com.hazelcast.sql.impl.calcite.logical.rule.ProjectLogicalRule;
import com.hazelcast.sql.impl.calcite.logical.rule.SortLogicalRule;
import com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionTrait;
import com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionTraitDef;
import com.hazelcast.sql.impl.calcite.physical.rel.PhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rule.MapScanPhysicalRule;
import com.hazelcast.sql.impl.calcite.physical.rule.RootPhysicalRule;
import com.hazelcast.sql.impl.calcite.physical.rule.SortPhysicalRule;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.schema.Person;
import com.hazelcast.util.collection.PartitionIdSet;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Calcite-based SQL optimizer.
 */
public class CalciteSqlOptimizer implements SqlOptimizer {
    /** Node engine. */
    private final NodeEngine nodeEngine;

    public CalciteSqlOptimizer(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public QueryPlan prepare(String sql) {
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
        LogicalRel logicalRel = doOptimizeLogical(rel);

        // 5. Perform physical cost-based optimization.
        PhysicalRel physicalRel = doOptimizePhysical(planner, logicalRel);

        // 6. Create plan.
        return doCreatePlan(physicalRel);
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
        return new HazelcastSqlValidator(
            opTab,
            catalogReader,
            typeFactory,
            HazelcastSqlConformance.INSTANCE
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
        planner.addRelTraitDef(PhysicalDistributionTraitDef.INSTANCE);
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
            parserConfig.setConformance(HazelcastSqlConformance.INSTANCE);

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

    private LogicalRel doOptimizeLogical(RelNode rel) {
        // TODO: Rules to merge scan and project/filter
        // TODO: Rule to eliminate sorting if the source is already sorted.

        HepProgramBuilder hepBuilder = new HepProgramBuilder();

        hepBuilder.addRuleInstance(new AbstractConverter.ExpandConversionRule(RelFactories.LOGICAL_BUILDER));

        hepBuilder.addRuleInstance(ProjectFilterTransposeRule.INSTANCE);
        hepBuilder.addRuleInstance(ProjectIntoScanLogicalRule.INSTANCE);

        hepBuilder.addRuleInstance(SortLogicalRule.INSTANCE);
        hepBuilder.addRuleInstance(LogicalMapScanRule.INSTANCE);
        hepBuilder.addRuleInstance(FilterLogicalRule.INSTANCE);
        hepBuilder.addRuleInstance(ProjectLogicalRule.INSTANCE);

        HepPlanner hepPlanner = new HepPlanner(
            hepBuilder.build()
        );

        hepPlanner.setRoot(rel);

        RelNode optimizedRel = hepPlanner.findBestExp();

        return new RootLogicalRel(
            optimizedRel.getCluster(),
            optimizedRel.getTraitSet(),
            optimizedRel
        );
    }

    /**
     * Optimize physical nodes.
     *
     * @param logicalRel Logical node.
     * @return Optimized physical node.
     */
    private PhysicalRel doOptimizePhysical(VolcanoPlanner planner, LogicalRel logicalRel) {
        // TODO: Cache rules
        RuleSet rules = RuleSets.ofList(
            SortPhysicalRule.INSTANCE,
            RootPhysicalRule.INSTANCE,
            MapScanPhysicalRule.INSTANCE,
            new AbstractConverter.ExpandConversionRule(RelFactories.LOGICAL_BUILDER)
        );

        RelTraitSet traits = logicalRel.getTraitSet()
            .plus(HazelcastConventions.PHYSICAL)
            .plus(PhysicalDistributionTrait.SINGLETON);

        final Program program = Programs.of(rules);

        RelNode res = program.run(
            planner,
            logicalRel,
            traits,
            ImmutableList.of(),
            ImmutableList.of()
        );

        return (PhysicalRel)res;
    }

    /**
     * Create plan from physical rel.
     *
     * @param rel Rel.
     * @return Plan.
     */
    private QueryPlan doCreatePlan(PhysicalRel rel) {
        // Get partition mapping.
        Collection<Partition> parts = nodeEngine.getHazelcastInstance().getPartitionService().getPartitions();

        int partCnt = parts.size();

        Map<String, PartitionIdSet> partMap = new HashMap<>();

        for (Partition part : parts) {
            String ownerId = part.getOwner().getUuid();

            partMap.computeIfAbsent(ownerId, (key) -> new PartitionIdSet(partCnt)).add(part.getPartitionId());
        }

        // Prepare list of all nodes and current node.
        Set<String> partMemberIds = new HashSet<>(partMap.keySet());
        String localMemberId = nodeEngine.getLocalMember().getUuid();

        // Collect remote addresses.
        List<Address> remoteAddresses = new ArrayList<>(partMemberIds.size() - 1);

        for (String partMemberId : partMemberIds) {
            MemberImpl member = nodeEngine.getClusterService().getMember(partMemberId);

            if (member == null)
                throw new HazelcastSqlTransientException(SqlErrorCode.MEMBER_LEAVE, "Participating member has " +
                    "left the topology: " + partMemberId);

            if (!member.localMember())
                remoteAddresses.add(member.getAddress());
        }

        // Create the plan.
        PlanCreateVisitor visitor = new PlanCreateVisitor(partMemberIds, localMemberId);

        rel.visit(visitor);

        List<QueryFragment> fragments = visitor.getFragments();

        return new QueryPlan(fragments, partMap, remoteAddresses);
    }
}
