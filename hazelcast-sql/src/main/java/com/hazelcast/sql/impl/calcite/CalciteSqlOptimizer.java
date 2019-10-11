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
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.HazelcastSqlTransientException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryPlan;
import com.hazelcast.sql.impl.SqlOptimizer;
import com.hazelcast.sql.impl.calcite.logical.LogicalJoinRules;
import com.hazelcast.sql.impl.calcite.logical.LogicalProjectFilterRules;
import com.hazelcast.sql.impl.calcite.logical.rel.LogicalRel;
import com.hazelcast.sql.impl.calcite.logical.rel.RootLogicalRel;
import com.hazelcast.sql.impl.calcite.logical.rule.AggregateLogicalRule;
import com.hazelcast.sql.impl.calcite.logical.rule.FilterLogicalRule;
import com.hazelcast.sql.impl.calcite.logical.rule.JoinLogicalRule;
import com.hazelcast.sql.impl.calcite.logical.rule.MapScanLogicalRule;
import com.hazelcast.sql.impl.calcite.logical.rule.ProjectLogicalRule;
import com.hazelcast.sql.impl.calcite.logical.rule.SortLogicalRule;
import com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionTrait;
import com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionTraitDef;
import com.hazelcast.sql.impl.calcite.physical.rel.PhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rule.CollocatedAggregatePhysicalRule;
import com.hazelcast.sql.impl.calcite.physical.rule.CollocatedJoinPhysicalRule;
import com.hazelcast.sql.impl.calcite.physical.rule.FilterPhysicalRule;
import com.hazelcast.sql.impl.calcite.physical.rule.MapScanPhysicalRule;
import com.hazelcast.sql.impl.calcite.physical.rule.ProjectPhysicalRule;
import com.hazelcast.sql.impl.calcite.physical.rule.RootPhysicalRule;
import com.hazelcast.sql.impl.calcite.physical.rule.SortPhysicalRule;
import com.hazelcast.sql.impl.calcite.schema.HazelcastRootSchema;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.HazelcastRootCalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Calcite-based SQL optimizer.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class CalciteSqlOptimizer implements SqlOptimizer {
    /** Node engine. */
    private final NodeEngine nodeEngine;

    public CalciteSqlOptimizer(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public QueryPlan prepare(String sql) {
        HazelcastCalciteContext.initialize(nodeEngine);

        try {
            return prepare0(sql);
        } finally {
            HazelcastCalciteContext.clear();
        }
    }

    private QueryPlan prepare0(String sql) {
        // 1. Prepare context.
        JavaTypeFactory typeFactory = new HazelcastTypeFactory();
        CalciteConnectionConfig config = prepareConfig();
        Prepare.CatalogReader catalogReader = prepareCatalogReader(typeFactory, config);
        SqlValidator validator = prepareValidator(typeFactory, catalogReader);
        VolcanoPlanner planner = preparePlanner(config);
        SqlToRelConverter sqlToRelConverter = prepareSqlToRelConverter(typeFactory, catalogReader, validator, planner);

        // 2. Parse SQL string and validate it.
        SqlNode node = doParse(sql, validator);

        // 3. Convert to REL.
        RelNode rel = doConvertToRel(node, sqlToRelConverter);

        // 4. Perform logical optimization.
        LogicalRel logicalRel = doOptimizeLogical(planner, rel);

        // 5. Perform physical optimization.
        PhysicalRel physicalRel = doOptimizePhysical(planner, logicalRel);

        // 6. Create plan.
        return doCreatePlan(physicalRel);
    }

    private CalciteConnectionConfig prepareConfig() {
        Properties properties = new Properties();

        properties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        properties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        properties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());

        return new CalciteConnectionConfigImpl(properties);
    }

    private Prepare.CatalogReader prepareCatalogReader(JavaTypeFactory typeFactory, CalciteConnectionConfig config) {
        HazelcastRootCalciteSchema schema = new HazelcastRootCalciteSchema(new HazelcastRootSchema(nodeEngine));

        return new HazelcastCalciteCatalogReader(
            schema,
            typeFactory,
            config
        );
    }

    private SqlValidator prepareValidator(JavaTypeFactory typeFactory, Prepare.CatalogReader catalogReader) {
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

    private VolcanoPlanner preparePlanner(CalciteConnectionConfig config) {
        VolcanoPlanner planner = new VolcanoPlanner(
            null,
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
        SqlToRelConverter.ConfigBuilder sqlToRelConfigBuilder = SqlToRelConverter.configBuilder()
            .withTrimUnusedFields(true)
            .withExpand(false)
            .withExplain(false)
            .withConvertTableAccess(false);

        return new SqlToRelConverter(
            null,
            validator,
            catalogReader,
            RelOptCluster.create(planner, new RexBuilder(typeFactory)),
            StandardConvertletTable.INSTANCE,
            sqlToRelConfigBuilder.build()
        );
    }

    private SqlNode doParse(String sql, SqlValidator validator) {
        SqlNode node;

        try {
            SqlParser.ConfigBuilder parserConfig = SqlParser.configBuilder();

            parserConfig.setCaseSensitive(true);
            parserConfig.setUnquotedCasing(Casing.UNCHANGED);
            parserConfig.setQuotedCasing(Casing.UNCHANGED);
            parserConfig.setConformance(HazelcastSqlConformance.INSTANCE);

            SqlParser parser = SqlParser.create(sql, parserConfig.build());

            node = parser.parseStmt();
        } catch (Exception e) {
            throw new HazelcastSqlException(SqlErrorCode.PARSING, e.getMessage(), e);
        }

        return validator.validate(node);
    }

    private RelNode doConvertToRel(SqlNode node, SqlToRelConverter sqlToRelConverter) {
        RelRoot root = sqlToRelConverter.convertQuery(node, false, true);

        // TODO: We loose the final projection here (see root.fields).
        return root.rel;
    }

    private LogicalRel doOptimizeLogical(VolcanoPlanner planner, RelNode rel) {
        RuleSet rules = RuleSets.ofList(
            // Join optimization rules.
            LogicalJoinRules.FILTER_PULL_RULE,
            LogicalJoinRules.CONDITION_PUSH_RULE,
            LogicalJoinRules.EXPRESSIONS_PUSH_RULE,

            // Filter and project rules.
            LogicalProjectFilterRules.FILTER_MERGE_RULE,
            LogicalProjectFilterRules.FILTER_PROJECT_TRANSPOSE_RULE,
            LogicalProjectFilterRules.FILTER_INTO_SCAN_RULE,
            // TODO: ProjectMergeRule: https://jira.apache.org/jira/browse/CALCITE-2223
            LogicalProjectFilterRules.PROJECT_FILTER_TRANSPOSE_RULE,
            LogicalProjectFilterRules.PROJECT_JOIN_TRANSPOSE_RULE,
            LogicalProjectFilterRules.PROJECT_REMOVE_RULE,
            LogicalProjectFilterRules.PROJECT_INTO_SCAN_RULE,


            // TODO: Aggregate rules

            // Convert Calcite node into Hazelcast nodes.
            MapScanLogicalRule.INSTANCE,
            FilterLogicalRule.INSTANCE,
            ProjectLogicalRule.INSTANCE,
            AggregateLogicalRule.INSTANCE,
            SortLogicalRule.INSTANCE,
            JoinLogicalRule.INSTANCE,

            // TODO: Transitive closures

            new AbstractConverter.ExpandConversionRule(RelFactories.LOGICAL_BUILDER)
        );

        Program program = Programs.of(rules);

        RelNode res = program.run(
            planner,
            rel,
            RuleUtils.toLogicalConvention(rel.getTraitSet()),
            ImmutableList.of(),
            ImmutableList.of()
        );

        return new RootLogicalRel(res.getCluster(), res.getTraitSet(), res);
    }

    /**
     * Optimize physical nodes.
     *
     * @param logicalRel Logical node.
     * @return Optimized physical node.
     */
    private PhysicalRel doOptimizePhysical(VolcanoPlanner planner, LogicalRel logicalRel) {
        RuleSet rules = RuleSets.ofList(
            SortPhysicalRule.INSTANCE,
            RootPhysicalRule.INSTANCE,
            FilterPhysicalRule.INSTANCE,
            ProjectPhysicalRule.INSTANCE,
            MapScanPhysicalRule.INSTANCE,
            CollocatedAggregatePhysicalRule.INSTANCE,
            CollocatedJoinPhysicalRule.INSTANCE,
            new AbstractConverter.ExpandConversionRule(RelFactories.LOGICAL_BUILDER)
        );

        Program program = Programs.of(rules);

        RelNode res = program.run(
            planner,
            logicalRel,
            RuleUtils.toPhysicalConvention(logicalRel.getTraitSet(), PhysicalDistributionTrait.SINGLETON),
            ImmutableList.of(),
            ImmutableList.of()
        );

        return (PhysicalRel) res;
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

        LinkedHashMap<UUID, PartitionIdSet> partMap = new LinkedHashMap<>();

        for (Partition part : parts) {
            UUID ownerId = part.getOwner().getUuid();

            partMap.computeIfAbsent(ownerId, (key) -> new PartitionIdSet(partCnt)).add(part.getPartitionId());
        }

        // Collect remote addresses.
        List<Address> dataMemberAddresses = new ArrayList<>(partMap.size());
        List<UUID> dataMemberIds = new ArrayList<>(partMap.size());

        for (UUID partMemberId : partMap.keySet()) {
            MemberImpl member = nodeEngine.getClusterService().getMember(partMemberId);

            if (member == null) {
                throw new HazelcastSqlTransientException(SqlErrorCode.MEMBER_LEAVE, "Participating member has "
                    + "left the topology: " + partMemberId);
            }

            dataMemberAddresses.add(member.getAddress());
            dataMemberIds.add(member.getUuid());
        }

        // Create the plan.
        PlanCreateVisitor visitor = new PlanCreateVisitor(partMap, dataMemberIds, dataMemberAddresses);

        rel.visit(visitor);

        return visitor.getPlan();
    }
}
