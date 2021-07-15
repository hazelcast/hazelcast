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

package com.hazelcast.jet.sql.impl.opt;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.sql.impl.JetPlanExecutor;
import com.hazelcast.jet.sql.impl.JetSqlBackend;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRules;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRules;
import com.hazelcast.jet.sql.impl.schema.MappingCatalog;
import com.hazelcast.jet.sql.impl.schema.MappingStorage;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.calcite.HazelcastSqlBackend;
import com.hazelcast.sql.impl.calcite.OptimizerContext;
import com.hazelcast.sql.impl.calcite.TestMapTable;
import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchemaUtils;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTableStatistic;
import com.hazelcast.sql.impl.calcite.validate.param.StrictParameterConverter;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PARTITIONED;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class OptimizerTestSupport extends SimpleTestInClusterSupport {

    protected RelNode optimizeLogical(String sql, HazelcastTable... tables) {
        HazelcastSchema schema =
                new HazelcastSchema(stream(tables).collect(toMap(table -> table.getTarget().getSqlName(), identity())));
        return optimize(sql, schema, null, false).getLogical();
    }

    protected RelNode optimizePhysical(String sql, List<QueryDataType> types, HazelcastTable... tables) {
        HazelcastSchema schema =
                new HazelcastSchema(stream(tables).collect(toMap(table -> table.getTarget().getSqlName(), identity())));

        QueryParameterMetadata parameterMetadata;

        if (types == null || types.size() == 0) {
            parameterMetadata = null;
        } else {
            ParameterConverter[] parameterConverters = new ParameterConverter[types.size()];

            for (int i = 0; i < types.size(); i++) {
                parameterConverters[i] = new StrictParameterConverter(0, SqlParserPos.ZERO, types.get(i));
            }

            parameterMetadata = new QueryParameterMetadata(parameterConverters);
        }
        return optimize(sql, schema, parameterMetadata, true).getPhysical();
    }

    protected static Result optimize(
            String sql,
            HazelcastSchema schema,
            QueryParameterMetadata queryParameterMetadata,
            boolean shouldOptimizePhysical) {
        HazelcastInstance instance = instance();
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        MappingStorage mappingStorage = new MappingStorage(nodeEngine);
        SqlConnectorCache connectorCache = new SqlConnectorCache(nodeEngine);
        MappingCatalog mappingCatalog = new MappingCatalog(nodeEngine, mappingStorage, connectorCache);
        JetPlanExecutor planExecutor = new JetPlanExecutor(mappingCatalog, instance, emptyMap());

        OptimizerContext context = OptimizerContext.create(
                HazelcastSchemaUtils.createCatalog(schema),
                QueryUtils.prepareSearchPaths(null, null),
                emptyList(),
                1,
                new HazelcastSqlBackend(nodeEngine),
                new JetSqlBackend(nodeEngine, planExecutor)
        );

        return optimize(sql, context, queryParameterMetadata, shouldOptimizePhysical);
    }

    private static Result optimize(
            String sql,
            OptimizerContext context,
            QueryParameterMetadata queryParameterMetadata,
            boolean shouldOptimizePhysical) {
        QueryParseResult parseResult = context.parse(sql);

        SqlNode node = parseResult.getNode();
        RelNode convertedRel = context.convert(parseResult).getRel();
        LogicalRel logicalRel = optimizeLogicalInternal(context, convertedRel);
        PhysicalRel physicalRel = null;
        if (shouldOptimizePhysical) {
            physicalRel = optimizePhysicalInternal(context, logicalRel, queryParameterMetadata);
        }

        return new Result(node, convertedRel, logicalRel, physicalRel);
    }

    private static LogicalRel optimizeLogicalInternal(OptimizerContext context, RelNode node) {
        return (LogicalRel) context.optimize(node, LogicalRules.getRuleSet(), OptUtils.toLogicalConvention(node.getTraitSet()));
    }

    private static PhysicalRel optimizePhysicalInternal(
            OptimizerContext context,
            RelNode node,
            QueryParameterMetadata parameterMetadata
    ) {
        RelTraitSet physicalTraitSet = OptUtils.toPhysicalConvention(node.getTraitSet());
        context.setParameterMetadata(parameterMetadata);
        return (PhysicalRel) context.optimize(node, PhysicalRules.getRuleSet(), physicalTraitSet);
    }

    protected static HazelcastTable partitionedTable(String name, List<TableField> fields, long rowCount) {
        return partitionedTable(name, fields, emptyList(), false, rowCount);
    }

    protected static HazelcastTable partitionedTable(
            String name,
            List<TableField> fields,
            List<MapTableIndex> indexes,
            boolean isHd,
            long rowCount) {
        PartitionedMapTable table = new PartitionedMapTable(
                SCHEMA_NAME_PARTITIONED,
                name,
                name,
                fields,
                new ConstantTableStatistics(rowCount),
                null,
                null,
                null,
                null,
                indexes,
                isHd
        );
        return new HazelcastTable(table, new HazelcastTableStatistic(rowCount));
    }

    protected static TableField field(String name, QueryDataType type) {
        return TestMapTable.field(name, type, false);
    }

    protected static void assertPlan(RelNode rel, PlanRows expected) {
        BufferedReader reader = new BufferedReader(new StringReader(RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES)));
        List<PlanRow> rows = reader.lines()
                .map(PlanRow::parse)
                .collect(toList());
        assertPlan(new PlanRows(rows), expected);
    }

    private static void assertPlan(PlanRows actual, PlanRows expected) {
        int expectedRowCount = expected.getRowCount();
        int actualRowCount = actual.getRowCount();
        assertThat(actualRowCount)
                .as("Plan are different" + "\n\n>>> EXPECTED PLAN:\n%s\n>>> ACTUAL PLAN:\n%s", expected, actual)
                .isEqualTo(expectedRowCount);

        for (int i = 0; i < expectedRowCount; i++) {
            PlanRow expectedRow = expected.getRow(i);
            PlanRow actualRow = actual.getRow(i);
            assertThat(actualRow)
                    .as("Plan rows are different at %s" + "\n\n>>> EXPECTED PLAN:\n%s\n>>> ACTUAL PLAN:\n%s", i + 1, expected, actual)
                    .isEqualTo(expectedRow);
        }
    }

    protected static PlanRows plan(PlanRow... rows) {
        return new PlanRows(asList(rows));
    }

    protected static PlanRow planRow(int level, Class<? extends RelNode> node) {
        return new PlanRow(level, node);
    }

    protected static class Result {

        private final SqlNode sql;
        private final RelNode original;
        private final LogicalRel logical;
        private final PhysicalRel physical;

        private Result(SqlNode sql, RelNode original, LogicalRel logical, PhysicalRel physical) {
            this.sql = sql;
            this.original = original;
            this.logical = logical;
            this.physical = physical;
        }

        public SqlNode getSql() {
            return sql;
        }

        public RelNode getOriginal() {
            return original;
        }

        public LogicalRel getLogical() {
            return logical;
        }

        public PhysicalRel getPhysical() {
            return physical;
        }
    }

    protected static class PlanRows {

        private final List<PlanRow> rows;

        private PlanRows(List<PlanRow> rows) {
            this.rows = rows;
        }

        private int getRowCount() {
            return rows.size();
        }

        private PlanRow getRow(int index) {
            return rows.get(index);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < rows.size(); i++) {
                PlanRow row = rows.get(i);
                builder.append(String.format("%02d", i)).append(": ").append(row).append("\n");
            }
            return builder.toString();
        }
    }

    protected static class PlanRow {

        private final int level;
        private final String node;

        protected PlanRow(int level, Class<? extends RelNode> nodeClass) {
            this(level, nodeClass.getSimpleName());
        }

        protected PlanRow(int level, String node) {
            this.level = level;
            this.node = node;
        }

        protected String getNode() {
            return node;
        }

        protected static PlanRow parse(String input) {
            // level
            int level = 0;
            while (input.charAt(level * 2) == ' ') {
                level++;
            }

            // node
            String nodeAndSignature = input.substring(0, input.lastIndexOf(":")).trim();
            String node = input.contains("(")
                    ? nodeAndSignature.substring(0, nodeAndSignature.indexOf('('))
                    : nodeAndSignature;

            return new PlanRow(level, node);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < level; i++) {
                builder.append("  ");
            }
            builder.append(node);
            return builder.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PlanRow planRow = (PlanRow) o;
            return level == planRow.level && Objects.equals(node, planRow.node);
        }

        @Override
        public int hashCode() {
            return Objects.hash(level, node);
        }
    }
}
