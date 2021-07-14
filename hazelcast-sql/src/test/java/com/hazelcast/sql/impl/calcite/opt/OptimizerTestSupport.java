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

package com.hazelcast.sql.impl.calcite.opt;

import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.calcite.HazelcastSqlBackend;
import com.hazelcast.sql.impl.calcite.OptimizerContext;
import com.hazelcast.sql.impl.calcite.TestMapTable;
import com.hazelcast.sql.impl.calcite.opt.cost.Cost;
import com.hazelcast.sql.impl.calcite.opt.cost.CostFactory;
import com.hazelcast.sql.impl.calcite.opt.logical.LogicalRel;
import com.hazelcast.sql.impl.calcite.opt.logical.LogicalRules;
import com.hazelcast.sql.impl.calcite.opt.logical.RootLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRules;
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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PARTITIONED;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Collections.emptyList;
import static junit.framework.TestCase.assertEquals;

/**
 * Base class to test optimizers.
 */
public abstract class OptimizerTestSupport extends SqlTestSupport {
    protected RelNode optimizeLogical(String sql, QueryDataType... parameterTypes) {
        return optimize(sql, 1, false, parameterTypes).getLogical();
    }

    protected RelNode optimizePhysical(String sql, QueryDataType... parameterTypes) {
        return optimize(sql, 1, true, parameterTypes).getPhysical();
    }

    protected RelNode optimizePhysical(String sql, int nodeCount, QueryDataType... parameterTypes) {
        return optimize(sql, nodeCount, true, parameterTypes).getPhysical();
    }

    private Result optimize(String sql, int nodeCount, boolean physical, QueryDataType... parameterTypes) {
        HazelcastSchema schema = createDefaultSchema();

        QueryParameterMetadata parameterMetadata;

        if (parameterTypes == null) {
            parameterMetadata = null;
        } else {
            ParameterConverter[] parameterConverters = new ParameterConverter[parameterTypes.length];

            for (int i = 0; i < parameterTypes.length; i++) {
                parameterConverters[i] = new StrictParameterConverter(0, SqlParserPos.ZERO, parameterTypes[i]);
            }

            parameterMetadata = new QueryParameterMetadata(parameterConverters);
        }

        return optimize(sql, schema, nodeCount, physical, parameterMetadata);
    }

    /**
     * Optimize with the given schema.
     *
     * @param sql    SQL.
     * @param schema Schema.
     * @return Result.
     */
    protected static Result optimize(
            String sql,
            HazelcastSchema schema,
            int nodeCount,
            boolean physical,
            QueryParameterMetadata parameterMetadata
    ) {
        OptimizerContext context = OptimizerContext.create(
                HazelcastSchemaUtils.createCatalog(schema),
                QueryUtils.prepareSearchPaths(null, null),
                emptyList(),
                nodeCount,
                new HazelcastSqlBackend(null),
                null
        );

        return optimize(sql, context, physical, parameterMetadata);
    }

    /**
     * Optimize with the given context.
     *
     * @param sql     SQL.
     * @param context Context.
     * @return Result.
     */
    private static Result optimize(
            String sql,
            OptimizerContext context,
            boolean physical,
            QueryParameterMetadata parameterMetadata
    ) {
        QueryParseResult parseResult = context.parse(sql);

        SqlNode node = parseResult.getNode();
        RelNode convertedRel = context.convert(parseResult).getRel();
        LogicalRel logicalRel = optimizeLogicalInternal(context, convertedRel);
        PhysicalRel physicalRel = physical ? optimizePhysicalInternal(context, logicalRel, parameterMetadata) : null;

        return new Result(node, convertedRel, logicalRel, physicalRel);
    }

    private static LogicalRel optimizeLogicalInternal(OptimizerContext context, RelNode node) {
        RelNode logicalRel = context.optimize(node, LogicalRules.getRuleSet(), OptUtils.toLogicalConvention(node.getTraitSet()));

        return new RootLogicalRel(logicalRel.getCluster(), logicalRel.getTraitSet(), logicalRel);
    }

    private static PhysicalRel optimizePhysicalInternal(
            OptimizerContext context,
            RelNode node,
            QueryParameterMetadata parameterMetadata
    ) {
        RelTraitSet physicalTraitSet = OptUtils.toPhysicalConvention(
                node.getTraitSet(),
                OptUtils.getDistributionDef(node).getTraitRoot()
        );

        context.setParameterMetadata(parameterMetadata);

        return (PhysicalRel) context.optimize(node, PhysicalRules.getRuleSet(), physicalTraitSet);
    }

    protected static HazelcastTable partitionedTable(
            String name,
            List<TableField> fields,
            List<MapTableIndex> indexes,
            long rowCount
    ) {
        return partitionedTable(name, fields, indexes, rowCount, false);
    }

    protected static HazelcastTable partitionedTable(
            String name,
            List<TableField> fields,
            List<MapTableIndex> indexes,
            long rowCount,
            boolean nativeMemoryEnabled
    ) {
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
                nativeMemoryEnabled
        );

        return new HazelcastTable(table, new HazelcastTableStatistic(rowCount));
    }

    /**
     * Creates the default test schema. Override this method if you would like to have another schema.
     *
     * @return Default schema.
     */
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        tableMap.put("p", partitionedTable(
                "p",
                fields("f0", INT, "f1", INT, "f2", INT, "f3", INT, "f4", INT),
                null,
                100,
                false
        ));

        return new HazelcastSchema(tableMap);
    }

    protected static List<TableField> fields(Object... namesAndTypes) {
        assert namesAndTypes.length % 2 == 0;

        List<TableField> res = new ArrayList<>();

        for (int i = 0; i < namesAndTypes.length / 2; ++i) {
            String fieldName = (String) namesAndTypes[i * 2];
            QueryDataType fieldType = (QueryDataType) namesAndTypes[i * 2 + 1];

            TableField field = TestMapTable.field(fieldName, fieldType, false);

            res.add(field);
        }

        return res;
    }

    public static PlanRows plan(PlanRow... rows) {
        PlanRows res = new PlanRows();

        for (PlanRow row : rows) {
            res.add(row);
        }

        return res;
    }

    public static PlanRows plan(RelNode rel) {
        PlanRows res = new PlanRows();

        BufferedReader br = new BufferedReader(new StringReader(RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES)));

        String line;

        try {
            while ((line = br.readLine()) != null) {
                PlanRow row = PlanRow.parse(line);

                res.add(row);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return res;
    }

    public static void assertPlan(RelNode rel, PlanRows expected) {
        PlanRows actual = plan(rel);

        assertPlan(actual, expected);
    }

    public static void assertPlan(PlanRows actual, PlanRows expected) {
        int expectedRowCount = expected.getRowCount();
        int actualRowCount = actual.getRowCount();

        assertEquals(planErrorMessage("Plans are different", expected, actual), expectedRowCount, actualRowCount);

        for (int i = 0; i < expectedRowCount; i++) {
            PlanRow expectedRow = expected.getRow(i);
            PlanRow actualRow = actual.getRow(i);

            assertEquals(
                    planErrorMessage(
                            "Plan rows are different at " + (i + 1), expected, actual
                    ),
                    expectedRow,
                    actualRow
            );
        }

        System.out.println(">>> VERIFIED PLAN:");
        System.out.println(actual);
    }

    public static PlanRow planRow(int level, Class<? extends RelNode> node, String signature) {
        return new PlanRow(level, node, signature);
    }

    public static PlanRow planRow(int level, Class<? extends RelNode> node, String signature, Double rows) {
        return new PlanRow(level, node, signature, rows);
    }

    public static PlanRow planRow(int level, Class<? extends RelNode> node, String signature, Double rows, RelOptCost cost) {
        return new PlanRow(level, node, signature, rows, (Cost) cost);
    }

    public static Cost cost(double rows, double cpu, double network) {
        return CostFactory.INSTANCE.makeCost(rows, cpu, network);
    }

    private static String planErrorMessage(String message, PlanRows expected, PlanRows actual) {
        return message + "\n\n>>> EXPECTED PLAN:\n" + expected + "\n>>> ACTUAL PLAN:\n" + actual;
    }

    /**
     * Optimization result with all steps recorded.
     */
    protected static class Result {

        private final SqlNode sql;
        private final RelNode original;
        private final LogicalRel logical;
        private final PhysicalRel physical;

        public Result(SqlNode sql, RelNode original, LogicalRel logical, PhysicalRel physical) {
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
}
