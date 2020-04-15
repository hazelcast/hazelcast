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

package com.hazelcast.sql.optimizer.support;

import com.hazelcast.sql.impl.calcite.OptimizerConfig;
import com.hazelcast.sql.impl.calcite.OptimizerContext;
import com.hazelcast.sql.impl.calcite.opt.logical.LogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.statistics.TableStatistics;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.predicate.AndPredicate;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.resolveTypeForClass;
import static junit.framework.TestCase.assertEquals;

/**
 * Base class to test optimizers.
 */
public abstract class OptimizerTestSupport {
    /** Last result. */
    protected Result last;

    @After
    public void after() {
        last = null;
    }

    /**
     * @return {code True} if physical optimization should be performed.
     */
    protected abstract boolean isOptimizePhysical();

    /**
     * Optimize with the default schema.
     *
     * @param sql SQL.
     * @return Context.
     */
    protected Result optimize(String sql) {
        HazelcastSchema schema = createDefaultSchema();

        return optimize(sql, schema);
    }

    /**
     * Optimize with the given schema.
     *
     * @param sql SQL.
     * @param schema Schema.
     * @return Result.
     */
    protected Result optimize(String sql, HazelcastSchema schema) {
        OptimizerConfig config = OptimizerConfig.builder().build();

        OptimizerContext context = OptimizerContext.create(schema, 1, config);

        return optimize(sql, context);
    }

    /**
     * Optimize with the given context.
     *
     * @param sql SQL.
     * @param context Context.
     * @return Result.
     */
    protected Result optimize(String sql, OptimizerContext context) {
        SqlNode node = context.parse(sql);
        RelNode converted = context.convert(node);
        LogicalRel logical = context.optimizeLogical(converted);
        PhysicalRel physical = isOptimizePhysical() ? context.optimizePhysical(logical, null) : null;

        Result res = new Result(node, converted, logical, physical);

        last = res;

        return res;
    }

    /**
     * Creates the default test schema. Override that method if you would like to have anoher schema.
     *
     * @return Default schema.
     */
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("p", new HazelcastTable(
            null,
            "p",
            true,
            null,
            null,
            null,
            null,
            fieldTypes("f1", INT, "f2", INT, "f3", INT, "f4", INT, "f5", INT),
            null,
            new TableStatistics(100))
        );

        return new HazelcastSchema(tableMap);
    }

    @SuppressWarnings("unchecked")
    protected static <T> List<T> list(T... vals) {
        if (vals == null) {
            return new ArrayList<>();
        } else {
            return new ArrayList<>(Arrays.asList(vals));
        }
    }

    protected static void assertFieldIndexes(List<Integer> expProjects, List<Integer> projects) {
        if (projects == null) {
            projects = new ArrayList<>();
        } else {
            projects = new ArrayList<>(projects);
        }

        assertEquals(expProjects, projects);
    }

    protected static Expression<?> constant(Object val) {
        QueryDataType type = resolveTypeForClass(val == null ? void.class : val.getClass());
        return ConstantExpression.create(type, val);
    }

    protected static Expression<?> compare(Expression<?> first, Expression<?> second, ComparisonMode type) {
        return ComparisonPredicate.create(first, second, type);
    }

    protected static Expression<?> compareColumnsEquals(int col1, int col2) {
        return compare(column(col1), column(col2), ComparisonMode.EQUALS);
    }

    protected static Expression<?> and(Expression<?>... operands) {
        return AndPredicate.create(operands);
    }

    protected static Expression<?> column(int col) {
        return ColumnExpression.create(col, QueryDataType.VARCHAR);
    }

    protected static Map<String, QueryDataType> fieldTypes(Object ... namesAndTypes) {
        Map<String, QueryDataType> fieldTypes = new LinkedHashMap<>();
        assert namesAndTypes.length % 2 == 0;
        for (int i = 0; i < namesAndTypes.length / 2; ++i) {
            fieldTypes.put((String) namesAndTypes[i * 2], (QueryDataType) namesAndTypes[i * 2 + 1]);
        }
        return fieldTypes;
    }

    /**
     * Optimization result with all steps recorded.
     */
    protected static class Result {
        /** SQL node. */
        private final SqlNode sql;

        /** Original rel. */
        private final RelNode original;

        /** Optimized logical rel. */
        private final LogicalRel logical;

        /** Optimized physical rel. */
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
