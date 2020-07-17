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

package com.hazelcast.sql.impl.calcite.opt.physical.index;

import com.hazelcast.config.IndexType;
import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import com.hazelcast.sql.impl.calcite.opt.PlanRows;
import com.hazelcast.sql.impl.calcite.opt.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

/**
 * Tests for sorted index scans
 */
// TODO: In separate classes:
//   Composite indexes
//   Different types!
public class PhysicalIndexScanTest extends OptimizerTestSupport {
    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        HazelcastTable pTable = OptimizerTestSupport.partitionedTable(
            "p",
            OptimizerTestSupport.fields("ret", INT, "f1", INT, "f2", INT, "f3", INT),
            Arrays.asList(
                new MapTableIndex("sorted_f1", IndexType.SORTED, singletonList(1), singletonList(INT)),
                new MapTableIndex("bitmap_f1", IndexType.BITMAP, singletonList(1), singletonList(INT)),

                new MapTableIndex("hash_f2", IndexType.HASH, singletonList(2), singletonList(INT)),
                new MapTableIndex("bitmap_f2", IndexType.BITMAP, singletonList(2), singletonList(INT)),

                new MapTableIndex("sorted_f3", IndexType.SORTED, singletonList(3), singletonList(INT)),
                new MapTableIndex("hash_f3", IndexType.HASH, singletonList(3), singletonList(INT)),
                new MapTableIndex("bitmap_f3", IndexType.BITMAP, singletonList(3), singletonList(INT))
            ),
            100
        );

        tableMap.put("p", pTable);

        return new HazelcastSchema(tableMap);
    }

    @Test
    public void test_equals_literal() {
        checkEquals("f1", "1", "sorted_f1", "$1", "1");
        checkEquals("f2", "1", "hash_f2", "$2", "1");
        checkEquals("f3", "1", "hash_f3", "$3", "1");
    }

    @Test
    public void test_equals_parameter() {
        checkEquals("f1", "?", "sorted_f1", "CAST($1):BIGINT(63)", "?0", INT);
        checkEquals("f2", "?", "hash_f2", "CAST($2):BIGINT(63)", "?0", INT);
        checkEquals("f3", "?", "hash_f3", "CAST($3):BIGINT(63)", "?0", INT);
    }

    @Test
    public void test_equals_expressions() {
        checkEquals("f1", "? + 1", "sorted_f1", "CAST($1):BIGINT(64)", "+(?0, 1:TINYINT(1))", INT);
        checkEquals("f2", "? + 1", "hash_f2", "CAST($2):BIGINT(64)", "+(?0, 1:TINYINT(1))", INT);
        checkEquals("f3", "? + 1", "hash_f3", "CAST($3):BIGINT(64)", "+(?0, 1:TINYINT(1))", INT);

        // TODO: Add tests with complex expressions with and without columns on both sides
    }

    private void checkEquals(
        String operand1,
        String operand2,
        String expectedIndexName,
        String expectedOperand1,
        String expectedOperand2,
        QueryDataType... parameterTypes
    ) {
        String condition = operand1 + " = " + operand2;
        String inverseCondition = operand2 + " = " + operand1;

        String expectedFilter = "=(" + expectedOperand1 + ", " + expectedOperand2 + ")";
        String expectedInverseFilter = "=(" + expectedOperand2 + ", " + expectedOperand1 + ")";

        checkIndex(condition, null, expectedIndexName, expectedFilter, "true", 15d, parameterTypes);
        checkIndex(inverseCondition, null, expectedIndexName, expectedInverseFilter, "true", 15d, parameterTypes);
    }

    @Test
    public void test_or_literal() {
        checkOr("f1", "1", "2", "sorted_f1", "=($1, 1)", "=($1, 2)");
        checkOr("f2", "1", "2", "hash_f2", "=($2, 1)", "=($2, 2)");
        checkOr("f3", "1", "2", "hash_f3", "=($3, 1)", "=($3, 2)");
    }

    @Ignore
    @Test
    public void test_or_params() {
        // TODO
    }

    @Ignore
    @Test
    public void test_or_expressions() {
        // TODO
    }

    @Test
    public void test_or_complex() {
        checkIndex("f1 = 1 OR f1 = 2 OR f1 = 3", null, "sorted_f1", "OR(=($1, 1), =($1, 2), =($1, 3))", "true", 25d);
        checkIndex("f1 = 1 OR (f1 = 2 OR f1 = 3)", null, "sorted_f1", "OR(=($1, 1), =($1, 2), =($1, 3))", "true", 25d);
        checkIndex("f1 = 1 OR f1 IN (2, 3)", null, "sorted_f1", "OR(=($1, 1), =($1, 2), =($1, 3))", "true", 25d);
    }

    private void checkOr(
        String column,
        String value1,
        String value2,
        String expectedIndexName,
        String expectedOperand1,
        String expectedOperand2,
        QueryDataType... parameterTypes
    ) {
        String condition = "(" + column + " = " + value1 + " OR " + column + " = " + value2 + ")";
        String conditionWithIn = column + " IN (" + value1 + ", " + value2 + ")";
        String inverseCondition = "(" + column + " = " + value2 + " OR " + column + " = " + value1 + ")";

        String expectedFilter = "OR(" + expectedOperand1 + ", " + expectedOperand2 + ")";
        String expectedInverseFilter = "OR(" + expectedOperand2 + ", " + expectedOperand1 + ")";

        checkIndex(condition, null, expectedIndexName, expectedFilter, "true", 25d, parameterTypes);
        checkIndex(conditionWithIn, null, expectedIndexName, expectedFilter, "true", 25d, parameterTypes);
        checkIndex(inverseCondition, null, expectedIndexName, expectedInverseFilter, "true", 25d, parameterTypes);

        // Make sure that inequality disallows index usage
        String conditionWithInequality = "(" + column + " = " + value1 + " OR " + column + " > " + value2 + ")";
        checkNoIndex(conditionWithInequality, null, parameterTypes);
    }

    @Test
    public void test_range_literal() {
        checkRange("f1", "1", "2", "sorted_f1", "$1", "1", "2");
        checkIndex("f1 BETWEEN 1 AND 2", null, "sorted_f1", "AND(>=($1, 1), <=($1, 2))", "true", 25d);

        checkRange("f2", "1", "2", null, null, null, null);
    }

    @Ignore
    @Test
    public void test_range_params() {
        // TODO
    }

    @Ignore
    @Test
    public void test_range_expressions() {
        // TODO
    }

    private void checkRange(
        String column,
        String value1,
        String value2,
        String expectedIndexName,
        String expectedColumnSignature,
        String expectedOperand1,
        String expectedOperand2,
        QueryDataType... parameterTypes
    ) {
        checkRangeOpen(column, ">", value1, expectedIndexName, expectedColumnSignature, expectedOperand1, parameterTypes);
        checkRangeOpen(column, ">=", value1, expectedIndexName, expectedColumnSignature, expectedOperand1, parameterTypes);
        checkRangeOpen(column, "<", value1, expectedIndexName, expectedColumnSignature, expectedOperand1, parameterTypes);
        checkRangeOpen(column, "<=", value1, expectedIndexName, expectedColumnSignature, expectedOperand1, parameterTypes);

        checkRangeClosed(column, ">", value1, "<", value2, expectedIndexName, expectedColumnSignature,
            expectedOperand1, expectedOperand2, parameterTypes);
        checkRangeClosed(column, ">", value1, "<=", value2, expectedIndexName, expectedColumnSignature,
            expectedOperand1, expectedOperand2, parameterTypes);
        checkRangeClosed(column, ">=", value1, "<", value2, expectedIndexName, expectedColumnSignature,
            expectedOperand1, expectedOperand2, parameterTypes);
        checkRangeClosed(column, ">=", value1, "<=", value2, expectedIndexName, expectedColumnSignature,
            expectedOperand1, expectedOperand2, parameterTypes);
    }

    private void checkRangeOpen(
        String column,
        String operator,
        String value,
        String expectedIndexName,
        String expectedColumnSignature,
        String expectedOperand,
        QueryDataType... parameterTypes
    ) {
        String condition = column + " " + operator + " " + value;

        if (expectedIndexName != null) {
            String expectedFilter = operator + "(" + expectedColumnSignature + ", " + expectedOperand + ")";
            checkIndex(condition, null, expectedIndexName, expectedFilter, "true", 50d, parameterTypes);
        } else {
            checkNoIndex(condition, null);
        }

        // Test that the opposite order of arguments also works as expected
        if (expectedIndexName != null) {
            String conditionInverse = value + " " + operator + " " + column;
            String expectedFilterInverse = operator + "(" + expectedOperand + ", " + expectedColumnSignature + ")";
            checkIndex(conditionInverse, null, expectedIndexName, expectedFilterInverse, "true", 50d, parameterTypes);
        } else {
            checkNoIndex(condition, null);
        }
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private void checkRangeClosed(
        String column,
        String operator1,
        String value1,
        String operator2,
        String value2,
        String expectedIndexName,
        String expectedColumnSignature,
        String expectedOperand1,
        String expectedOperand2,
        QueryDataType... parameterTypes
    ) {
        String condition = column + " " + operator1 + " " + value1 + " AND " + column + " " + operator2 + " " + value2;

        if (expectedIndexName != null) {
            String expectedFilter1 = operator1 + "(" + expectedColumnSignature + ", " + expectedOperand1 + ")";
            String expectedFilter2 = operator2 + "(" + expectedColumnSignature + ", " + expectedOperand2 + ")";
            String expectedFilter = "AND(" + expectedFilter1 + ", " + expectedFilter2 + ")";

            checkIndex(condition, null, expectedIndexName, expectedFilter, "true", 25d, parameterTypes);
        } else {
            checkNoIndex(condition, null, parameterTypes);
        }
    }

    @Test
    public void testAdditionalPredicates() {
        // No additional predicates, just to confirm that index works
        checkIndex("f1 = 1", null, "sorted_f1", "=($1, 1)", "true", 15d);

        // AND should not interfere with index choice
        checkIndex("f1 = 1", "AND ret = 5", "sorted_f1", "=($1, 1)", "=($0, 5)", 2.2d);

        // OR cancels out the index optimization
        checkNoIndex("f1 = 1", "OR ret = 5");

        // NOT cancels out the index optimization
        checkNoIndex("NOT (f1 = 1)", null);
    }

    @Test
    public void testExpressionTypePreference() {
        // Equality has top priority
        checkIndex("f1 = 1 AND f1 IN (1, 2)", null, "sorted_f1", "=($1, 1)", "OR(=($1, 1), =($1, 2))", 3.8d);

        // IN is better than range
        checkIndex("f1 IN (1, 2) AND f1 >= 1", null, "sorted_f1", "OR(=($1, 1), =($1, 2))", ">=($1, 1)", 12.5d);
    }

    private void checkIndex(
        String condition,
        String additionalCondition,
        String expectedIndex,
        String expectedIndexFilter,
        String expectedRemainderFilter,
        double expectedRowCount,
        QueryDataType... parameterTypes
    ) {
        // Build actual plan
        String sql = additionalCondition == null ? sql(condition) : sql(condition, additionalCondition);

        RelNode actualRel = optimizePhysical(sql, parameterTypes);
        PlanRows actualPlan = plan(actualRel);

        // Build expected plan
        PlanRows expectedPlan = planWithIndex(expectedIndex, expectedIndexFilter, expectedRemainderFilter, expectedRowCount);

        // Check
        assertPlan(actualPlan, expectedPlan);
    }

    private void checkNoIndex(String condition, String additionalCondition, QueryDataType... parameterTypes) {
        String sql = additionalCondition == null ? sql(condition) : sql(condition, additionalCondition);

        RelNode rel = optimizePhysical(sql, parameterTypes);
        PlanRows plan = plan(rel);

        assertEquals(2, plan.getRowCount());
        assertEquals(MapScanPhysicalRel.class.getSimpleName(), plan.getRow(1).getNode());
    }

    private static PlanRows planWithIndex(String indexName, String indexExp, String remainderExp, double rowCount) {
        return plan(
            planRow(0, RootPhysicalRel.class, "", rowCount),
            planRow(1, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0]]]], index=[" + indexName
                + "], indexExp=[" + indexExp + "], remainderExp=[" + remainderExp + "]", rowCount)
        );
    }

    private static String sql(String condition, String... otherConditions) {
        StringBuilder res = new StringBuilder("SELECT ret FROM p WHERE " + condition);

        if (otherConditions != null && otherConditions.length > 0) {
            for (String otherCondition : otherConditions) {
                res.append(" ").append(otherCondition);
            }
        }

        return res.toString();
    }
}
