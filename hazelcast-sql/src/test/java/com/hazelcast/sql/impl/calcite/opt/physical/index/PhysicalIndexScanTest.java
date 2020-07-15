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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for sorted index scans
 */
// TODO: In this class:
//  Ranges (including BETWEEN)
//  IN conditions (including OR with manual collapse)
//
// TODO: In separate classes:
//   Composite indexes
public class PhysicalIndexScanTest extends OptimizerTestSupport {
    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        HazelcastTable pTable = OptimizerTestSupport.partitionedTable(
            "p",
            OptimizerTestSupport.fields("ret", INT, "f1", INT, "f2", INT, "f3", INT),
            Arrays.asList(
                new MapTableIndex("sorted_f1", IndexType.SORTED, Collections.singletonList(1)),
                new MapTableIndex("bitmap_f1", IndexType.BITMAP, Collections.singletonList(1)),

                new MapTableIndex("hash_f2", IndexType.HASH, Collections.singletonList(2)),
                new MapTableIndex("bitmap_f2", IndexType.BITMAP, Collections.singletonList(2)),

                new MapTableIndex("sorted_f3", IndexType.SORTED, Collections.singletonList(3)),
                new MapTableIndex("hash_f3", IndexType.HASH, Collections.singletonList(3)),
                new MapTableIndex("bitmap_f3", IndexType.BITMAP, Collections.singletonList(3))
            ),
            100
        );

        tableMap.put("p", pTable);

        return new HazelcastSchema(tableMap);
    }

    @Test
    public void test_equals_literal_sorted() {
        checkEqualsLiteral("f1", "sorted_f1", "1");
    }

    @Test
    public void test_equals_literal_hash() {
        checkEqualsLiteral("f2", "hash_f2", "2");
    }

    @Test
    public void test_equals_literal_hashOverSorted() {
        checkEqualsLiteral("f3", "hash_f3", "3");
    }

    private void checkEqualsLiteral(String columnName, String expectedIndexName, String expectedColumnOrdinal) {
        String condition = columnName + " = 1";
        String inverseCondition = "1 = " + columnName;

        String expectedFilter = "=($" + expectedColumnOrdinal + ", 1)";
        String expectedInverseFilter = "=(1, $" + expectedColumnOrdinal + ")";

        checkIndex(condition, null, expectedIndexName, expectedFilter, "true", 15d);
        checkIndex(inverseCondition, null, expectedIndexName, expectedInverseFilter, "true", 15d);

        // Unrelated AND condition
        checkIndex(condition, "AND ret = 5", expectedIndexName, expectedFilter, "=($0, 5)", 2.2d);
        checkIndex(inverseCondition, "AND ret = 5", expectedIndexName, expectedInverseFilter, "=($0, 5)", 2.2d);

        // Unrelated OR condition
        checkNoIndex(condition, "OR ret = 5");
        checkNoIndex(inverseCondition, "OR ret = 5");
    }

    @Ignore("Remove coercion from binary operation if one of the sides is a column")
    @Test
    public void test_equals_parameter() {
        // Plain quality
        checkIndex("f1 = ?", null, "sorted_f1", "=($1, ?0)", "true", 15d, QueryDataType.INT);
        checkIndex("? = f1", null, "sorted_f1", "=(?0, $1)", "true", 15d, QueryDataType.INT);

        // Unrelated AND condition
        checkIndex("f1 = ?", "AND ret = 5", "sorted_f1", "=($1, ?0)", "=($0, 5)", 2.2d);
        checkIndex("? = f1", "AND ret = 5", "sorted_f1", "=(?0, $1)", "=($0, 5)", 2.2d);

        // Unrelated OR condition
        checkNoIndex("f1 = ?", "OR ret = 5");
        checkNoIndex("? = f1", "OR ret = 5");
    }

    @Ignore("Remove coercion from binary operation if one of the sides is a column")
    @Test
    public void test_equals_expressions() {
        // TODO
        fail("Add tests with complex expressions with and without columns on both sides");
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
