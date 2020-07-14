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

/**
 * Tests for sorted index scans
 */
// TODO: In this class:
//  Ranges (including BETWEEN)
//  IN conditions (including OR with manual collapse)
//  AND/OR added predicates
//  Expressions on the column side - do not use index!
//
// TODO: In separate classes:
//   Composite indexes
//   Hash indexes
//   Prefer HASH over SORTED where possible
public class PhysicalSortedIndexScanTest extends OptimizerTestSupport {
    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        HazelcastTable pTable = OptimizerTestSupport.partitionedTable(
            "p",
            OptimizerTestSupport.fields("ret", INT, "f1", INT, "f2", INT, "f3", INT),
            Arrays.asList(
                new MapTableIndex("sorted_f1", IndexType.SORTED, Collections.singletonList(1))
            ),
            100
        );

        tableMap.put("p", pTable);

        return new HazelcastSchema(tableMap);
    }

    @Test
    public void test_equals_sorted_literal() {
        // Literals
        assertPlan(
            optimizePhysical(sql("f1 = 1")),
            planWithIndex("sorted_f1", "=($1, 1)", "true", 15d)
        );

        assertPlan(
            optimizePhysical(sql("1 = f1")),
            planWithIndex("sorted_f1", "=(1, $1)", "true", 15d)
        );
    }

    @Ignore("Remove coercion from binary operation if one of the sides is a column")
    @Test
    public void test_equals_sorted_parameter() {
        // Parameters
        assertPlan(
            optimizePhysical(sql("f1 = ?"), QueryDataType.INT),
            planWithIndex("sorted_f1", "=($1, ?0)", "true", 15d)
        );

        assertPlan(
            optimizePhysical(sql("? = f1"), QueryDataType.INT),
            planWithIndex("sorted_f1", "=(?0, $1)", "true", 15d)
        );
    }

    @Ignore("Remove coercion from binary operation if one of the sides is a column")
    @Test
    public void test_equals_sorted_expression() {
        // Parameters
        assertPlan(
            optimizePhysical(sql("f1 = ? + 1"), QueryDataType.INT),
            planWithIndex("sorted_f1", "=($1, +(?0, 1:TINYINT(1))))", "true", 15d)
        );

        assertPlan(
            optimizePhysical(sql("? + 1 = f1"), QueryDataType.INT),
            planWithIndex("sorted_f1", "=(+(?0, 1:TINYINT(1))), $1)", "true", 15d)
        );
    }

    @Test
    public void test_equals_sorted_column() {
        // Parameters
        assertNoIndex(optimizePhysical(sql("f1 = ret")));
        assertNoIndex(optimizePhysical(sql("ret = f1")));
    }

    private static void assertNoIndex(RelNode rel) {
        PlanRows actual = plan(rel);

        assertEquals(2, actual.getRowCount());
        assertEquals(MapScanPhysicalRel.class.getSimpleName(), actual.getRow(1).getNode());
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
