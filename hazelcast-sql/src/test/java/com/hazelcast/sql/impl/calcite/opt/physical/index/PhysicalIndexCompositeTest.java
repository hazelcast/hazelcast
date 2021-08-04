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

package com.hazelcast.sql.impl.calcite.opt.physical.index;

import com.hazelcast.config.IndexType;
import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import com.hazelcast.sql.impl.calcite.opt.PlanRows;
import com.hazelcast.sql.impl.calcite.opt.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * This class focuses on composite index filter resolution.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category(ParallelJVMTest.class)
public class PhysicalIndexCompositeTest extends IndexOptimizerTestSupport {
    private static final String INDEX_NAME = "index";

    @Parameterized.Parameter
    public IndexType indexType;

    @Parameterized.Parameter(1)
    public boolean hd;

    @Parameterized.Parameters(name = "indexType:{0}, hd:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {IndexType.SORTED, true},
                {IndexType.SORTED, false},
                {IndexType.HASH, true},
                {IndexType.HASH, false}
        });
    }

    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        HazelcastTable pTable = OptimizerTestSupport.partitionedTable(
                "p",
                OptimizerTestSupport.fields("ret", INT, "f1", INT, "f2", INT),
                Collections.singletonList(
                        new MapTableIndex(INDEX_NAME, indexType, 2, asList(1, 2), asList(INT, INT))
                ),
                100,
                hd
        );

        tableMap.put("p", pTable);

        return new HazelcastSchema(tableMap);
    }

    @Test
    public void testEquals() {

        // First component can be used only for SORTED index
        if (indexType == IndexType.SORTED) {
            checkIndexForCondition("f1=1", "=($1, 1)");
            checkIndexForCondition("f1 IS NULL", "IS NULL($1)");
        } else {
            checkNoIndexForCondition("f1=1");
            checkNoIndexForCondition("f1 IS NULL");
        }

        // Second component cannot be used
        checkNoIndexForCondition("f2=1");
        checkNoIndexForCondition("f2 IS NULL");

        // Use both components
        checkIndexForCondition("f1=1 AND f2=2", "AND(=($1, 1), =($2, 2))");
        checkIndexForCondition("f1 IS NULL AND f2=2", "AND(IS NULL($1), =($2, 2))");
        checkIndexForCondition("f1=1 AND f2 IS NULL", "AND(IS NULL($2), =($1, 1))");
    }

    @Test
    public void testRange() {
        // First component can be used only for SORTED index
        if (indexType == IndexType.SORTED) {
            checkIndexForCondition("f1>1", ">($1, 1)");
            checkIndexForCondition("f1>1 AND f1<3", "AND(>($1, 1), <($1, 3))");
        } else {
            checkNoIndexForCondition("f1>1");
            checkNoIndexForCondition("f1>1 AND f1<3");
        }

        // Second component cannot be used
        checkNoIndexForCondition("f2>1");
        checkNoIndexForCondition("f2>1 AND f2<3");

        // Both components could be used only if the first one is equality
        if (indexType == IndexType.SORTED) {
            checkIndexForCondition("f1=1 AND f2>2", "AND(=($1, 1), >($2, 2))");
            checkIndexForCondition("f1=1 AND f2>2 AND f2<4", "AND(<($2, 4), =($1, 1), >($2, 2))");
            checkIndexForCondition("f1 IS NULL AND f2>2", "AND(IS NULL($1), >($2, 2))");
            checkIndexForCondition("f1 IS NULL AND f2>2 AND f2<4", "AND(IS NULL($1), <($2, 4), >($2, 2))");
        } else {
            checkNoIndexForCondition("f1=1 AND f2>2");
            checkNoIndexForCondition("f1=1 AND f2>2 AND f2<4");
            checkNoIndexForCondition("f1 IS NULL AND f2>2");
            checkNoIndexForCondition("f1 IS NULL AND f2>2 AND f2<4");
        }

        if (indexType == IndexType.SORTED) {
            checkIndexForConditionWithRemainder("f1>1 AND f2>2", ">($1, 1)", ">($2, 2)");
            checkIndexForConditionWithRemainder("f1>1 AND f1<3 AND f2>2", "AND(>($1, 1), <($1, 3))", ">($2, 2)");
            checkIndexForConditionWithRemainder("f1>1 AND f1<3 AND f2>2 AND f2<4", "AND(>($1, 1), <($1, 3))", "AND(>($2, 2), <($2, 4))");
        } else {
            checkNoIndexForCondition("f1>1 AND f2>2");
            checkNoIndexForCondition("f1>1 AND f1<3 AND f2>2");
            checkNoIndexForCondition("f1>1 AND f1<3 AND f2>2 AND f2<4");
        }
    }

    @Test
    public void testIn() {
        // First component can be used only for SORTED index
        if (indexType == IndexType.SORTED) {
            checkIndexForCondition("f1=1 OR f1=2", "OR(=($1, 1), =($1, 2))");
        } else {
            checkNoIndexForCondition("f1=1 OR f1=2");
        }

        // Second component cannot be used alone
        checkNoIndexForCondition("f2=1 OR f2=2");

        // Conjunction on different columns inside a disjunction is not supported
        checkNoIndexForCondition("(f1=1 AND f2=2) OR f1=3");

        // Conjunction on the leading part is not supported
        if (indexType == IndexType.SORTED) {
            checkIndexForConditionWithRemainder("(f1=1 OR f1=2) AND f2=3", "OR(=($1, 1), =($1, 2))", "=($2, 3)");
        } else {
            checkNoIndexForCondition("(f1=1 OR f1=2) AND f2=3");
        }

        // Conjunction on the tail is supported
        checkIndexForCondition("f1=1 AND (f2=2 OR f2=3)", "AND(=($1, 1), OR(=($2, 2), =($2, 3)))");
    }

    private void checkIndexForCondition(
            String condition,
            String expectedIndexFilter,
            QueryDataType... parameterTypes
    ) {
        checkIndexForConditionWithRemainder(condition, expectedIndexFilter, "null", parameterTypes);
    }

    private void checkIndexForConditionWithRemainder(
            String condition,
            String expectedIndexFilter,
            String expectedRemainderFilter,
            QueryDataType... parameterTypes
    ) {
        String sql = "SELECT ret FROM p WHERE " + condition;

        checkIndex(sql, INDEX_NAME, expectedIndexFilter, expectedRemainderFilter, parameterTypes);
    }

    private void checkNoIndexForCondition(String condition, QueryDataType... parameterTypes) {
        String sql = "SELECT ret FROM p WHERE " + condition;

        if (hd) {
            RelNode rel = optimizePhysical(sql, parameterTypes);
            PlanRows plan = plan(rel);

            assertEquals(2, plan.getRowCount());
            assertEquals(MapIndexScanPhysicalRel.class.getSimpleName(), plan.getRow(1).getNode());

            MapIndexScanPhysicalRel indexScan = (MapIndexScanPhysicalRel) rel.getInput(0);
            assertEquals(INDEX_NAME, indexScan.getIndex().getName());
            assertNull(indexScan.getIndexFilter());
        } else {
            checkNoIndex(sql, parameterTypes);
        }
    }
}
