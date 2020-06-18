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

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.calcite.TestMapTable;
import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import com.hazelcast.sql.impl.calcite.opt.cost.Cost;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.RootExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.schema.Table;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for root node planning.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PhysicalRootTest extends OptimizerTestSupport {
    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        tableMap.put("p", OptimizerTestSupport.partitionedTable(
            "p",
            Arrays.asList(
                TestMapTable.field("f0"),
                TestMapTable.field("f1"),
                TestMapTable.field("f2"),
                TestMapTable.field("f3"),
                TestMapTable.field("f4")
            ),
            100
        ));

        tableMap.put("e", OptimizerTestSupport.partitionedTable(
            "e",
            Arrays.asList(
                TestMapTable.field("f0"),
                TestMapTable.field("f1"),
                TestMapTable.field("f2"),
                TestMapTable.field("f3"),
                TestMapTable.field("f4")
            ),
            0
        ));

        return new HazelcastSchema(tableMap);
    }

    @Test
    public void test_singleNode_nonEmpty() {
        Cost scanCost = cost(100d, 500d, 0d);
        Cost rootCost = cost(100d, 100d, 0d).plus(scanCost);

        assertPlan(
            optimizePhysical("SELECT * FROM p"),
            plan(
                planRow(0, RootPhysicalRel.class, "", 100d, rootCost),
                planRow(1, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2, 3, 4]]]]", 100d, scanCost)
            )
        );
    }

    @Test
    public void test_singleNode_empty() {
        Cost scanCost = cost(0d, 0d, 0d);
        Cost rootCost = cost(1d, 1d, 0d).plus(scanCost);

        assertPlan(
            optimizePhysical("SELECT * FROM e"),
            plan(
                planRow(0, RootPhysicalRel.class, "", 1d, rootCost),
                planRow(1, MapScanPhysicalRel.class, "table=[[hazelcast, e[projects=[0, 1, 2, 3, 4]]]]", 1d, scanCost)
            )
        );
    }

    @Test
    public void test_multipleNodes_nonEmpty() {
        Cost scanCost = cost(100d, 500d, 0d);
        Cost rootExchangeCost = cost(100d, 100d, 2000d).plus(scanCost);
        Cost rootCost = cost(100d, 100d, 0d).plus(rootExchangeCost);

        assertPlan(
            optimizePhysical("SELECT * FROM p", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 100d, rootCost),
                planRow(1, RootExchangePhysicalRel.class, "", 100d, rootExchangeCost),
                planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0, 1, 2, 3, 4]]]]", 100d, scanCost)
            )
        );
    }

    @Test
    public void test_multipleNodes_empty() {
        Cost scanCost = cost(0d, 0d, 0d);
        Cost rootExchangeCost = cost(1d, 1d, 20d).plus(scanCost);
        Cost rootCost = cost(1d, 1d, 0d).plus(rootExchangeCost);

        assertPlan(
            optimizePhysical("SELECT * FROM e", 2),
            plan(
                planRow(0, RootPhysicalRel.class, "", 1d, rootCost),
                planRow(1, RootExchangePhysicalRel.class, "", 1d, rootExchangeCost),
                planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, e[projects=[0, 1, 2, 3, 4]]]]", 1d, scanCost)
            )
        );
    }
}
