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

package com.hazelcast.sql.impl.calcite.opt.logical;

import com.hazelcast.sql.impl.calcite.TestMapTable;
import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.test.HazelcastSerialClassRunner;
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
 * Tests for SELECT * expansion.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LogicalSelectStarTest extends OptimizerTestSupport {
    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        tableMap.put("r", OptimizerTestSupport.partitionedTable(
            "r",
            Arrays.asList(
                TestMapTable.field("r_f1", false),
                TestMapTable.field("r_f2", true)
            ),
            100
        ));

        tableMap.put("s", OptimizerTestSupport.partitionedTable(
            "s",
            Arrays.asList(
                TestMapTable.field("s_f1", false),
                TestMapTable.field("s_f2", true)
            ),
            100
        ));

        return new HazelcastSchema(tableMap);
    }

    @Test
    public void testStar() {
        assertPlan(
            optimizeLogical("SELECT * FROM r"),
            plan(
                planRow(0, RootLogicalRel.class, ""),
                planRow(1, ProjectLogicalRel.class, "r_f1=[$0]"),
                planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, r]], projects=[[0, 1]]")
            )
        );

        assertPlan(
            optimizeLogical("SELECT r.* FROM r"),
            plan(
                planRow(0, RootLogicalRel.class, ""),
                planRow(1, ProjectLogicalRel.class, "r_f1=[$0]"),
                planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, r]], projects=[[0, 1]]")
            )
        );

        assertPlan(
            optimizeLogical("SELECT *, r.r_f2 FROM r"),
            plan(
                planRow(0, RootLogicalRel.class, ""),
                planRow(1, ProjectLogicalRel.class, "r_f1=[$0], r_f2=[$1]"),
                planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, r]], projects=[[0, 1]]")
            )
        );

        assertPlan(
            optimizeLogical("SELECT r.*, r.r_f2 FROM r"),
            plan(
                planRow(0, RootLogicalRel.class, ""),
                planRow(1, ProjectLogicalRel.class, "r_f1=[$0], r_f2=[$1]"),
                planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, r]], projects=[[0, 1]]")
            )
        );

        assertPlan(
            optimizeLogical("SELECT r.r_f2, r.* FROM r"),
            plan(
                planRow(0, RootLogicalRel.class, ""),
                planRow(1, ProjectLogicalRel.class, "r_f2=[$1], r_f1=[$0]"),
                planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, r]], projects=[[0, 1]]")
            )
        );
    }
}
