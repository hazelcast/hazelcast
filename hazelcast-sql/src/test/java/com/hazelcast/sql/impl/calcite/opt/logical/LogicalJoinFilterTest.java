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

import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.schema.Table;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;

/**
 * Tests for join filter pushdown.
 */
// TODO: More tests with different join types and different expressions and permutations
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LogicalJoinFilterTest extends OptimizerTestSupport {
    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("r", OptimizerTestSupport.partitionedTable("r", OptimizerTestSupport.fields("r_f1", INT, "r_f2", INT, "r_f3", INT), null, 100));
        tableMap.put("s", OptimizerTestSupport.partitionedTable("s", OptimizerTestSupport.fields("s_f1", INT, "s_f2", INT, "s_f3", INT), null, 100));

        return new HazelcastSchema(tableMap);
    }

    /**
     * Make sure that join conditions are pushed down to the underlying operators.
     */
    @Test
    public void testJoinOnFilterPush() {
        checkJoinFilterPush("SELECT r.r_f1, s.s_f1 FROM r INNER JOIN s ON r.r_f2 = s.s_f2 AND r.r_f3 = 1 AND s.s_f3 = 2");
    }

    @Test
    public void testJoinWhereFilterPush() {
        checkJoinFilterPush("SELECT r.r_f1, s.s_f1 FROM r, s WHERE r.r_f2 = s.s_f2 AND r.r_f3 = 1 AND s.s_f3 = 2");
    }

    @Test
    public void testJoinOnAndWhereFilterPush() {
        checkJoinFilterPush("SELECT r.r_f1, s.s_f1 FROM r INNER JOIN s ON r.r_f2 = s.s_f2 AND r.r_f3 = 1 WHERE s.s_f3 = 2");
    }

    private void checkJoinFilterPush(String sql) {
        assertPlan(
            optimizeLogical(sql),
            plan(
                planRow(0, RootLogicalRel.class, "", 33.8d),
                planRow(1, ProjectLogicalRel.class, "r_f1=[$0], s_f1=[$2]", 33.8d),
                planRow(2, JoinLogicalRel.class, "condition=[=($1, $3)], joinType=[inner]", 33.8d),
                planRow(3, MapScanLogicalRel.class, "table=[[hazelcast, r[projects=[0, 1], filter==($2, 1)]]]", 15d),
                planRow(3, MapScanLogicalRel.class, "table=[[hazelcast, s[projects=[0, 1], filter==($2, 2)]]]", 15d)
            )
        );
    }
}
