/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.optimizer;

import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.statistics.TableStatistics;
import com.hazelcast.sql.optimizer.support.LogicalOptimizerTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for join filter pushdown.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LogicalOptimizerJoinFilterTest extends LogicalOptimizerTestSupport {
    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("r", new HazelcastTable("r", true, null, null, new TableStatistics(100)));
        tableMap.put("s", new HazelcastTable("s", true, null, null, new TableStatistics(100)));

        tableMap.put("person", new HazelcastTable("person", true, null, null, new TableStatistics(100)));
        tableMap.put("department", new HazelcastTable("department", true, null, null, new TableStatistics(100)));

        return new HazelcastSchema(tableMap);
    }

    @Test
    public void testJoinOnFilterPush() {
        RelNode rootInput = optimizeLogical(
            "SELECT r.r_f1, s.s_f1 FROM r INNER JOIN s ON r.r_f2 = s.s_f2 AND r.r_f3 = 1 AND s.s_f3 = 2"
        );

        // TODO: Implement me.
        System.out.println(rootInput);
    }

    @Test
    public void testJoinWhereFilterPush() {
        RelNode rootInput = optimizeLogical(
            "SELECT r.r_f1, s.s_f1 FROM r, s WHERE r.r_f2 = s.s_f2 AND r.r_f3 = 1 AND s.s_f3 = 2"
        );

        // TODO: Implement me.
        System.out.println(rootInput);
    }

    @Test
    public void testJoinOnAndWhereFilterPush() {
        RelNode rootInput = optimizeLogical(
            "SELECT r.r_f1, s.s_f1 "
                + "FROM r INNER JOIN s ON r.r_f2 = s.s_f2 AND r.r_f3 = 1 AND s.s_f3 = 2"
                + "WHERE s.s_f4 = 3 AND r.r_f4 = 4");

        // TODO: Implement me.
        System.out.println(rootInput);
    }
}
