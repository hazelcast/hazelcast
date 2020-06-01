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

import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
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
public class LogicalJoinFilterTest extends LogicalOptimizerTestSupport {
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
        RelNode rootInput = optimizeLogical(
            "SELECT r.r_f1, s.s_f1 FROM r INNER JOIN s ON r.r_f2 = s.s_f2 AND r.r_f3 = 1 AND s.s_f3 = 2"
        );

        checkJoinFilterPush(rootInput);
    }

    @Test
    public void testJoinWhereFilterPush() {
        RelNode rootInput = optimizeLogical(
            "SELECT r.r_f1, s.s_f1 FROM r, s WHERE r.r_f2 = s.s_f2 AND r.r_f3 = 1 AND s.s_f3 = 2"
        );

        checkJoinFilterPush(rootInput);
    }

    @Test
    public void testJoinOnAndWhereFilterPush() {
        RelNode rootInput = optimizeLogical(
            "SELECT r.r_f1, s.s_f1 FROM r INNER JOIN s ON r.r_f2 = s.s_f2 AND r.r_f3 = 1 WHERE s.s_f3 = 2"
        );

        checkJoinFilterPush(rootInput);
    }

    private void checkJoinFilterPush(RelNode rootInput) {
        ProjectLogicalRel project = assertProject(
            rootInput,
            OptimizerTestSupport.list(
                OptimizerTestSupport.column(0),
                OptimizerTestSupport.column(2)
            )
        );

        JoinLogicalRel join = assertJoin(
            project.getInput(),
            JoinRelType.INNER,
            OptimizerTestSupport.compare(OptimizerTestSupport.column(1), OptimizerTestSupport.column(3), ComparisonMode.EQUALS)
        );

        assertScan(
            join.getLeft(),
            "r",
            OptimizerTestSupport.list(0, 1),
            OptimizerTestSupport.compare(OptimizerTestSupport.column(2), OptimizerTestSupport.constant(1), ComparisonMode.EQUALS)
        );

        assertScan(
            join.getRight(),
            "s",
            OptimizerTestSupport.list(0, 1),
            OptimizerTestSupport.compare(OptimizerTestSupport.column(2), OptimizerTestSupport.constant(2), ComparisonMode.EQUALS)
        );
    }
}
