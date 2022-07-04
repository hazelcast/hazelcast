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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import org.junit.Test;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;

public class PhysicalJoinTest extends OptimizerTestSupport {

    @Test
    public void when_isSimpleJoin_then_useNestedLoopJoin() {
        HazelcastTable tableLeft = partitionedTable("l", asList(field(KEY, INT), field(VALUE, INT)), 1);
        HazelcastTable tableRight = partitionedTable("r", asList(field(KEY, INT), field(VALUE, INT)), 1);

        String query = "SELECT * FROM l JOIN r ON l.__key = r.__key";
        assertPlan(
                optimizePhysical(query, asList(INT, INT, INT, INT), tableLeft, tableRight).getPhysical(),
                plan(
                        planRow(0, JoinNestedLoopPhysicalRel.class),
                        planRow(1, FullScanPhysicalRel.class),
                        planRow(1, FullScanPhysicalRel.class)
                )
        );
    }

    @Test
    public void when_rightChildIsNotTableScan_then_useHashJoin() {
        HazelcastTable tableLeft = partitionedTable("l", asList(field(KEY, INT), field(VALUE, INT)), 1);
        HazelcastTable tableRight = partitionedTable("r", asList(field(KEY, INT), field(VALUE, INT)), 1);

        String query = "SELECT * FROM l WHERE EXISTS (SELECT 1 FROM r WHERE l.__key = r.__key)";
        assertPlan(
                optimizePhysical(query, asList(), tableLeft, tableRight).getPhysical(),
                plan(
                        planRow(0, CalcPhysicalRel.class),
                        planRow(1, JoinHashPhysicalRel.class),
                        planRow(2, FullScanPhysicalRel.class),
                        planRow(2, AggregateCombineByKeyPhysicalRel.class),
                        planRow(3, AggregateAccumulateByKeyPhysicalRel.class),
                        planRow(4, FullScanPhysicalRel.class)
                )
        );
    }
}
