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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.physical.AggregateAccumulateByKeyPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.AggregateCombineByKeyPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.CalcPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;

public class LogicalStreamAggTest extends OptimizerTestSupport {
    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Test
    public void test() {
        HazelcastTable table = partitionedTable("map", asList(field(KEY, INT), field(VALUE, INT)), 1);
        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, QueryDataType.INT);

        final String sql = "SELECT window_start, window_end, SUM(__key) FROM " +
                "TABLE(HOP(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER((SELECT __key, this FROM map), DESCRIPTOR(__key), 1))), " +
                "DESCRIPTOR(__key), 2, 1)) " +
                "GROUP BY window_start, window_end, __key, this";

        assertPlan(optimizePhysical(sql, parameterTypes, table).getPhysical(), plan(
                planRow(0, CalcPhysicalRel.class),
                planRow(1, AggregateCombineByKeyPhysicalRel.class),
                planRow(2, AggregateAccumulateByKeyPhysicalRel.class),
                planRow(3, CalcPhysicalRel.class),
                planRow(4, SlidingWindowPhysicalRel.class),
                planRow(5, FullScanPhysicalRel.class)
        ));
    }
}
