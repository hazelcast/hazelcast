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

package com.hazelcast.jet.sql.impl.opt;

import com.hazelcast.jet.sql.impl.opt.logical.AggregateLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.CalcLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;

public class CalcOptimizerTest extends OptimizerTestSupport {
    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Test
    public void test() {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, INT), field(VALUE, VARCHAR)), 10);
        assertPlan(
                optimizeLogical("SELECT a FROM (SELECT AVG(__key) AS a FROM m) t WHERE a > 10", table),
                plan(
                        planRow(0, CalcLogicalRel.class),
                        planRow(1, AggregateLogicalRel.class),
                        planRow(2, FullScanLogicalRel.class)
                )
        );
    }
}
