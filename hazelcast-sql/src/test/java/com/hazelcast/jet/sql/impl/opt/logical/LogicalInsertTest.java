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
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import junitparams.JUnitParamsRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;

@RunWith(JUnitParamsRunner.class)
public class LogicalInsertTest extends OptimizerTestSupport {

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    @Test
    public void test_requiresJob() {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, INT), field(VALUE, VARCHAR)), 0);
        assertPlan(
                optimizeLogical("INSERT INTO m VALUES (1, '1')", true, table),
                plan(
                        planRow(0, InsertLogicalRel.class),
                        planRow(1, ValuesLogicalRel.class)
                )
        );
    }

    @Test
    public void test_insertValues() {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, INT), field(VALUE, VARCHAR)), 0);
        assertPlan(
                optimizeLogical("INSERT INTO m VALUES (1, '1')", table),
                plan(
                        planRow(0, InsertMapLogicalRel.class)
                )
        );
    }

    @Test
    public void test_insertMultiValues() {
        HazelcastTable table = partitionedTable("m", asList(field(KEY, INT), field(VALUE, VARCHAR)), 0);
        assertPlan(
                optimizeLogical("INSERT INTO m VALUES (1, '1'), (2, '2')", table),
                plan(
                        planRow(0, InsertLogicalRel.class),
                        planRow(1, ValuesLogicalRel.class)
                )
        );
    }

    @Test
    public void test_insertSelect() {
        HazelcastTable target = partitionedTable("m1", asList(field(KEY, INT), field(VALUE, VARCHAR)), 0);
        HazelcastTable source = partitionedTable("m2", asList(field(KEY, INT), field(VALUE, VARCHAR)), 0);
        assertPlan(
                optimizeLogical("INSERT INTO m1 SELECT * FROM m2", target, source),
                plan(
                        planRow(0, InsertLogicalRel.class),
                        planRow(1, FullScanLogicalRel.class)
                )
        );
    }
}
