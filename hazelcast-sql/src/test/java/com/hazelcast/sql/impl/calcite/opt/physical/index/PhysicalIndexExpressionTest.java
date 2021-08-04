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
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
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
import static java.util.Collections.singletonList;

/**
 * Tests for different expression types.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category(ParallelJVMTest.class)
public class PhysicalIndexExpressionTest extends IndexOptimizerTestSupport {

    private static final String INDEX_NAME = "index";

    @Parameterized.Parameter
    public boolean hd;

    @Parameterized.Parameters(name = "hd:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true},
                {false},
        });
    }

    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        HazelcastTable pTable = OptimizerTestSupport.partitionedTable(
                "p",
                OptimizerTestSupport.fields("ret", INT, "f", INT),
                Collections.singletonList(new MapTableIndex(INDEX_NAME, IndexType.SORTED, 1, singletonList(1), singletonList(INT))),
                100,
                hd
        );

        tableMap.put("p", pTable);

        return new HazelcastSchema(tableMap);
    }

    @Test
    public void test_no_filter() {
        if (hd) {
            checkIndex("SELECT ret FROM p", INDEX_NAME, null, null);
        } else {
            checkNoIndex("SELECT ret FROM p");
        }
    }

    @Test
    public void test_column_literal() {
        checkIndexForCondition("f=1", "=($1, 1)");
        checkIndexForCondition("1=f", "=(1, $1)");
    }

    @Test
    public void test_range() {
        checkIndexForCondition("f>1", ">($1, 1)");
        checkIndexForCondition("f>=1", ">=($1, 1)");
        checkIndexForCondition("f<1", "<($1, 1)");
        checkIndexForCondition("f<=1", "<=($1, 1)");

        checkIndexForCondition("f>1 AND f<5", "AND(<($1, 5), >($1, 1))");
        checkIndexForCondition("f>1 AND f<=5", "AND(<=($1, 5), >($1, 1))");
        checkIndexForCondition("f>=1 AND f<5", "AND(<($1, 5), >=($1, 1))");
        checkIndexForCondition("f>=1 AND f<=5", "AND(<=($1, 5), >=($1, 1))");

        checkIndexForCondition("f>?", ">($1, ?0)");
        checkIndexForCondition("f>=?", ">=($1, ?0)");
        checkIndexForCondition("f<?", "<($1, ?0)");
        checkIndexForCondition("f<=?", "<=($1, ?0)");

        checkIndexForCondition("f>? AND f<?", "AND(>($1, ?0), <($1, ?1))");
        checkIndexForCondition("f>? AND f<=?", "AND(<=($1, ?1), >($1, ?0))");
        checkIndexForCondition("f>=? AND f<?", "AND(<($1, ?1), >=($1, ?0))");
        checkIndexForCondition("f>=? AND f<=?", "AND(<=($1, ?1), >=($1, ?0))");
    }

    @Test
    public void test_column_parameter() {
        checkIndexForCondition("f=?", "=($1, ?0)");
        checkIndexForCondition("?=f", "=(?0, $1)");

        checkIndexForCondition("f>?", ">($1, ?0)");
        checkIndexForCondition("?<f", "<(?0, $1)");

        checkIndexForCondition("f>=?", ">=($1, ?0)");
        checkIndexForCondition("?<=f", "<=(?0, $1)");

        checkIndexForCondition("f<?", "<($1, ?0)");
        checkIndexForCondition("?>f", ">(?0, $1)");

        checkIndexForCondition("f<=?", "<=($1, ?0)");
        checkIndexForCondition("?>=f", ">=(?0, $1)");
    }

    @Test
    public void test_column_expressionWithoutColumns() {
        checkIndexForCondition("f=(1+?)", "=(CAST($1):BIGINT(64), +(1:BIGINT(64), ?0))");
        checkIndexForCondition("(1+?)=f", "=(+(1:BIGINT(64), ?0), CAST($1):BIGINT(64))");
    }

    @Test
    public void test_column_expressionWithColumns() {
        checkNoIndexForCondition("f=(1+ret)", "=(CAST($1):BIGINT(32), +(1, CAST($0):BIGINT(32)))");
    }

    @Test
    public void test_or() {
        checkIndexForCondition("f=1 OR f=2", "OR(=($1, 1), =($1, 2))");
        checkIndexForCondition("f=1 OR f=2 OR f=3", "OR(=($1, 1), =($1, 2), =($1, 3))");
        checkIndexForCondition("f=1 OR (f=2 OR f=3)", "OR(=($1, 1), =($1, 2), =($1, 3))");

        checkIndexForCondition("f=1 OR f=?", "OR(=($1, 1), =($1, ?0))");
        checkIndexForCondition("f=? OR f=?", "OR(=($1, ?0), =($1, ?1))");

        checkNoIndexForCondition("f=1 OR ret=2", "OR(=($1, 1), =($0, 2))");
        checkNoIndexForCondition("f=1 OR f>2", "OR(=($1, 1), >($1, 2))");
    }

    @Test
    public void test_is_null() {
        checkIndexForCondition("f IS NULL", "IS NULL($1)");
        checkNoIndexForCondition("(f+?) IS NULL", "OR(IS NULL($1), IS NULL(?0))");
    }

    @Test
    public void test_not() {
        checkNoIndexForCondition("NOT f=?", "<>($1, ?0)");
    }

    @Test
    public void test_equals_and_range_combined() {
        checkIndexForCondition("f IS NULL OR f=1", "OR(IS NULL($1), =($1, 1))");
        checkNoIndexForCondition("f IS NULL OR f>1", "OR(IS NULL($1), >($1, 1))");
    }

    private void checkIndexForCondition(String condition, String expectedIndexFilter) {
        String sql = "SELECT ret FROM p WHERE " + condition;

        checkIndex(sql, INDEX_NAME, expectedIndexFilter, "null", INT, INT, INT, INT, INT);
    }

    private void checkNoIndexForCondition(String condition, String hdRemainderFilter) {
        String sql = "SELECT ret FROM p WHERE " + condition;

        if (hd) {
            checkIndex(sql, INDEX_NAME, null, hdRemainderFilter, INT, INT, INT, INT, INT);
        } else {
            checkNoIndex(sql, INT, INT, INT, INT, INT);
        }
    }
}
