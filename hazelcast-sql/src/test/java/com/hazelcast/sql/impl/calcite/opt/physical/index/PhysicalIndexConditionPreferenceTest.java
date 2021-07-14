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
import com.hazelcast.test.annotation.QuickTest;
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
 * Tests that ensure how different conditions are preferred one over the other
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PhysicalIndexConditionPreferenceTest extends IndexOptimizerTestSupport {

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
    public void test_equality_preference() {
        // Prefer over IN
        checkIndexForCondition("f=? AND (f=? OR f=?)", "=($1, ?0)", "OR(=($1, ?1), =($1, ?2))");

        // Prefer over range
        checkIndexForCondition("f=? AND f>?", "=($1, ?0)", ">($1, ?1)");
        checkIndexForCondition("f=? AND f>=?", "=($1, ?0)", ">=($1, ?1)");
        checkIndexForCondition("f=? AND f<?", "=($1, ?0)", "<($1, ?1)");
        checkIndexForCondition("f=? AND f<=?", "=($1, ?0)", "<=($1, ?1)");

        checkIndexForCondition("f=? AND f>? AND f<?", "=($1, ?0)", "AND(>($1, ?1), <($1, ?2))");
        checkIndexForCondition("f=? AND f>? AND f<=?", "=($1, ?0)", "AND(>($1, ?1), <=($1, ?2))");
        checkIndexForCondition("f=? AND f>=? AND f<?", "=($1, ?0)", "AND(>=($1, ?1), <($1, ?2))");
        checkIndexForCondition("f=? AND f>=? AND f<=?", "=($1, ?0)", "AND(>=($1, ?1), <=($1, ?2))");
    }

    @Test
    public void test_in_preference() {
        checkIndexForCondition("(f=? OR f=?) AND f>?", "OR(=($1, ?0), =($1, ?1))", ">($1, ?2)");
        checkIndexForCondition("(f=? OR f=?) AND f>=?", "OR(=($1, ?0), =($1, ?1))", ">=($1, ?2)");
        checkIndexForCondition("(f=? OR f=?) AND f<?", "OR(=($1, ?0), =($1, ?1))", "<($1, ?2)");
        checkIndexForCondition("(f=? OR f=?) AND f<=?", "OR(=($1, ?0), =($1, ?1))", "<=($1, ?2)");

        checkIndexForCondition("(f=? OR f=?) AND f>? AND f<?", "OR(=($1, ?0), =($1, ?1))", "AND(>($1, ?2), <($1, ?3))");
        checkIndexForCondition("(f=? OR f=?) AND f>? AND f<=?", "OR(=($1, ?0), =($1, ?1))", "AND(>($1, ?2), <=($1, ?3))");
        checkIndexForCondition("(f=? OR f=?) AND f>=? AND f<?", "OR(=($1, ?0), =($1, ?1))", "AND(>=($1, ?2), <($1, ?3))");
        checkIndexForCondition("(f=? OR f=?) AND f>=? AND f<=?", "OR(=($1, ?0), =($1, ?1))", "AND(>=($1, ?2), <=($1, ?3))");
    }

    private void checkIndexForCondition(
            String condition,
            String expectedIndexFilter,
            String expectedRemainderFilter
    ) {
        String sql = "SELECT ret FROM p WHERE " + condition;

        checkIndex(sql, INDEX_NAME, expectedIndexFilter, expectedRemainderFilter, INT, INT, INT, INT, INT);
    }
}
