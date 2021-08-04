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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Tests that ensure that hash indexes are preferred over sorted, and that bitmap indexes are no used.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category(ParallelJVMTest.class)
public class PhysicalIndexPreferenceTest extends IndexOptimizerTestSupport {
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
                OptimizerTestSupport.fields("ret", INT, "f1", INT, "f2", INT, "f3", INT),
                Arrays.asList(
                        new MapTableIndex("sorted_f1", IndexType.SORTED, 1, singletonList(1), singletonList(INT)),
                        new MapTableIndex("bitmap_f1", IndexType.BITMAP, 1, singletonList(1), singletonList(INT)),

                        new MapTableIndex("hash_f2", IndexType.HASH, 1, singletonList(2), singletonList(INT)),
                        new MapTableIndex("bitmap_f2", IndexType.BITMAP, 1, singletonList(2), singletonList(INT)),

                        new MapTableIndex("sorted_f3", IndexType.SORTED, 1, singletonList(3), singletonList(INT)),
                        new MapTableIndex("hash_f3", IndexType.HASH, 1, singletonList(3), singletonList(INT)),
                        new MapTableIndex("bitmap_f3", IndexType.BITMAP, 1, singletonList(3), singletonList(INT))
                ),
                100,
                hd
        );

        tableMap.put("p", pTable);

        return new HazelcastSchema(tableMap);
    }

    @Test
    public void test_sorted() {
        checkIndexForCondition("f1=1", "sorted_f1", "=($1, 1)");
    }

    @Test
    public void test_hash() {
        checkIndexForCondition("f2=1", "hash_f2", "=($2, 1)");
    }

    @Test
    public void test_hash_over_sorted() {
        checkIndexForCondition("f3=1", "hash_f3", "=($3, 1)");
    }

    private void checkIndexForCondition(
            String condition,
            String expectedIndex,
            String expectedIndexFilter
    ) {
        String sql = "SELECT ret FROM p WHERE " + condition;

        checkIndex(sql, expectedIndex, expectedIndexFilter, "null");
    }
}
