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
import com.hazelcast.sql.impl.calcite.opt.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.SortMergeExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.schema.Table;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PhysicalSortIndexPreferenceTest extends IndexOptimizerTestSupport {

    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        HazelcastTable pTable = partitionedTable(
                "p",
                fields("ret", INT, "f1", INT, "f2", INT, "f3", INT),
                Arrays.asList(
                        new MapTableIndex("sorted_f1", IndexType.SORTED, 1, singletonList(1), singletonList(INT)),
                        new MapTableIndex("sorted_f1_f2", IndexType.SORTED, 2, asList(1, 2), asList(INT, INT)),
                        new MapTableIndex("sorted_f1_f2_f3", IndexType.SORTED, 3, asList(1, 2, 3), asList(INT, INT)),
                        new MapTableIndex("sorted_f2_f3", IndexType.SORTED, 2, asList(2, 3), asList(INT, INT))


                ),
                100,
                false
        );

        tableMap.put("p", pTable);

        return new HazelcastSchema(tableMap);
    }

    @Test
    public void testSortIndexSelection() {
        assertPlan(
                optimizePhysical("SELECT f1, f2, f3 FROM p ORDER BY f1", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0]], fetch=[null], offset=[null]", 100d),
                        planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 3]]]], index=[sorted_f1_f2_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testSortIndexSelection2() {
        assertPlan(
                optimizePhysical("SELECT f1, f2, f3 FROM p ORDER BY f2, f3", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[1, 2]], fetch=[null], offset=[null]", 100d),
                        planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 3]]]], index=[sorted_f2_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }


}
