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
import com.hazelcast.sql.impl.calcite.opt.physical.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ProjectPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.SortPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.SortMergeExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
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
@Category(ParallelJVMTest.class)
public class PhysicalSortIndexTest extends IndexOptimizerTestSupport {

    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        HazelcastTable pTable = partitionedTable(
                "p",
                fields("ret", INT, "f1", INT, "f2", INT, "f3", INT),
                Arrays.asList(
                        new MapTableIndex("sorted_f1", IndexType.SORTED, 1, singletonList(1), singletonList(INT)),
                        new MapTableIndex("sorted_f1_f3", IndexType.SORTED, 2, asList(1, 3), asList(INT, INT)),
                        new MapTableIndex("sorted_f1_f2", IndexType.HASH, 2, asList(1, 2), asList(INT, INT))
                ),
                100,
                false
        );

        tableMap.put("p", pTable);

        return new HazelcastSchema(tableMap);
    }

    @Test
    public void testTrivialSort() {
        assertPlan(
                optimizePhysical("SELECT f1, f2, f3 FROM p ORDER BY f1", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0]], fetch=[null], offset=[null]", 100d),
                        planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testSortAndFetchAndOffset() {
        assertPlan(
                optimizePhysical("SELECT f1, f2, f3 FROM p ORDER BY f1 OFFSET 20 ROWS FETCH FIRST 10 ROWS ONLY", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0]], fetch=[10], offset=[20]", 100d),
                        planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testFetchAndOffset() {
        assertPlan(
                optimizePhysical("SELECT f1, f2, f3 FROM p OFFSET 20 ROWS FETCH FIRST 10 ROWS ONLY", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[]], fetch=[10], offset=[20]", 100d),
                        planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 3]]]]", 100d)
                )
        );
    }

    @Test
    public void testSortAndLookup() {
        assertPlan(
                optimizePhysical("SELECT f1, f2, f3 FROM p WHERE f1 = 1 ORDER BY f1, f3", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 15d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0, 2]], fetch=[null], offset=[null]", 15d),
                        planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 3]]]], index=[sorted_f1_f3], indexExp=[=($1, 1)], remainderExp=[null]", 15d)
                )
        );
    }

    @Test
    public void testSortAndLookupAndFetchOffset() {
        assertPlan(
                optimizePhysical("SELECT f1, f2, f3 FROM p WHERE f1 = 1 ORDER BY f1, f3 OFFSET 20 ROWS FETCH FIRST 10 ROWS ONLY", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 15d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0, 2]], fetch=[10], offset=[20]", 15d),
                        planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 3]]]], index=[sorted_f1_f3], indexExp=[=($1, 1)], remainderExp=[null]", 15d)
                )
        );
    }

    @Test
    public void testLookupAndFetchOffset() {
        assertPlan(
                optimizePhysical("SELECT f1, f2, f3 FROM p WHERE f1 = 1 AND f3 = 5 OFFSET 20 ROWS FETCH FIRST 10 ROWS ONLY", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 2.2d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[]], fetch=[10], offset=[20]", 2.2d),
                        planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 3]]]], index=[sorted_f1_f3], indexExp=[AND(=($3, 5), =($1, 1))], remainderExp=[null]", 2.2d)
                )
        );
    }

    @Test
    public void testTrivialSortDescending() {
        assertPlan(
                optimizePhysical("SELECT f1, f2, f3 FROM p ORDER BY f1 DESC", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0 DESC]], fetch=[null], offset=[null]", 100d),
                        planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testTrivialSortWithFetch() {
        assertPlan(
                optimizePhysical("SELECT f1, f2, f3 FROM p ORDER BY f1 OFFSET 4090 FETCH NEXT 5 ROWS ONLY", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0]], fetch=[5], offset=[4090]", 100d),
                        planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testCompositeSort() {
        assertPlan(
                optimizePhysical("SELECT f1, f3 FROM p ORDER BY f1, f3", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0, 1]], fetch=[null], offset=[null]", 100d),
                        planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testCompositeSortAndLookup() {
        assertPlan(
                optimizePhysical("SELECT f1, f3 FROM p WHERE f1 = 1 ORDER BY f1, f3", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 15d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0, 1]], fetch=[null], offset=[null]", 15d),
                        planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]], index=[sorted_f1_f3], indexExp=[=($1, 1)], remainderExp=[null]", 15d)
                )
        );
    }


    @Test
    public void testCompositeSortNoMerge() {
        assertPlan(
                optimizePhysical("SELECT f1, f3 FROM p ORDER BY f1, f3", 1),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortPhysicalRel.class, "sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC], requiresSort=[false]", 100d),
                        planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testCompositeSortWithProject2() {
        assertPlan(
                optimizePhysical("SELECT f1 + f3 FROM p ORDER BY f1, f3", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, ProjectPhysicalRel.class, "EXPR$0=[$0]"),
                        planRow(2, SortMergeExchangePhysicalRel.class, "collation=[[1, 2]], fetch=[null], offset=[null]", 100d),
                        planRow(3, ProjectPhysicalRel.class, "EXPR$0=[+(CAST($0):BIGINT(32), CAST($1):BIGINT(32))], f1=[$0], f3=[$1]"),
                        planRow(4, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testCompositeSortWithProject3() {
        assertPlan(
                optimizePhysical("SELECT f1, f1 + f3, f3 FROM p ORDER BY f1, f3", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0, 2]], fetch=[null], offset=[null]", 100d),
                        planRow(2, ProjectPhysicalRel.class, "f1=[$0], EXPR$1=[+(CAST($0):BIGINT(32), CAST($1):BIGINT(32))], f3=[$1]"),
                        planRow(3, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testCompositeSortWithProject4() {
        assertPlan(
                optimizePhysical("SELECT a, b FROM (SELECT f1+f3 a, f1-f3 b FROM p)"
                        + "ORDER BY a, b", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0, 1]], fetch=[null], offset=[null]"),
                        planRow(2, SortPhysicalRel.class, "sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC], requiresSort=[true]", 100d),
                        planRow(3, ProjectPhysicalRel.class, "a=[+(CAST($0):BIGINT(32), CAST($1):BIGINT(32))], b=[-(CAST($0):BIGINT(32), CAST($1):BIGINT(32))]"),
                        planRow(4, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]]", 100d)
                )
        );
    }

    @Test
    public void testCompositeSortWithProjectNoMerge() {
        assertPlan(
                optimizePhysical("SELECT f1 + f3 FROM p ORDER BY f1, f3", 1),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, ProjectPhysicalRel.class, "EXPR$0=[$0]"),
                        planRow(2, SortPhysicalRel.class, "sort0=[$1], sort1=[$2], dir0=[ASC], dir1=[ASC], requiresSort=[false]", 100d),
                        planRow(3, ProjectPhysicalRel.class, "EXPR$0=[+(CAST($0):BIGINT(32), CAST($1):BIGINT(32))], f1=[$0], f3=[$1]"),
                        planRow(4, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testCompositeSortWithProjectAndFetchOffsetNoMerge() {
        assertPlan(
                optimizePhysical("SELECT f1 + f3 FROM p ORDER BY f1, f3 OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", 1),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 20d),
                        planRow(1, ProjectPhysicalRel.class, "EXPR$0=[$0]"),
                        planRow(2, SortPhysicalRel.class, "sort0=[$1], sort1=[$2], dir0=[ASC], dir1=[ASC], offset=[10], fetch=[20], requiresSort=[false]", 20d),
                        planRow(3, ProjectPhysicalRel.class, "EXPR$0=[+(CAST($0):BIGINT(32), CAST($1):BIGINT(32))], f1=[$0], f3=[$1]"),
                        planRow(4, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testProjectAndFetchOffsetNoMerge() {
        assertPlan(
                optimizePhysical("SELECT f1 + f3 FROM p OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY", 1),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 20d),
                        planRow(1, SortPhysicalRel.class, "offset=[10], fetch=[20], requiresSort=[false]", 20d),
                        planRow(2, ProjectPhysicalRel.class, "EXPR$0=[+(CAST($0):BIGINT(32), CAST($1):BIGINT(32))]"),
                        planRow(3, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]]", 100d)
                )
        );
    }

    @Test
    public void testCompositeSortWithProjectManyNodes() {
        assertPlan(
                optimizePhysical("SELECT f1 + f3 FROM p ORDER BY f1, f3", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, ProjectPhysicalRel.class, "EXPR$0=[$0]"),
                        planRow(2, SortMergeExchangePhysicalRel.class, "collation=[[1, 2]], fetch=[null], offset=[null]", 100d),
                        planRow(3, ProjectPhysicalRel.class, "EXPR$0=[+(CAST($0):BIGINT(32), CAST($1):BIGINT(32))], f1=[$0], f3=[$1]"),
                        planRow(4, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
                )
        );
    }

    @Test
    public void testSortNoIndexWithCollation() {
        assertPlan(
                optimizePhysical("SELECT f1, f2 FROM p WHERE f1 = 1 ORDER BY f1, f2", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 15d),
                        planRow(1, SortMergeExchangePhysicalRel.class, "collation=[[0, 1]], fetch=[null], offset=[null]", 15d),
                        planRow(2, SortPhysicalRel.class, "sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC], requiresSort=[true]", 15d),
                        planRow(3, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2]]]], index=[sorted_f1], indexExp=[=($1, 1)], remainderExp=[null]", 15d)
                )
        );
    }

    @Test
    public void testSortNoIndexWithCollationHashNotUsed() {
        assertPlan(
                optimizePhysical("SELECT f1 FROM p ORDER BY f1, f2", 2),
                plan(
                        planRow(0, RootPhysicalRel.class, "", 100d),
                        planRow(1, ProjectPhysicalRel.class, "f1=[$0]", 100d),
                        planRow(2, SortMergeExchangePhysicalRel.class, "collation=[[0, 1]], fetch=[null], offset=[null]", 100d),
                        planRow(3, SortPhysicalRel.class, "sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC], requiresSort=[true]", 100d),
                        planRow(4, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2]]]]", 100d)
                )
        );
    }
}
