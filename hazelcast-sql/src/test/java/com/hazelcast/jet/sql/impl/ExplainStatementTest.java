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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.config.IndexType;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExplainStatementTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    // TODO: ORDER BY unsupported?

    @Test
    public void test_explainStatementBase() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 10);

        String sql = "EXPLAIN PLAN FOR SELECT * FROM map";

        createMapping("map", Integer.class, Integer.class);
        assertRowsAnyOrder(sql, singletonList(
                new Row("FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[0, 1]]]])")
        ));
    }

    @Test
    public void test_explainStatementIndexScan() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 10);
        map.put(2, 10);
        map.addIndex(IndexType.HASH, "this");

        String sql = "EXPLAIN PLAN FOR SELECT * FROM map WHERE this = 10 ";

        createMapping("map", Integer.class, Integer.class);
        assertRowsAnyOrder(sql, singletonList(
                new Row("IndexScanMapPhysicalRel(table=[[hazelcast, public, map[projects=[0, 1]]]], " +
                        "index=[map_hash_this], indexExp=[=($1, 10)], remainderExp=[null])")
        ));
    }

    @Test
    public void test_explainStatementSelectBelowUnion() {
        IMap<Integer, Integer> map = instance().getMap("map");
        map.put(1, 10);

        String sql = "EXPLAIN PLAN FOR SELECT * FROM map UNION ALL SELECT * FROM map";

        createMapping("map", Integer.class, Integer.class);
        assertRowsAnyOrder(sql, asList(
                new Row("UnionPhysicalRel(all=[true])"),
                new Row("  FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[0, 1]]]])"),
                new Row("  FullScanPhysicalRel(table=[[hazelcast, public, map[projects=[0, 1]]]])")
        ));
    }

}
