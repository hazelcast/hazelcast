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

package com.hazelcast.jet.sql.impl.connector.map.index;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

/**
 * Test of merge-sort correctness on DAG level (ordered edge).
 *
 * @see com.hazelcast.jet.sql.impl.opt.physical.CreateDagVisitor#onMapIndexScan
 */
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapSortedIndexScanIntegrationTest extends SqlTestSupport {
    private static final int ITEM_COUNT = 10_000;
    private static final String MAP_NAME = "map";

    private IMap<Integer, Integer> map;

    @BeforeClass
    public static void beforeClass() {
        initialize(3, smallInstanceConfig());
    }

    @Before
    public void before() {
        createMapping(MAP_NAME, int.class, int.class);
        map = instance().getMap(MAP_NAME);
    }

    @Test
    public void test_sorted() {
        List<Row> expected = new ArrayList<>();
        for (int i = 0; i <= ITEM_COUNT; i++) {
            map.put(i, i);
            expected.add(new Row(ITEM_COUNT - i, ITEM_COUNT - i));
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "this").setName(randomName());
        map.addIndex(indexConfig);

        // we'd try to break order correctness couple of time.
        for (int i = 0; i < 5; i++) {
            assertRowsOrdered("SELECT * FROM " + MAP_NAME + " ORDER BY this DESC", expected);
        }
    }
}
