/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.index;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.exec.scan.index.MapIndexScanExec;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlIndexTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";
    private static final String SORTED_INDEX_NAME = "sorted";
    private static final String HASH_INDEX_NAME = "hash";

    private static final TestHazelcastFactory FACTORY = new TestHazelcastFactory(2);
    private static HazelcastInstance member;

    @BeforeClass
    public static void beforeClass() {
        member = FACTORY.newHazelcastInstance();

        IMap<Integer, Integer>  intMap = member.getMap(MAP_NAME);
        intMap.addIndex(new IndexConfig().setName(SORTED_INDEX_NAME).setType(IndexType.SORTED).addAttribute("this"));
        intMap.addIndex(new IndexConfig().setName(HASH_INDEX_NAME).setType(IndexType.HASH).addAttribute("__key"));

        Map<Integer, Integer> localMap = new HashMap<>();

        for (int i = 0; i < 100; i++) {
            localMap.put(i, i);
        }

        intMap.putAll(localMap);
    }

    @AfterClass
    public static void afterClass() {
        FACTORY.shutdownAll();
    }

    @Test
    public void testEquals_sorted() {
        checkIntRange(executeWithIndex(MAP_NAME, SORTED_INDEX_NAME, "this = 50"), IntStream.range(50, 51));
    }

    @Test
    public void testIn_sorted() {
        checkIntRange(executeWithIndex(MAP_NAME, SORTED_INDEX_NAME, "this = 50 OR this = 51"), IntStream.range(50, 52));
        checkIntRange(executeWithIndex(MAP_NAME, SORTED_INDEX_NAME, "this IN (50, 51)"), IntStream.range(50, 52));
    }

    @Test
    public void testRange_sorted() {
        checkIntRange(executeWithIndex(MAP_NAME, SORTED_INDEX_NAME, "this > 50"), IntStream.range(51, 100));
        checkIntRange(executeWithIndex(MAP_NAME, SORTED_INDEX_NAME, "this >= 50"), IntStream.range(50, 100));
        checkIntRange(executeWithIndex(MAP_NAME, SORTED_INDEX_NAME, "this < 50"), IntStream.range(0, 50));
        checkIntRange(executeWithIndex(MAP_NAME, SORTED_INDEX_NAME, "this <= 50"), IntStream.range(0, 51));
        checkIntRange(executeWithIndex(MAP_NAME, SORTED_INDEX_NAME, "this > 25 AND this < 75"), IntStream.range(26, 75));
        checkIntRange(executeWithIndex(MAP_NAME, SORTED_INDEX_NAME, "this > 25 AND this <= 75"), IntStream.range(26, 76));
        checkIntRange(executeWithIndex(MAP_NAME, SORTED_INDEX_NAME, "this >= 25 AND this < 75"), IntStream.range(25, 75));
        checkIntRange(executeWithIndex(MAP_NAME, SORTED_INDEX_NAME, "this >= 25 AND this <= 75"), IntStream.range(25, 76));
    }

    @Test
    public void testEquals_hash() {
        checkIntRange(executeWithIndex(MAP_NAME, HASH_INDEX_NAME, "__key = 50"), IntStream.range(50, 51));
    }

    @Test
    public void testIn_hash() {
        checkIntRange(executeWithIndex(MAP_NAME, HASH_INDEX_NAME, "__key = 50 OR __key = 51"), IntStream.range(50, 52));
        checkIntRange(executeWithIndex(MAP_NAME, HASH_INDEX_NAME, "__key IN (50, 51)"), IntStream.range(50, 52));
    }

    private static void checkIntRange(List<Integer> rows, IntStream expectedStream) {
        int expectedSize = 0;

        for (int value : expectedStream.toArray()) {
            assertTrue("Cannot find value: " + value, rows.contains(value));

            expectedSize++;
        }

        assertEquals(expectedSize, rows.size());
    }

    private <T> List<T> executeWithIndex(String mapName, String indexName, String condition) {
        AtomicReference<MapIndexScanExec> lastIndexExecRef = new AtomicReference<>();

        setExecHook(member, exec -> {
            if (exec instanceof MapIndexScanExec) {
                lastIndexExecRef.set((MapIndexScanExec) exec);
            }

            return exec;
        });

        String sql = "SELECT this FROM " + mapName + " WHERE " + condition;

        List<T> res = new ArrayList<>();

        for (SqlRow row : execute(member, sql)) {
            T value = row.getObject(0);

            res.add(value);
        }

        MapIndexScanExec lastIndexExec = lastIndexExecRef.get();

        assertNotNull("Index was not used for the request", lastIndexExec);
        assertEquals(indexName, lastIndexExec.getIndexName());

        return res;
    }
}
