/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryStoreSimpleTest extends HazelcastTestSupport {

    private TestEntryStore testEntryStore = new TestEntryStore();

    protected HazelcastInstance[] instances;

    protected IMap<String, String> map;

    @Before
    public void setup() {
        instances = createInstances();
        map = instances[0].getMap(randomMapName());
    }

    protected HazelcastInstance[] createInstances() {
        return createHazelcastInstanceFactory(1).newInstances(getConfig());
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true).setImplementation(testEntryStore);
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        return config;
    }

    @Test
    public void testPut() {
        map.put("key", "value");
        assertEntryStore("key", "value");
    }

    @Test
    public void testPut_withTtl() {
        map.put("key", "value", 10, TimeUnit.DAYS);
        assertEntryStore("key", "value", 10, TimeUnit.DAYS, 5000);
    }

    @Test
    public void testPut_withMaxIdle() {
        map.put("key", "value", 10, TimeUnit.DAYS, 5, TimeUnit.DAYS);
        assertEntryStore("key", "value", 5, TimeUnit.DAYS, 5000);
    }

    @Test
    public void testOverrideValueWithTtl() {
        map.put("key", "value", 10, TimeUnit.DAYS);
        map.put("key", "value2", 5, TimeUnit.DAYS);
        assertEntryStore("key", "value2", 5, TimeUnit.DAYS, 5000);
    }

    @Test
    public void testOverrideValueWithMaxIdle() {
        map.put("key", "value", 10, TimeUnit.DAYS, 5, TimeUnit.DAYS);
        map.put("key", "value2", 10, TimeUnit.DAYS, 1, TimeUnit.DAYS);
        assertEntryStore("key", "value2", 1, TimeUnit.DAYS, 5000);
    }

    @Test
    public void testPutAll() {
        Map<String, String> businessObjects = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            map.put("k" + i, "v" + i);
        }
        map.putAll(businessObjects);
        for (int i = 0; i < 100; i++) {
            assertEntryStore("k" + i, "v" + i);
        }
    }

    @Test
    public void testPutAsync() throws ExecutionException, InterruptedException {
        map.putAsync("key", "value").get();
        assertEntryStore("key", "value");
    }

    @Test
    public void testPutAsync_withTtl() throws ExecutionException, InterruptedException {
        map.putAsync("key", "value", 10, TimeUnit.DAYS).get();
        assertEntryStore("key", "value", 10, TimeUnit.DAYS, 5000);
    }

    @Test
    public void testPutAsync_withMaxIdle() throws ExecutionException, InterruptedException {
        map.putAsync("key", "value", 10, TimeUnit.DAYS, 5, TimeUnit.DAYS).get();
        assertEntryStore("key", "value", 5, TimeUnit.DAYS, 5000);
    }

    @Test
    public void testPutIfAbsent() {
        map.putIfAbsent("key", "value");
        assertEntryStore("key", "value");
    }

    @Test
    public void testPutIfAbsent_withTtl() {
        map.putIfAbsent("key", "value", 10, TimeUnit.DAYS);
        assertEntryStore("key", "value", 10, TimeUnit.DAYS, 5000);
    }

    @Test
    public void testPutIfAbsent_withMaxIdle() {
        map.putIfAbsent("key", "value", 10, TimeUnit.DAYS, 5, TimeUnit.DAYS);
        assertEntryStore("key", "value", 5, TimeUnit.DAYS, 5000);
    }

    @Test
    public void testRemove() {
        map.put("key", "value");
        map.remove("key");
        assertEntryStore("key", null);
    }

    @Test
    public void testRemoveIfSame() {
        map.put("key", "value");
        map.remove("key", "value");
        assertEntryStore("key", null);
    }

    @Test
    public void testRemoveAll() {
        final int mapSize = 100;
        for (int i = 0; i < mapSize; i++) {
            map.put("k" + i,  "v" + i);
        }
        map.removeAll(Predicates.alwaysTrue());
        for (int i = 0; i < mapSize; i++) {
            assertEntryStore("k" + i, null);
        }
    }

    @Test
    public void testRemoveAsync() throws ExecutionException, InterruptedException {
        map.put("key", "value");
        map.removeAsync("key").get();
        assertEntryStore("key", null);
    }

    @Test
    public void tesReplace() {
        map.put("key", "value");
        map.replace("key", "replaced");
        assertEntryStore("key", "replaced");
    }

    @Test
    public void testReplace_withTtl() {
        map.put("key", "value", 10, TimeUnit.DAYS);
        map.replace("key", "replaced");
        assertEntryStore("key", "replaced", 10, TimeUnit.DAYS, 5000);
    }

    @Test
    public void testReplaceIfSame() {
        map.put("key", "value");
        map.replace("key", "value", "replaced");
        assertEntryStore("key", "replaced");
    }

    @Test
    public void testReplaceIfSame_withTtl() {
        map.put("key", "value", 10, TimeUnit.DAYS);
        map.replace("key", "value", "replaced");
        assertEntryStore("key", "replaced", 10, TimeUnit.DAYS, 5000);
    }

    @Test
    public void testSet() {
        map.set("key", "value");
        assertEntryStore("key", "value");
    }

    @Test
    public void testSet_withTtl() {
        map.set("key", "value", 10, TimeUnit.DAYS);
        assertEntryStore("key", "value", 10, TimeUnit.DAYS, 5000);
    }

    @Test
    public void testSet_withMaxIdle() {
        map.set("key", "value", 10, TimeUnit.DAYS, 5, TimeUnit.DAYS);
        assertEntryStore("key", "value", 5, TimeUnit.DAYS, 5000);
    }

    @Test
    public void testSetAsync() throws ExecutionException, InterruptedException {
        map.setAsync("key", "value").get();
        assertEntryStore("key", "value");
    }

    @Test
    public void testSetAsync_withTtl() throws ExecutionException, InterruptedException {
        map.setAsync("key", "value", 10, TimeUnit.DAYS).get();
        assertEntryStore("key", "value", 10, TimeUnit.DAYS, 5000);
    }

    @Test
    public void testSetAsync_withMaxIdle() throws ExecutionException, InterruptedException {
        map.setAsync("key", "value", 10, TimeUnit.DAYS, 5, TimeUnit.DAYS).get();
        assertEntryStore("key", "value", 5, TimeUnit.DAYS, 5000);
    }

    @Test
    public void testSetTtl() {
        map.set("key", "value");
        map.setTtl("key", 1, TimeUnit.DAYS);
        assertEntryStore("key", "value", 1, TimeUnit.DAYS, 5000);
    }

    @Test
    public void testTryPut() {
        map.tryPut("key", "value", 10, TimeUnit.SECONDS);
        assertEntryStore("key", "value");
    }

    private void assertEntryStore(String key, String value) {
        TestEntryStore.Record record = testEntryStore.getRecord(key);
        if (value == null && record == null) {
            return;
        }
        assertEquals(value, record.value);
        assertEquals(-1, record.expirationTime);
    }

    private void assertEntryStore(String key, String value, long remainingTtl, TimeUnit timeUnit, long delta) {
        TestEntryStore.Record record = testEntryStore.getRecord(key);
        assertEquals(value, record.value);
        long expectedExpirationTime = System.currentTimeMillis() + timeUnit.toMillis(remainingTtl);
        assertBetween("expirationTime", record.expirationTime, expectedExpirationTime - delta, expectedExpirationTime + delta);
    }
}
