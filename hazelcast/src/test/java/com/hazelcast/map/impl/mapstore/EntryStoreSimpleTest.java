/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryStoreSimpleTest extends HazelcastTestSupport {

    @Parameters(name = "inMemoryFormat: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT}
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    protected TestEntryStore<String, String> testEntryStore = new TestEntryStore<>();

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
        Config config = smallInstanceConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true).setImplementation(testEntryStore);
        config.getMapConfig("default")
                .setMapStoreConfig(mapStoreConfig)
                .setInMemoryFormat(inMemoryFormat);
        return config;
    }

    @Test
    public void testPut() {
        map.put("key", "value");
        assertEntryStore("key", "value");
    }

    @Test
    public void testPut_returns_old_value_from_entry_store() {
        // 1. Insert value into entry-store
        map.put("key", "value1");

        // 2. Then remove it only from memory, entry still exists in entry-store
        map.evict("key");

        // 3. Update entry value to a new one
        String old = map.put("key", "value2");

        // 4. Expect we got correct old value
        assertEquals("value1", old);
    }

    @Test
    public void testReplace_returns_old_value_from_entry_store() {
        // 1. Insert value into entry-store
        map.put("key", "value1");

        // 2. Then remove it only from memory, entry still exists in entry-store
        map.evict("key");

        // 3. Replace entry value with a new one
        String old = map.replace("key", "value2");

        // 4. Expect we got correct old value
        assertEquals("value1", old);
    }

    @Test
    public void testPut_withTtl() {
        map.put("key", "value", 10, TimeUnit.DAYS);
        assertEntryStore("key", "value", 10, TimeUnit.DAYS, 10000);
    }

    @Test
    public void testPut_withMaxIdle() {
        map.put("key", "value", 10, TimeUnit.DAYS, 5, TimeUnit.DAYS);
        assertEntryStore("key", "value", 5, TimeUnit.DAYS, 10000);
    }

    @Test
    public void testOverrideValueWithTtl() {
        map.put("key", "value", 10, TimeUnit.DAYS);
        map.put("key", "value2", 5, TimeUnit.DAYS);
        assertEntryStore("key", "value2", 5, TimeUnit.DAYS, 10000);
    }

    @Test
    public void testOverrideValueWithMaxIdle() {
        map.put("key", "value", 10, TimeUnit.DAYS, 5, TimeUnit.DAYS);
        map.put("key", "value2", 10, TimeUnit.DAYS, 1, TimeUnit.DAYS);
        assertEntryStore("key", "value2", 1, TimeUnit.DAYS, 10000);
    }

    @Test
    public void testPutAll_WithoutMapListener() {
        final int max = 100;
        final Map<String, String> businessObjects = IntStream.range(0, max).boxed()
                .collect(Collectors.toMap(i -> "k" + i, i -> "v" + i));

        map.putAll(businessObjects);

        IntStream.range(0, max).forEach(i -> assertEntryStore("k" + i, "v" + i));
        assertEquals(0, testEntryStore.getLoadCallCount());
    }

    @Test
    public void testPutAll_WithMapListener() {
        final int max = 100;
        final Map<String, String> businessObjects = IntStream.range(0, max).boxed()
                .collect(Collectors.toMap(i -> "k" + i, i -> "v" + i));

        final CountDownLatch latch = new CountDownLatch(max);
        map.addEntryListener((EntryAddedListener) event -> latch.countDown(), true);

        map.putAll(businessObjects);

        IntStream.range(0, max).forEach(i -> assertEntryStore("k" + i, "v" + i));
        assertEquals(max, testEntryStore.getLoadCallCount());
        assertOpenEventually(latch);
    }

    @Test
    public void testPutAsync() throws ExecutionException, InterruptedException {
        map.putAsync("key", "value").toCompletableFuture().get();
        assertEntryStore("key", "value");
    }

    @Test
    public void testPutAsync_withTtl() throws ExecutionException, InterruptedException {
        map.putAsync("key", "value", 10, TimeUnit.DAYS).toCompletableFuture().get();
        assertEntryStore("key", "value", 10, TimeUnit.DAYS, 10000);
    }

    @Test
    public void testPutAsync_withMaxIdle() throws ExecutionException, InterruptedException {
        map.putAsync("key", "value", 10, TimeUnit.DAYS, 5, TimeUnit.DAYS).toCompletableFuture().get();
        assertEntryStore("key", "value", 5, TimeUnit.DAYS, 10000);
    }

    @Test
    public void testPutIfAbsent() {
        map.putIfAbsent("key", "value");
        assertEntryStore("key", "value");
    }

    @Test
    public void testPutIfAbsent_withTtl() {
        map.putIfAbsent("key", "value", 10, TimeUnit.DAYS);
        assertEntryStore("key", "value", 10, TimeUnit.DAYS, 10000);
    }

    @Test
    public void testPutIfAbsent_withMaxIdle() {
        map.putIfAbsent("key", "value", 10, TimeUnit.DAYS, 5, TimeUnit.DAYS);
        assertEntryStore("key", "value", 5, TimeUnit.DAYS, 10000);
    }

    @Test
    public void testPutIfAbsentAsync() throws ExecutionException, InterruptedException {
        assumeTrue(map instanceof MapProxyImpl);

        ((MapProxyImpl<String, String>) map)
                .putIfAbsentAsync("key", "value").toCompletableFuture().get();
        assertEntryStore("key", "value");
    }

    @Test
    public void testPutIfAbsentAsync_withTtl() throws ExecutionException, InterruptedException {
        assumeTrue(map instanceof MapProxyImpl);

        ((MapProxyImpl<String, String>) map)
                .putIfAbsentAsync("key", "value", 10, TimeUnit.DAYS).toCompletableFuture().get();
        assertEntryStore("key", "value", 10, TimeUnit.DAYS, 10000);
    }

    @Test
    public void testPutIfAbsentAsync_withMaxIdle() throws ExecutionException, InterruptedException {
        assumeTrue(map instanceof MapProxyImpl);

        ((MapProxyImpl<String, String>) map)
                .putIfAbsentAsync("key", "value", 10, TimeUnit.DAYS, 5, TimeUnit.DAYS).toCompletableFuture().get();
        assertEntryStore("key", "value", 5, TimeUnit.DAYS, 10000);
    }

    @Test
    public void testRemove() {
        map.put("key", "value");
        map.remove("key");
        assertEntryNotStored("key");
    }

    @Test
    public void testRemoveIfSame() {
        map.put("key", "value");
        map.remove("key", "value");
        assertEntryNotStored("key");
    }

    @Test
    public void testRemoveAll() {
        final int mapSize = 100;
        for (int i = 0; i < mapSize; i++) {
            map.put("k" + i, "v" + i);
        }
        map.removeAll(Predicates.alwaysTrue());
        for (int i = 0; i < mapSize; i++) {
            assertEntryNotStored("k" + i);
        }
    }

    @Test
    public void testRemoveAsync() throws ExecutionException, InterruptedException {
        map.put("key", "value");
        map.removeAsync("key").toCompletableFuture().get();
        assertEntryNotStored("key");
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
        assertEntryStore("key", "replaced");
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
        assertEntryStore("key", "replaced");
    }

    @Test
    public void testSet() {
        map.set("key", "value");
        assertEntryStore("key", "value");
    }

    @Test
    public void testSet_withTtl() {
        map.set("key", "value", 10, TimeUnit.DAYS);
        assertEntryStore("key", "value", 10, TimeUnit.DAYS, 10000);
    }

    @Test
    public void testSet_withMaxIdle() {
        map.set("key", "value", 10, TimeUnit.DAYS, 5, TimeUnit.DAYS);
        assertEntryStore("key", "value", 5, TimeUnit.DAYS, 10000);
    }

    @Test
    public void testSetAsync() throws ExecutionException, InterruptedException {
        map.setAsync("key", "value").toCompletableFuture().get();
        assertEntryStore("key", "value");
    }

    @Test
    public void testSetAsync_withTtl() throws ExecutionException, InterruptedException {
        map.setAsync("key", "value", 10, TimeUnit.DAYS).toCompletableFuture().get();
        assertEntryStore("key", "value", 10, TimeUnit.DAYS, 10000);
    }

    @Test
    public void testSetAsync_withMaxIdle() throws ExecutionException, InterruptedException {
        map.setAsync("key", "value", 10, TimeUnit.DAYS, 5, TimeUnit.DAYS).toCompletableFuture().get();
        assertEntryStore("key", "value", 5, TimeUnit.DAYS, 10000);
    }

    @Test
    public void testSetAll_WithoutMapListener() {
        final int max = 100;
        final Map<String, String> businessObjects = IntStream.range(0, max).boxed()
                .collect(Collectors.toMap(i -> "k" + i, i -> "v" + i));

        map.setAll(businessObjects);

        IntStream.range(0, max).forEach(i -> assertEntryStore("k" + i, "v" + i));
        assertEquals(0, testEntryStore.getLoadCallCount());
    }

    @Test
    public void testSetAll_WithMapListener() {
        final int max = 100;
        final Map<String, String> businessObjects = IntStream.range(0, max).boxed()
                .collect(Collectors.toMap(i -> "k" + i, i -> "v" + i));

        final CountDownLatch latch = new CountDownLatch(max);
        map.addEntryListener((EntryAddedListener) event -> latch.countDown(), true);

        map.setAll(businessObjects);

        IntStream.range(0, max).forEach(i -> assertEntryStore("k" + i, "v" + i));
        assertEquals(0, testEntryStore.getLoadCallCount());
        assertOpenEventually(latch);
    }

    @Test
    public void testSetAllAsync() {
        final int max = 100;
        final Map<String, String> businessObjects = IntStream.range(0, max).boxed()
                .collect(Collectors.toMap(i -> "k" + i, i -> "v" + i));

        final Future<Void> future = map.setAllAsync(businessObjects).toCompletableFuture();
        assertEqualsEventually(future::isDone, true);

        IntStream.range(0, max).forEach(i -> assertEntryStore("k" + i, "v" + i));
        assertEquals(0, testEntryStore.getLoadCallCount());
    }

    @Test
    public void testSetTtl() {
        map.set("key", "value");
        map.setTtl("key", 1, TimeUnit.DAYS);
        assertEntryStore("key", "value", 1, TimeUnit.DAYS, 10000);
    }

    @Test
    public void testTryPut() {
        map.tryPut("key", "value", 10, TimeUnit.SECONDS);
        assertEntryStore("key", "value");
    }

    protected void assertEntryNotStored(String key) {
        testEntryStore.assertRecordNotStored(key);
    }

    protected void assertEntryStore(String key, String value) {
        testEntryStore.assertRecordStored(key, value);
    }

    protected void assertEntryStore(String key, String value, long remainingTtl, TimeUnit timeUnit, long delta) {
        long expectedExpirationTime = System.currentTimeMillis() + timeUnit.toMillis(remainingTtl);
        testEntryStore.assertRecordStored(key, value, expectedExpirationTime, delta);
    }
}
