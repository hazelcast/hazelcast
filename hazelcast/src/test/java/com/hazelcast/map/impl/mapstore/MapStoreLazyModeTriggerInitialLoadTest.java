/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueAllTheTime;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests that the initial load is triggered by the expected methods
 * and not triggered by the methods that should not trigger it.
*/
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("ResultOfMethodCallIgnored")
public class MapStoreLazyModeTriggerInitialLoadTest {

    private static final String MAP_NAME_PREFIX = "mapWithLoader_";
    private static final int MAP_LOADER_SIZE = 20;
    private static final long LOAD_WAIT_TIMEOUT_SECONDS = 2;
    private static final CountingMapLoader mapLoader = new CountingMapLoader(MAP_LOADER_SIZE);

    private static IMap<Integer, Integer> map;
    private static TestHazelcastFactory factory;
    private static HazelcastInstance[] instances;

    @BeforeClass
    public static void setupCluster() {
        mapLoader.reset();
        factory = new TestHazelcastFactory();
        instances = factory.newInstances(getConfig(), 1);
    }

    @AfterClass
    public static void after() {
        factory.shutdownAll();
    }

    @Before
    public void setup() {
        mapLoader.reset();
        map = instances[0].getMap(MAP_NAME_PREFIX + randomString());
    }

    protected static Config getConfig() {
        Config config = smallInstanceConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(InitialLoadMode.LAZY)
                .setImplementation(mapLoader);
        config.getMapConfig(MAP_NAME_PREFIX + "*")
                .setMapStoreConfig(mapStoreConfig);
        return config;
    }

    @Test
    public void initMap() {
        mapLoader.assertNoFullLoadTriggered();
    }

    // === Methods that TRIGGER initial load ===
    @Test
    public void put() {
        map.put(1, 2);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void get() {
        map.get(1);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void size() {
        map.size();
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void delete() {
        map.delete(1);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void putTransient() {
        map.putTransient(1, 2, 10, TimeUnit.HOURS);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void entrySet() {
        map.entrySet();
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void removeAll() {
        map.removeAll(Predicates.alwaysTrue());
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void addIndex() {
        IndexConfig indexConfig = new IndexConfig();
        indexConfig.addAttribute("this");
        map.addIndex(indexConfig);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void getAll() {
        map.getAll(Set.of(1, 2));
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void putIfAbsent() {
        map.putIfAbsent(1, 2);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void remove() {
        map.remove(1);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void removeWithValue() {
        map.remove(1, 2);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void replace() {
        map.replace(1, 2);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void replaceWithOldValue() {
        map.replace(1, 2, 3);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void containsKey() {
        map.containsKey(1);
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void containsValue() {
        map.containsValue(1);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void keySet() {
        map.keySet();
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void values() {
        map.values();
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void loadAllSubset() {
        map.loadAll(Set.of(1), true);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void loadAll() {
        map.loadAll(true);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void flush() {
        map.flush();
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void evict() {
        map.evict(1);
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void lock() {
        map.lock(1);
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void tryLock() throws InterruptedException {
        map.tryLock(1, 1, TimeUnit.SECONDS);
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void submitToKey() {
        map.submitToKey(1, key -> "value" + key).toCompletableFuture().join();
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test

    public void isEmpty() {
        map.isEmpty();
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void set() {
        map.set(1, 2);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void setTtl() {
        map.setTtl(1, 1, TimeUnit.HOURS);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void putAll() {
        map.putAll(Map.of(1, 2, 3, 4));
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void computeIfAbsent() {
        map.computeIfAbsent(1, k -> k + 1);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void computeIfPresent() {
        map.computeIfPresent(1, (k, v) -> 2);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void compute() {
        map.compute(1, (k, v) -> 2);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void merge() {
        map.merge(1, 2, Integer::sum);
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void executeOnKey() {
        map.executeOnKey(1, entry -> null);
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void submitToKeyEntryProcessor() {
        map.submitToKey(1, entry -> null)
            .toCompletableFuture()
            .join();

        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void keySetPredicate() {
        map.keySet(Predicates.alwaysTrue());
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void valuesPredicate() {
        map.values(Predicates.alwaysTrue());
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void entrySetPredicate() {
        map.entrySet(Predicates.alwaysTrue());
        mapLoader.assertFullLoadTriggeredOnce();
    }

    @Test
    public void getAsync() {
        map.getAsync(1).toCompletableFuture().join();
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void putAsync() {
        map.putAsync(1, 2).toCompletableFuture().join();
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void removeAsync() {
        map.removeAsync(1).toCompletableFuture().join();
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void forEach() {
        map.forEach((entry) -> {});
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void localKeySet() {
        map.localKeySet();
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void getEntryView() {
        map.getEntryView(1);
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void setAsync() {
        map.setAsync(1, 2).toCompletableFuture().join();
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void putAllAsync() {
        map.putAllAsync(Map.of(1, 2)).toCompletableFuture().join();
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void tryPut() {
        map.tryPut(1, 1, 5, TimeUnit.SECONDS);
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void tryRemove() {
        map.tryRemove(1, 5, TimeUnit.SECONDS);
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void setAllAsync() {
        map.setAllAsync(Map.of(1, 2)).toCompletableFuture().join();
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void deleteAsync() {
        map.deleteAsync(1).toCompletableFuture().join();
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void iterator() {
        map.iterator().hasNext();
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void localValues() {
        map.localValues();
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void project() {
        map.project(v -> v);
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void aggregate() {
        map.aggregate(Aggregators.count(), Predicates.alwaysTrue());
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void executeOnEntries() {
        map.executeOnEntries(entry -> null);
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void executeOnKeys() {
        map.executeOnKeys(Set.of(1), entry -> null);
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void submitToKeys() {
        map.submitToKeys(Set.of(1), entry -> null).toCompletableFuture().join();
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void replaceAll() {
        map.replaceAll((k, v) -> v);
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    @Test
    public void setAll() {
        map.setAll(Map.of(1, 2));
        assertTrueEventually(mapLoader::assertFullLoadTriggeredOnce);
    }

    // === Methods that DOES NOT TRIGGER initial load ===
    @Test
    public void clear() {
        map.clear();
        assertTrueAllTheTime(mapLoader::assertNoFullLoadTriggered, LOAD_WAIT_TIMEOUT_SECONDS);
    }

    @Test
    public void evictAll() {
        map.evictAll();
        assertTrueAllTheTime(mapLoader::assertNoFullLoadTriggered, LOAD_WAIT_TIMEOUT_SECONDS);
    }

    @Test
    public void destroy() {
        map.destroy();
        assertTrueAllTheTime(mapLoader::assertNoFullLoadTriggered, LOAD_WAIT_TIMEOUT_SECONDS);
    }

    @Test
    public void addEntryListener() {
        map.addEntryListener(mock(EntryListener.class), true);
        assertTrueAllTheTime(mapLoader::assertNoFullLoadTriggered, LOAD_WAIT_TIMEOUT_SECONDS);
    }

    @Test
    public void addInterceptor() {
        map.addInterceptor(mock(MapInterceptor.class));
        assertTrueAllTheTime(mapLoader::assertNoFullLoadTriggered, LOAD_WAIT_TIMEOUT_SECONDS);
    }

    @Test
    public void addLocalEntryListener() {
        map.addLocalEntryListener(mock(EntryListener.class));
        assertTrueAllTheTime(mapLoader::assertNoFullLoadTriggered, LOAD_WAIT_TIMEOUT_SECONDS);
    }

    @Test
    public void getLocalMapStats() {
        map.getLocalMapStats();
        assertTrueAllTheTime(mapLoader::assertNoFullLoadTriggered, LOAD_WAIT_TIMEOUT_SECONDS);
    }

    @Test
    public void getQueryCache() {
        map.getQueryCache("cache");
        assertTrueAllTheTime(mapLoader::assertNoFullLoadTriggered, LOAD_WAIT_TIMEOUT_SECONDS);
    }

    @Test
    public void removeInterceptor() {
        map.removeInterceptor("dummy");
        assertTrueAllTheTime(mapLoader::assertNoFullLoadTriggered, LOAD_WAIT_TIMEOUT_SECONDS);
    }

    @Test
    public void removeEntryListener() {
        map.removeEntryListener(UUID.randomUUID());
        assertTrueAllTheTime(mapLoader::assertNoFullLoadTriggered, LOAD_WAIT_TIMEOUT_SECONDS);
    }

    @Test
    public void removePartitionLostListener() {
        map.removePartitionLostListener(UUID.randomUUID());
        assertTrueAllTheTime(mapLoader::assertNoFullLoadTriggered, LOAD_WAIT_TIMEOUT_SECONDS);
    }

    @Test
    public void forceUnlock() {
        map.forceUnlock(1);
        assertTrueAllTheTime(mapLoader::assertNoFullLoadTriggered, LOAD_WAIT_TIMEOUT_SECONDS);
    }

    @Test
    public void addPartitionLostListener() {
        map.addPartitionLostListener(event -> {});
        assertTrueAllTheTime(mapLoader::assertNoFullLoadTriggered, LOAD_WAIT_TIMEOUT_SECONDS);
    }

    @Test
    public void isLocked() {
        map.isLocked(1);
        assertTrueAllTheTime(mapLoader::assertNoFullLoadTriggered, LOAD_WAIT_TIMEOUT_SECONDS);
    }

    @Test
    public void allIMapMethodsAreCovered() {
        var iMapMethods = Arrays.stream(IMap.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(Method::getName)
            .filter(name -> !Objects.equals("unlock", name))
            .collect(Collectors.toSet());

        var testMethods = Arrays.stream(MapStoreLazyModeTriggerInitialLoadTest.class.getDeclaredMethods())
            .filter(method -> method.isAnnotationPresent(Test.class))
            .map(Method::getName)
            .collect(Collectors.toSet());

        assertThat(testMethods).containsAll(iMapMethods);
    }
}
