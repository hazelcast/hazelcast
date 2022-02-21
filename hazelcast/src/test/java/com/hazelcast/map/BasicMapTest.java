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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ConfigureParallelRunnerWith;
import com.hazelcast.test.annotation.HeavilyMultiThreadedTestLimiter;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import testsubjects.StaticSerializableBiConsumer;
import testsubjects.StaticSerializableBiFunction;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@ConfigureParallelRunnerWith(HeavilyMultiThreadedTestLimiter.class)
public class BasicMapTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public boolean statisticsEnabled;
    @Parameterized.Parameter(1)
    public boolean perEntryStatsEnabled;

    @Parameterized.Parameters(name = "statisticsEnabled:{0}, perEntryStatsEnabled:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true, true},
                {false, true},
                {true, false},
                {false, false},
        });
    }

    /**
     * This rule is here artificially just to test that ChangeLoggingRule is working (meaning not broken).
     */
    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2.xml");

    static final int INSTANCE_COUNT = 3;
    static final Random RANDOM = new Random();

    HazelcastInstance[] instances;

    @Before
    public void init() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(INSTANCE_COUNT);
        Config config = getConfig();
        instances = factory.newInstances(config);
    }

    protected Config getConfig() {
        Config cfg = smallInstanceConfig();
        cfg.getMapConfig("default")
                .setStatisticsEnabled(statisticsEnabled)
                .setPerEntryStatsEnabled(perEntryStatsEnabled);

        MapConfig mapConfig = new MapConfig("mapWithTTL*");
        mapConfig.setTimeToLiveSeconds(1);
        mapConfig.setStatisticsEnabled(statisticsEnabled);
        mapConfig.setPerEntryStatsEnabled(perEntryStatsEnabled);

        cfg.getMapConfig("testEntryView")
        .setStatisticsEnabled(statisticsEnabled)
        .setPerEntryStatsEnabled(perEntryStatsEnabled);

        cfg.addMapConfig(mapConfig);
        return cfg;
    }

    HazelcastInstance getInstance() {
        return instances[RANDOM.nextInt(INSTANCE_COUNT)];
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void testBoxedPrimitives() {
        IMap<String, Object> map = getInstance().getMap("testPrimitives");

        assertPutGet(map, Boolean.TRUE);
        assertPutGet(map, Boolean.FALSE);

        assertPutGet(map, new Integer(10));

        assertPutGet(map, new Short((short) 10));

        assertPutGet(map, new Byte((byte) 10));

        assertPutGet(map, new Long(10));

        assertPutGet(map, new Float(10));

        assertPutGet(map, new Double(10));

        assertPutGet(map, new Character('x'));
    }

    public void assertPutGet(Map<String, Object> map, Object value) {
        String key = UUID.randomUUID().toString();
        map.put(key, value);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testArrays() {
        IMap<String, Object> map = getInstance().getMap("testArrays");

        boolean[] booleanArray = {true, false};
        map.put("boolean", booleanArray);
        assertTrue(Arrays.equals(booleanArray, (boolean[]) map.get("boolean")));

        int[] intArray = {1, 2};
        map.put("int", intArray);
        assertArrayEquals(intArray, (int[]) map.get("int"));

        short[] shortArray = {(short) 1, (short) 2};
        map.put("short", shortArray);
        assertArrayEquals(shortArray, (short[]) map.get("short"));

        short[] byteArray = {(byte) 1, (byte) 2};
        map.put("byte", byteArray);
        assertArrayEquals(byteArray, (short[]) map.get("byte"));

        long[] longArray = {1L, 2L};
        map.put("long", longArray);
        assertArrayEquals(longArray, (long[]) map.get("long"));

        float[] floatArray = {(float) 1, (float) 2};
        map.put("float", floatArray);
        assertTrue(Arrays.equals(floatArray, (float[]) map.get("float")));

        double[] doubleArray = {(double) 1, (double) 2};
        map.put("double", doubleArray);
        assertTrue(Arrays.equals(doubleArray, (double[]) map.get("double")));

        char[] charArray = {'1', '2'};
        map.put("char", charArray);
        assertArrayEquals(charArray, (char[]) map.get("char"));

        Object[] objectArray = {"foo", null, Integer.decode("3")};
        map.put("object", objectArray);
        assertArrayEquals(objectArray, (Object[]) map.get("object"));
    }

    @Test
    public void testMapPutAndGet() {
        IMap<String, String> map = getInstance().getMap("testMapPutAndGet");
        String value = map.put("Hello", "World");
        assertEquals("World", map.get("Hello"));
        assertEquals(1, map.size());
        assertNull(value);
        value = map.put("Hello", "World");
        assertEquals("World", map.get("Hello"));
        assertEquals(1, map.size());
        assertEquals("World", value);
        value = map.put("Hello", "New World");
        assertEquals("World", value);
        assertEquals("New World", map.get("Hello"));
    }

    @Test
    public void testMapPutIfAbsent() {
        IMap<String, String> map = getInstance().getMap("testMapPutIfAbsent");
        assertEquals(map.putIfAbsent("key1", "value1"), null);
        assertEquals(map.putIfAbsent("key2", "value2"), null);
        assertEquals(map.putIfAbsent("key1", "valueX"), "value1");
        assertEquals(map.get("key1"), "value1");
        assertEquals(map.size(), 2);
    }

    @Test
    public void testMapGetNullIsNotAllowed() {
        IMap<String, String> map = getInstance().getMap("testMapGetNullIsNotAllowed");
        try {
            map.get(null);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof NullPointerException);
        }
    }

    @Test
    public void testMapExpireAndListener() {
        IMap<String, String> map = getInstance().getMap("testMapExpireAndListener");

        final String value1 = "/home/data/file1.dat";
        final String value2 = "/home/data/file2.dat";

        final AtomicReference<String> oldValue1 = new AtomicReference<String>();
        final AtomicReference<String> oldValue2 = new AtomicReference<String>();
        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);

        map.addEntryListener((EntryExpiredListener<String, String>) event -> {
            if (value1.equals(event.getOldValue())) {
                oldValue1.set(event.getOldValue());
                latch1.countDown();
            } else if (value2.equals(event.getOldValue())) {
                oldValue2.set(event.getOldValue());
                latch2.countDown();
            }
        }, true);

        map.put("key", value1, 1, SECONDS);
        assertOpenEventually(latch1);

        map.put("key", value2, 1, SECONDS);
        assertOpenEventually(latch2);

        assertEquals(value1, oldValue1.get());
        assertEquals(value2, oldValue2.get());
    }

    @Test
    public void testMapEntryListener() {
        IMap<String, String> map = getInstance().getMap("testMapEntryListener");

        final CountDownLatch latchAdded = new CountDownLatch(1);
        final CountDownLatch latchRemoved = new CountDownLatch(1);
        final CountDownLatch latchUpdated = new CountDownLatch(1);
        final CountDownLatch latchCleared = new CountDownLatch(1);
        final CountDownLatch latchEvicted = new CountDownLatch(1);

        map.addEntryListener(new EntryListener<String, String>() {
            @Override
            public void entryExpired(EntryEvent<String, String> event) {

            }

            @Override
            public void entryAdded(EntryEvent event) {
                latchAdded.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
                assertEquals("hello", event.getKey());
                assertEquals("new world", event.getOldValue());
                latchRemoved.countDown();
            }

            @Override
            public void entryUpdated(EntryEvent event) {
                assertEquals("world", event.getOldValue());
                assertEquals("new world", event.getValue());
                assertEquals("hello", event.getKey());
                latchUpdated.countDown();
            }

            @Override
            public void entryEvicted(EntryEvent event) {
                entryRemoved(event);
            }

            @Override
            public void mapEvicted(MapEvent event) {
                latchEvicted.countDown();
            }

            @Override
            public void mapCleared(MapEvent event) {
                latchCleared.countDown();
            }
        }, true);

        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        map.put("hi", "new world");
        map.evictAll();
        map.put("hello", "world");
        map.clear();
        try {
            assertTrue(latchAdded.await(5, SECONDS));
            assertTrue(latchUpdated.await(5, SECONDS));
            assertTrue(latchRemoved.await(5, SECONDS));
            assertTrue(latchEvicted.await(5, SECONDS));
            assertTrue(latchCleared.await(5, SECONDS));
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertFalse(e.getMessage(), true);
        }
    }

    /**
     * Test for issue #181
     */
    @Test
    public void testMapKeyListenerWithRemoveAndUnlock() throws Exception {
        int count = 20;
        IMap<String, String> map = getInstance().getMap("testMapKeyListenerWithRemoveAndUnlock");

        final String key = "key";
        final CountDownLatch latch = new CountDownLatch(count * 2);
        map.addEntryListener(new EntryAdapter<String, String>() {
            @Override
            public void entryAdded(EntryEvent<String, String> e) {
                testEvent(e);
            }

            @Override
            public void entryRemoved(EntryEvent<String, String> e) {
                testEvent(e);
            }

            private void testEvent(EntryEvent<String, String> e) {
                if (key.equals(e.getKey())) {
                    latch.countDown();
                } else {
                    fail("Invalid event: " + e);
                }
            }
        }, key, true);

        for (int i = 0; i < count; i++) {
            map.lock(key);
            map.put(key, "value");
            map.remove(key);
            map.unlock(key);
        }
        assertTrue("Listener events are missing! Remaining: " + latch.getCount(), latch.await(5, SECONDS));
    }

    @Test
    public void testMapRemove() {
        IMap<String, String> map = getInstance().getMap("testMapRemove");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        assertEquals(map.remove("key1"), "value1");
        assertEquals(map.size(), 2);
        assertEquals(map.remove("key1"), null);
        assertEquals(map.size(), 2);
        assertEquals(map.remove("key3"), "value3");
        assertEquals(map.size(), 1);
    }

    @Test
    public void testMapDelete() {
        IMap<String, String> map = getInstance().getMap("testMapRemove");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.delete("key1");
        assertEquals(map.size(), 2);
        map.delete("key1");
        assertEquals(map.size(), 2);
        map.delete("key3");
        assertEquals(map.size(), 1);
    }

    @Test
    public void testMapClear_nonEmptyMap() {
        IMap<String, String> map = getInstance().getMap("testMapClear");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.clear();
        assertEquals(map.size(), 0);
        assertEquals(map.get("key1"), null);
        assertEquals(map.get("key2"), null);
        assertEquals(map.get("key3"), null);
    }

    @Test
    public void testMapClear_emptyMap() {
        String mapName = "testMapClear_emptyMap";
        HazelcastInstance hz = getInstance();
        IMap<String, String> map = hz.getMap(mapName);
        map.clear();
        assertEquals(map.size(), 0);

        // TODO: This test is going to be enabled as soon as the size has been fixed (since it also triggers unwanted RecordStore
        // creation). We need to make sure there are no unwanted RecordStores (consumes memory) being created because of the
        // clear. So we are going to check one of the partitions if it has a RecordStore and then we can safely assume that the
        // rest of the partitions have no record store either.
        // MapService mapService  = getNode(hz).nodeEngine.getService(MapService.SERVICE_NAME);
        // RecordStore recordStore = mapService.getPartitionContainer(1).getExistingRecordStore(mapName);
        // assertNull(recordStore);
    }

    @Test
    public void testMapEvict() {
        IMap<String, String> map = getInstance().getMap("testMapEvict");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        assertEquals(map.remove("key1"), "value1");
        assertEquals(map.size(), 2);
        assertEquals(map.remove("key1"), null);
        assertEquals(map.size(), 2);
        assertEquals(map.remove("key3"), "value3");
        assertEquals(map.size(), 1);
    }

    @Test
    public void testMapEvictAll() {
        IMap<String, String> map = getInstance().getMap("testMapEvict");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");

        map.evictAll();

        assertEquals(0, map.size());
    }

    @Test
    public void testMapTryRemove() throws Exception {
        final IMap<Object, Object> map = getInstance().getMap("testMapTryRemove");
        map.put("key1", "value1");
        map.lock("key1");

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        final CountDownLatch latch3 = new CountDownLatch(1);
        final AtomicBoolean firstBool = new AtomicBoolean();
        final AtomicBoolean secondBool = new AtomicBoolean();
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    firstBool.set(map.tryRemove("key1", 1, SECONDS));
                    latch2.countDown();
                    latch1.await();
                    secondBool.set(map.tryRemove("key1", 1, SECONDS));
                    latch3.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }
            }
        });

        thread.start();
        latch2.await();
        map.unlock("key1");
        latch1.countDown();
        latch3.await();
        assertFalse(firstBool.get());
        assertTrue(secondBool.get());
        thread.join();
    }

    @Test
    public void testMapRemoveIfSame() {
        IMap<String, String> map = getInstance().getMap("testMapRemoveIfSame");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        assertFalse(map.remove("key1", "nan"));
        assertEquals(map.size(), 3);
        assertTrue(map.remove("key1", "value1"));
        assertEquals(map.size(), 2);
        assertTrue(map.remove("key2", "value2"));
        assertTrue(map.remove("key3", "value3"));
        assertEquals(map.size(), 0);
    }

    @Test
    public void testMapSet() {
        IMap<String, String> map = getInstance().getMap("testMapSet");
        map.put("key1", "value1");
        assertEquals(map.get("key1"), "value1");
        assertEquals(map.size(), 1);
        map.set("key1", "valueX", 0, TimeUnit.MILLISECONDS);
        assertEquals(map.size(), 1);
        assertEquals(map.get("key1"), "valueX");
        map.set("key2", "value2", 0, TimeUnit.MILLISECONDS);
        assertEquals(map.size(), 2);
        assertEquals(map.get("key1"), "valueX");
        assertEquals(map.get("key2"), "value2");
    }

    @Test
    public void testMapContainsKey() {
        IMap<String, String> map = getInstance().getMap("testMapContainsKey");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        assertEquals(map.containsKey("key1"), true);
        assertEquals(map.containsKey("key5"), false);
        map.remove("key1");
        assertEquals(map.containsKey("key1"), false);
        assertEquals(map.containsKey("key2"), true);
        assertEquals(map.containsKey("key5"), false);
    }

    @Test
    public void testMapContainsValue() {
        IMap<Integer, Integer> map = getInstance().getMap("testMapContainsValue");
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        assertTrue(map.containsValue(1));
        assertFalse(map.containsValue(5));
        map.remove(1);
        assertFalse(map.containsValue(1));
        assertTrue(map.containsValue(2));
        assertFalse(map.containsValue(5));
    }

    @Test
    public void testMapIsEmpty() {
        IMap<String, String> map = getInstance().getMap("testMapIsEmpty");
        assertTrue(map.isEmpty());
        map.put("key1", "value1");
        assertFalse(map.isEmpty());
        map.remove("key1");
        assertTrue(map.isEmpty());
    }

    @Test
    public void testMapSize() {
        IMap<Integer, Integer> map = getInstance().getMap("testMapSize");
        assertEquals(map.size(), 0);
        map.put(1, 1);
        assertEquals(map.size(), 1);
        map.put(2, 2);
        map.put(3, 3);
        assertEquals(map.size(), 3);
    }

    @Test
    public void testMapReplace() {
        IMap<Integer, Integer> map = getInstance().getMap("testMapReplace");
        map.put(1, 1);
        assertNull(map.replace(2, 1));
        assertNull(map.get(2));
        map.put(2, 2);
        assertEquals(2, map.replace(2, 3).intValue());
        assertEquals(3, map.get(2).intValue());
    }

    @Test
    public void testMapReplaceIfSame() {
        IMap<Integer, Integer> map = getInstance().getMap("testMapReplaceIfSame");
        map.put(1, 1);
        assertFalse(map.replace(1, 2, 3));
        assertTrue(map.replace(1, 1, 2));
        assertEquals(map.get(1).intValue(), 2);
        map.put(2, 2);
        assertTrue(map.replace(2, 2, 3));
        assertEquals(map.get(2).intValue(), 3);
        assertTrue(map.replace(2, 3, 4));
        assertEquals(map.get(2).intValue(), 4);
    }

    @Test
    public void testMapClonedCollectionsImmutable() {
        testMapClonedCollectionsImmutable(getInstance(), true);
    }

    /**
     * Tests the cloned collections returned by IMap's keySet(), localKeySet(),
     * values(), entrySet() are immutable. To avoid code duplication the static
     * method is called from client's test.
     *
     * @param instance the HZ instance
     */
    public static void testMapClonedCollectionsImmutable(HazelcastInstance instance, boolean onMember) {
        IMap<Integer, Integer> map = instance.getMap("testMapClonedCollectionsImmutable");

        // test empty map
        checkMapClonedCollectionsImmutable(map, onMember);

        // put some values
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        checkMapClonedCollectionsImmutable(map, onMember);
    }

    private static <K, V> void checkMapClonedCollectionsImmutable(IMap<K, V> map, boolean onMember) {
        PagingPredicate<K, V> pagingPredicate = Predicates.pagingPredicate(5);

        checkCollectionImmutable(map.entrySet());
        checkCollectionImmutable(map.entrySet(e -> true));
        checkCollectionImmutable(map.entrySet(e -> false));
        checkCollectionImmutable(map.entrySet(pagingPredicate));
        if (onMember) {
            checkCollectionImmutable(map.localKeySet());
            checkCollectionImmutable(map.localKeySet(k -> true));
            checkCollectionImmutable(map.localKeySet(k -> false));
            checkCollectionImmutable(map.localKeySet(pagingPredicate));
        }
        checkCollectionImmutable(map.keySet());
        checkCollectionImmutable(map.keySet(k -> true));
        checkCollectionImmutable(map.keySet(k -> false));
        checkCollectionImmutable(map.keySet(pagingPredicate));
        checkCollectionImmutable(map.values());
        checkCollectionImmutable(map.values(v -> true));
        checkCollectionImmutable(map.values(v -> false));
        checkCollectionImmutable(map.values(pagingPredicate));
        checkMapImmutable(map.getAll(map.keySet()));
        checkMapImmutable(map.getAll(Collections.emptySet()));
    }

    private static <K, V> void checkMapImmutable(Map<K, V> map) {
        K key = map.isEmpty() ? null : map.keySet().iterator().next();
        V value = map.isEmpty() ? null : map.values().iterator().next();
        assertThrows(UnsupportedOperationException.class, () -> map.put(key, value));
        assertThrows(UnsupportedOperationException.class, () -> map.putIfAbsent(key, value));
        assertThrows(UnsupportedOperationException.class, () -> map.computeIfAbsent(key, k -> value));
        assertThrows(UnsupportedOperationException.class, () -> map.computeIfPresent(key, (k, v) -> value));
        assertThrows(UnsupportedOperationException.class, () -> map.putAll(map));
        assertThrows(UnsupportedOperationException.class, () -> map.merge(key, value, (v1, v2) -> value));
        assertThrows(UnsupportedOperationException.class, () -> map.remove(key));
        assertThrows(UnsupportedOperationException.class, () -> map.remove(key, value));
        assertThrows(UnsupportedOperationException.class, () -> map.clear());
        assertThrows(UnsupportedOperationException.class, () -> map.replace(key, value));
        assertThrows(UnsupportedOperationException.class, () -> map.replace(key, value, value));
        assertThrows(UnsupportedOperationException.class, () -> map.replaceAll((k, v) -> value));
        assertThrows(UnsupportedOperationException.class, () -> map.compute(key, (k, v) -> v));
        assertThrows(UnsupportedOperationException.class, () -> map.computeIfAbsent(key, k -> value));
        assertThrows(UnsupportedOperationException.class, () -> map.computeIfPresent(key, (k, v) -> v));
        checkCollectionImmutable(map.entrySet());
        checkCollectionImmutable(map.keySet());
        checkCollectionImmutable(map.values());
    }

    private static <T> void checkCollectionImmutable(Collection<T> c) {
        assertThrows(UnsupportedOperationException.class, () -> c.remove(null));
        assertThrows(UnsupportedOperationException.class, () -> c.removeIf(e -> true));
        assertThrows(UnsupportedOperationException.class, () -> c.remove(c.isEmpty() ? null : c.iterator().next()));
        assertThrows(UnsupportedOperationException.class, () -> c.removeAll(c));
        assertThrows(UnsupportedOperationException.class, () -> c.add(c.isEmpty() ? null : c.iterator().next()));
        assertThrows(UnsupportedOperationException.class, () -> c.addAll(c));
        assertThrows(UnsupportedOperationException.class, () -> c.retainAll(Collections.emptyList()));
        assertThrows(UnsupportedOperationException.class, () -> c.clear());

        if (!c.isEmpty()) {
            Iterator<T> iterator = c.iterator();
            iterator.next();
            assertThrows(UnsupportedOperationException.class, () -> iterator.remove());
        }
    }

    @Test
    public void testMapTryLock() throws Exception {
        final IMap<Object, Object> map = getInstance().getMap("testMapTryLock");
        final String key = "key";
        map.lock(key);

        final CountDownLatch latch = new CountDownLatch(1);
        Future<Object> f = spawn(new Callable<Object>() {
            public Object call() throws Exception {
                assertFalse("Should NOT be able to acquire lock!", map.tryLock(key));
                latch.countDown();

                assertTrue("Should be able to acquire lock!", map.tryLock(key, 60, SECONDS));
                return null;
            }
        });

        assertOpenEventually(latch);
        map.unlock(key);
        f.get();
    }

    @Test
    public void testMapPut_whenKeyLocked() throws Exception {
        final IMap<Object, Object> map = getInstance().getMap("testMapPut_whenKeyLocked");
        final String key = "key";
        final String invalidValue = "valuex";
        final String value = "value";
        map.lock(key);

        Future f1 = spawn(new Runnable() {
            @Override
            public void run() {
                assertFalse(map.tryPut(key, invalidValue, 1, SECONDS));
            }
        });

        Future f2 = spawn(new Runnable() {
            @Override
            public void run() {
                map.put(key, value);
            }
        });

        f1.get();
        try {
            f2.get(1, SECONDS);
            fail("Should not be able to put entry when key is locked!");
        } catch (TimeoutException ignored) {
        }
        map.unlock(key);

        f2.get();
        assertEquals(value, map.get(key));
    }

    @Test
    public void testMapIsLocked() throws Exception {
        final IMap<Object, Object> map = getInstance().getMap("testMapIsLocked");
        map.lock("key1");
        assertTrue(map.isLocked("key1"));
        assertFalse(map.isLocked("key2"));

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean b1 = new AtomicBoolean();
        final AtomicBoolean b2 = new AtomicBoolean();
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    b1.set(map.isLocked("key1"));
                    b2.set(map.isLocked("key2"));
                    latch.countDown();
                } catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        });
        thread.start();
        latch.await();
        assertTrue(b1.get());
        assertFalse(b2.get());
        thread.join();
    }

    @Test
    @SuppressWarnings("OverwrittenKey")
    public void testEntryView() {
        assumeThat(perEntryStatsEnabled, is(true));

        HazelcastInstance instance = getInstance();

        IMap<Integer, Integer> map = instance.getMap("testEntryView");
        long time1 = Clock.currentTimeMillis();
        sleepSeconds(1);

        map.put(1, 1);
        map.put(1, 1);
        map.get(1);
        map.put(2, 2);
        map.put(2, 2);
        map.get(2);
        map.put(3, 3);
        map.put(3, 3);

        long time2 = Clock.currentTimeMillis();
        sleepSeconds(1);
        map.get(3);
        map.get(3);
        sleepSeconds(1);

        long time3 = Clock.currentTimeMillis();
        sleepSeconds(1);
        map.get(2);
        map.put(2, 22);

        EntryView<Integer, Integer> entryView1 = map.getEntryView(1);
        EntryView<Integer, Integer> entryView2 = map.getEntryView(2);
        EntryView<Integer, Integer> entryView3 = map.getEntryView(3);

        assertEqualsStringFormat("Expected entryView1.getKey() to be %d, but was %d", 1, entryView1.getKey());
        assertEqualsStringFormat("Expected entryView2.getKey() to be %d, but was %d", 2, entryView2.getKey());
        assertEqualsStringFormat("Expected entryView3.getKey() to be %d, but was %d", 3, entryView3.getKey());

        assertEqualsStringFormat("Expected entryView1.getValue() to be %d, but was %d", 1, entryView1.getValue());
        assertEqualsStringFormat("Expected entryView2.getValue() to be %d, but was %d", 22, entryView2.getValue());
        assertEqualsStringFormat("Expected entryView3.getValue() to be %d, but was %d", 3, entryView3.getValue());

        assertEqualsStringFormat("Expected entryView1.getHits() to be %d, but were %d", 2L, entryView1.getHits());
        assertEqualsStringFormat("Expected entryView2.getHits() to be %d, but were %d", 4L, entryView2.getHits());
        assertEqualsStringFormat("Expected entryView3.getHits() to be %d, but were %d", 3L, entryView3.getHits());

        assertEqualsStringFormat("Expected entryView1.getVersion() to be %d, but was %d", 1L, entryView1.getVersion());
        assertEqualsStringFormat("Expected entryView2.getVersion() to be %d, but was %d", 2L, entryView2.getVersion());
        assertEqualsStringFormat("Expected entryView3.getVersion() to be %d, but was %d", 1L, entryView3.getVersion());

        assertBetween("entryView1.getCreationTime()", entryView1.getCreationTime(), time1, time2);
        assertBetween("entryView2.getCreationTime()", entryView2.getCreationTime(), time1, time2);
        assertBetween("entryView3.getCreationTime()", entryView3.getCreationTime(), time1, time2);

        assertBetween("entryView1.getLastAccessTime()", entryView1.getLastAccessTime(), time1, time2);
        assertGreaterOrEquals("entryView2.getLastAccessTime()",
                MILLISECONDS.toSeconds(entryView2.getLastAccessTime()), MILLISECONDS.toSeconds(time3));
        assertBetween("entryView3.getLastAccessTime()", MILLISECONDS.toSeconds(entryView3.getLastAccessTime()),
                MILLISECONDS.toSeconds(time2), MILLISECONDS.toSeconds(time3));

        assertBetween("entryView1.getLastUpdateTime()", MILLISECONDS.toSeconds(entryView1.getLastUpdateTime()),
                MILLISECONDS.toSeconds(time1), MILLISECONDS.toSeconds(time2));
        assertGreaterOrEquals("entryView2.getLastUpdateTime()", MILLISECONDS.toSeconds(entryView2.getLastUpdateTime()),
                MILLISECONDS.toSeconds(time3));
        assertBetween("entryView3.getLastUpdateTime()", MILLISECONDS.toSeconds(entryView3.getLastUpdateTime()),
                MILLISECONDS.toSeconds(time1), MILLISECONDS.toSeconds(time2));
    }

    @Test
    public void testTryPut_whenKeyNotLocked() {
        IMap<Object, Object> map = getInstance().getMap(randomMapName());
        String key = "key";
        String value = "value";

        assertTrue(map.tryPut(key, value, 1, SECONDS));
        assertEquals(value, map.get(key));
    }

    @Test
    public void testTryPut_fails_whenKeyLocked() throws Exception {
        final IMap<Object, Object> map = getInstance().getMap(randomMapName());
        final String key = "key";
        final String value = "value";

        // lock the key
        spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                map.lock(key);
                return null;
            }
        }).get(30, SECONDS);

        assertFalse(map.tryPut(key, value, 100, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testTryPut_whenKeyLocked_thenUnlocked() throws Exception {
        final IMap<Object, Object> map = getInstance().getMap(randomMapName());
        final String key = "key";
        final String value = "value";

        map.lock(key);

        final CountDownLatch tryPutFailureLatch = new CountDownLatch(1);

        Future<Object> future = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    assertFalse("tryPut() on a locked key should fail!",
                            map.tryPut(key, value, 100, TimeUnit.MILLISECONDS));
                } finally {
                    tryPutFailureLatch.countDown();
                }

                assertTrue("tryPut() should have been succeeded, key is already unlocked!",
                        map.tryPut(key, value, 30, SECONDS));
                return null;
            }
        });

        tryPutFailureLatch.await(30, SECONDS);
        map.unlock(key);

        future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testGetPutRemoveAsync() {
        IMap<Integer, Object> map = getInstance().getMap("testGetPutRemoveAsync");
        try {
            assertNull(map.putAsync(1, 1).toCompletableFuture().get());
            assertEquals(1, map.putAsync(1, 2).toCompletableFuture().get());
            assertEquals(2, map.getAsync(1).toCompletableFuture().get());
            assertEquals(2, map.removeAsync(1).toCompletableFuture().get());
            assertEquals(0, map.size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPutIfAbsentAsync() {
        MapProxyImpl<Object, Object> map = (MapProxyImpl<Object, Object>) getInstance().getMap("testPutIfAbsentAsync");
        try {
            assertNull(map.putIfAbsentAsync(1, 1).toCompletableFuture().get());
            assertEquals(1, map.putIfAbsentAsync(1, 2).toCompletableFuture().get());
            assertEquals(1, map.putIfAbsentAsync(1, 3).toCompletableFuture().get());
            assertEquals(1, map.size());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAsyncMethodChaining() {
        IMap<Integer, Integer> map = getInstance().getMap("testGetPutRemoveAsync");
        CompletionStage<Integer> setThenGet = map.setAsync(1, 1)
                .thenCompose(v -> map.getAsync(1));
        assertEquals(1L, (long) setThenGet.toCompletableFuture().join());
    }

    @Test
    public void testPutAllEmpty() {
        IMap<Integer, Integer> map = getInstance().getMap("testPutAllEmpty");
        map.putAll(emptyMap());
    }

    @Test
    public void testPutAllAsyncEmpty() {
        IMap<Integer, Integer> map = getInstance().getMap("testPutAllEmpty");
        map.putAllAsync(emptyMap());
    }

    @Test
    public void testGetAllPutAll() {
        warmUpPartitions(instances);
        IMap<Integer, Integer> map = getInstance().getMap("testGetAllPutAll");
        Set<Integer> set = new HashSet<Integer>();
        set.add(1);
        set.add(3);
        map.getAll(set);
        assertTrue(map.isEmpty());

        int size = 100;
        Map<Integer, Integer> mm = new HashMap<Integer, Integer>();
        for (int i = 0; i < size; i++) {
            mm.put(i, i);
        }

        map.putAll(mm);
        assertEquals(size, map.size());
        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i).intValue(), i);
        }

        size = 10000;
        for (int i = 0; i < size; i++) {
            mm.put(i, i);
        }
        map.putAll(mm);
        assertEquals(size, map.size());
        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i).intValue(), i);
        }

        set = new HashSet<Integer>();
        set.add(1);
        set.add(3);
        Map m2 = map.getAll(set);
        assertEquals(m2.size(), 2);
        assertEquals(m2.get(1), 1);
        assertEquals(m2.get(3), 3);
    }

    @Test
    public void testPutAllBackup() {
        int size = 100;

        HazelcastInstance instance1 = instances[0];
        HazelcastInstance instance2 = instances[1];

        IMap<Integer, Integer> map1 = instance1.getMap("testPutAllBackup");
        IMap<Integer, Integer> map2 = instance2.getMap("testPutAllBackup");
        warmUpPartitions(instances);

        Map<Integer, Integer> mm = new HashMap<Integer, Integer>();
        for (int i = 0; i < size; i++) {
            mm.put(i, i);
        }

        map2.putAll(mm);
        assertEquals(size, map2.size());
        for (int i = 0; i < size; i++) {
            assertEquals(i, map2.get(i).intValue());
        }

        instance2.shutdown();
        assertEquals(size, map1.size());
        for (int i = 0; i < size; i++) {
            assertEquals(i, map1.get(i).intValue());
        }
    }

    @Test
    public void testPutAllTooManyEntriesWithBackup() {
        int size = 10000;

        HazelcastInstance instance1 = instances[0];
        HazelcastInstance instance2 = instances[1];
        IMap<Integer, Integer> map1 = instance1.getMap("testPutAllTooManyEntries");
        IMap<Integer, Integer> map2 = instance2.getMap("testPutAllTooManyEntries");
        warmUpPartitions(instances);

        Map<Integer, Integer> mm = new HashMap<Integer, Integer>();
        for (int i = 0; i < size; i++) {
            mm.put(i, i);
        }

        map2.putAll(mm);
        assertEquals(size, map2.size());
        for (int i = 0; i < size; i++) {
            assertEquals(i, map2.get(i).intValue());
        }

        instance2.shutdown();
        assertEquals(size, map1.size());
        for (int i = 0; i < size; i++) {
            assertEquals(i, map1.get(i).intValue());
        }
    }

    @Test
    public void testPutAllAsync() {
        int size = 10000;

        IMap<Integer, Integer> map = instances[0].getMap("testPutAllAsync");
        Map<Integer, Integer> mm = new HashMap<Integer, Integer>();
        for (int i = 0; i < size; i++) {
            mm.put(i, i);
        }
        CompletableFuture<Void> future = map.putAllAsync(mm).toCompletableFuture();
        assertTrueEventually(() -> assertTrue(future.isDone()));
        assertEquals(map.size(), size);
        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i).intValue());
        }
    }

    @Test
    public void testSetAll() {
        int max = 10000;
        final IMap<Integer, Integer> map = instances[0].getMap(randomString());

        final CountDownLatch latch = new CountDownLatch(max);
        map.addEntryListener((EntryAddedListener<Integer, Integer>) event -> latch.countDown(), true);

        final Map<Integer, Integer> expected = IntStream.range(0, max).boxed()
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
        map.setAll(expected);

        assertEquals(max, map.size());
        expected.keySet().forEach(i -> assertEquals(i, map.get(i)));
        assertOpenEventually(latch);
    }

    @Test
    public void testSetAll_WhenKeyExists() {
        int max = 100;
        final IMap<Integer, Integer> map = instances[0].getMap(randomString());
        IntStream.range(0, max).forEach(i -> map.put(i, 0));
        assertEquals(max, map.size());

        final CountDownLatch latch = new CountDownLatch(max);
        map.addEntryListener((EntryAddedListener<Integer, Integer>) event -> latch.countDown(), true);

        final Map<Integer, Integer> expected = IntStream.range(0, max).boxed()
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
        map.setAll(expected);

        assertEquals(max, map.size());
        expected.keySet().forEach(i -> assertEquals(i, map.get(i)));
        assertOpenEventually(latch);
    }

    @Test
    public void testSetAllAsync() {
        int max = 100;
        final IMap<Integer, Integer> map = instances[0].getMap(randomString());

        final CountDownLatch latch = new CountDownLatch(max);
        map.addEntryListener((EntryAddedListener<Integer, Integer>) event -> latch.countDown(), true);

        final Map<Integer, Integer> expected = IntStream.range(0, max).boxed()
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
        final Future<Void> future = map.setAllAsync(expected).toCompletableFuture();

        assertEqualsEventually(future::isDone, true);
        assertEquals(max, map.size());
        expected.keySet().forEach(i -> assertEquals(i, map.get(i)));
        assertOpenEventually(latch);
    }

    @Test
    public void testMapListenersWithValue() {
        IMap<Object, Object> map = getInstance().getMap("testMapListenersWithValue");

        final Object[] addedKey = new Object[1];
        final Object[] addedValue = new Object[1];
        final Object[] updatedKey = new Object[1];
        final Object[] oldValue = new Object[1];
        final Object[] newValue = new Object[1];
        final Object[] removedKey = new Object[1];
        final Object[] removedValue = new Object[1];

        EntryListener<Object, Object> listener = new EntryAdapter<Object, Object>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                addedKey[0] = event.getKey();
                addedValue[0] = event.getValue();
            }

            @Override
            public void entryRemoved(EntryEvent<Object, Object> event) {
                removedKey[0] = event.getKey();
                removedValue[0] = event.getOldValue();
            }

            @Override
            public void entryUpdated(EntryEvent<Object, Object> event) {
                updatedKey[0] = event.getKey();
                oldValue[0] = event.getOldValue();
                newValue[0] = event.getValue();
            }

            public void entryEvicted(EntryEvent<Object, Object> event) {
            }

            @Override
            public void mapEvicted(MapEvent event) {
            }

            @Override
            public void mapCleared(MapEvent event) {
            }
        };
        map.addEntryListener(listener, true);

        map.put("key", "value");
        map.put("key", "value2");
        map.remove("key");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(addedKey[0], "key");
                assertEquals(addedValue[0], "value");
                assertEquals(updatedKey[0], "key");
                assertEquals(oldValue[0], "value");
                assertEquals(newValue[0], "value2");
                assertEquals(removedKey[0], "key");
                assertEquals(removedValue[0], "value2");
            }
        });
    }

    @Test
    public void testMapQueryListener() {
        IMap<Object, Object> map = getInstance().getMap(randomMapName());

        final Object[] addedKey = new Object[1];
        final Object[] addedValue = new Object[1];
        final Object[] updatedKey = new Object[1];
        final Object[] oldValue = new Object[1];
        final Object[] newValue = new Object[1];
        final Object[] removedKey = new Object[1];
        final Object[] removedValue = new Object[1];

        EntryListener<Object, Object> listener = new EntryAdapter<Object, Object>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                addedKey[0] = event.getKey();
                addedValue[0] = event.getValue();
            }

            @Override
            public void entryRemoved(EntryEvent<Object, Object> event) {
                removedKey[0] = event.getKey();
                removedValue[0] = event.getOldValue();
            }

            @Override
            public void entryUpdated(EntryEvent<Object, Object> event) {
                updatedKey[0] = event.getKey();
                oldValue[0] = event.getOldValue();
                newValue[0] = event.getValue();
            }

            @Override
            public void entryEvicted(EntryEvent<Object, Object> event) {
            }

            @Override
            public void mapEvicted(MapEvent event) {
            }

            @Override
            public void mapCleared(MapEvent event) {
            }
        };
        map.addEntryListener(listener, new StartsWithPredicate("a"), null, true);

        map.put("key1", "abc");
        map.put("key2", "bcd");
        map.put("key2", "axyz");
        map.remove("key1");

        assertTrueEventually(() -> {
            assertEquals("key1", addedKey[0]);
            assertEquals("abc", addedValue[0]);
            assertEquals("key2", updatedKey[0]);
            assertEquals("bcd", oldValue[0]);
            assertEquals("axyz", newValue[0]);
            assertEquals("key1", removedKey[0]);
            assertEquals("abc", removedValue[0]);
        });
    }

    private static class StartsWithPredicate implements Predicate<Object, Object>, Serializable {

        private static final long serialVersionUID = 4193947125511602220L;

        String pref;

        StartsWithPredicate(String pref) {
            this.pref = pref;
        }

        @Override
        public boolean apply(Map.Entry<Object, Object> mapEntry) {
            String val = (String) mapEntry.getValue();
            if (val == null) {
                return false;
            }
            if (val.startsWith(pref)) {
                return true;
            }
            return false;
        }
    }

    @Test
    public void testMapListenersWithValueAndKeyFiltered() {
        IMap<Object, Object> map = getInstance().getMap("testMapListenersWithValueAndKeyFiltered");

        final Object[] addedKey = new Object[1];
        final Object[] addedValue = new Object[1];
        final Object[] updatedKey = new Object[1];
        final Object[] oldValue = new Object[1];
        final Object[] newValue = new Object[1];
        final Object[] removedKey = new Object[1];
        final Object[] removedValue = new Object[1];

        EntryListener<Object, Object> listener = new EntryAdapter<Object, Object>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                addedKey[0] = event.getKey();
                addedValue[0] = event.getValue();
            }

            @Override
            public void entryRemoved(EntryEvent<Object, Object> event) {
                removedKey[0] = event.getKey();
                removedValue[0] = event.getOldValue();
            }

            @Override
            public void entryUpdated(EntryEvent<Object, Object> event) {
                updatedKey[0] = event.getKey();
                oldValue[0] = event.getOldValue();
                newValue[0] = event.getValue();
            }

            @Override
            public void entryEvicted(EntryEvent<Object, Object> event) {
            }

            @Override
            public void mapEvicted(MapEvent event) {
            }

            @Override
            public void mapCleared(MapEvent event) {
            }
        };
        map.addEntryListener(listener, "key", true);

        map.put("keyx", "valuex");
        map.put("key", "value");
        map.put("key", "value2");
        map.put("keyx", "valuex2");
        map.put("keyz", "valuez");
        map.remove("keyx");
        map.remove("key");
        map.remove("keyz");

        assertTrueEventually(() -> {
            assertEquals(addedKey[0], "key");
            assertEquals(addedValue[0], "value");
            assertEquals(updatedKey[0], "key");
            assertEquals(oldValue[0], "value");
            assertEquals(newValue[0], "value2");
            assertEquals(removedKey[0], "key");
            assertEquals(removedValue[0], "value2");
        });
    }

    @Test
    public void testMapListenersWithoutValue() {
        IMap<Object, Object> map = getInstance().getMap("testMapListenersWithoutValue");

        final Object[] addedKey = new Object[1];
        final Object[] addedValue = new Object[1];
        final Object[] updatedKey = new Object[1];
        final Object[] oldValue = new Object[1];
        final Object[] newValue = new Object[1];
        final Object[] removedKey = new Object[1];
        final Object[] removedValue = new Object[1];

        EntryListener<Object, Object> listener = new EntryAdapter<Object, Object>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                addedKey[0] = event.getKey();
                addedValue[0] = event.getValue();
            }

            @Override
            public void entryRemoved(EntryEvent<Object, Object> event) {
                removedKey[0] = event.getKey();
                removedValue[0] = event.getOldValue();
            }

            @Override
            public void entryUpdated(EntryEvent<Object, Object> event) {
                updatedKey[0] = event.getKey();
                oldValue[0] = event.getOldValue();
                newValue[0] = event.getValue();
            }

            @Override
            public void entryEvicted(EntryEvent<Object, Object> event) {
            }

            @Override
            public void mapEvicted(MapEvent event) {
            }

            @Override
            public void mapCleared(MapEvent event) {
            }
        };
        map.addEntryListener(listener, false);

        map.put("key", "value");
        map.put("key", "value2");
        map.remove("key");

        assertTrueEventually(() -> {
            assertEquals(addedKey[0], "key");
            assertEquals(addedValue[0], null);
            assertEquals(updatedKey[0], "key");
            assertEquals(oldValue[0], null);
            assertEquals(newValue[0], null);
            assertEquals(removedKey[0], "key");
            assertEquals(removedValue[0], null);
        });
    }

    @Test
    public void testPutWithTtl() {
        final IMap<String, String> map = getInstance().getMap("testPutWithTtl");

        map.put("key", "value", 2, SECONDS);

        assertTrueEventually(() -> assertNull(map.get("key")), 30);
    }

    @Test
    public void testSetTtlReturnsTrue() {
        final IMap<String, String> map = getInstance().getMap(randomString());

        map.put("key", "value");
        assertTrue(map.setTtl("key", 10, TimeUnit.SECONDS));
    }

    @Test
    public void testSetTtlReturnsFalse_whenKeyDoesNotExist() {
        final IMap<String, String> map = getInstance().getMap(randomString());
        assertFalse(map.setTtl("key", 10, TimeUnit.SECONDS));
    }

    @Test
    public void testSetTtlReturnsFalse_whenKeyIsAlreadyExpired() {
        final IMap<String, String> map = getInstance().getMap(randomString());
        map.put("key", "value", 1, TimeUnit.SECONDS);
        sleepAtLeastSeconds(5);
        assertFalse(map.setTtl("key", 10, TimeUnit.SECONDS));
    }

    @Test
    public void testAlterTTLOfAnEternalKey() {
        final IMap<String, String> map = getInstance().getMap("testSetTTL");

        map.put("key", "value");
        map.setTtl("key", 1, TimeUnit.SECONDS);

        sleepAtLeastMillis(2000);

        assertNull(map.get("key"));
    }

    @Test
    @Category(SlowTest.class)
    public void testExtendTTLOfAKeyBeforeItExpires() {
        final IMap<String, String> map = getInstance().getMap("testSetTTLExtend");
        map.put("key", "value", 10, TimeUnit.SECONDS);

        sleepAtLeastMillis(SECONDS.toMillis(1));
        //Make the entry eternal
        map.setTtl("key", 0, TimeUnit.DAYS);

        sleepAtLeastMillis(SECONDS.toMillis(15));

        assertEquals("value", map.get("key"));
    }

    @Test
    public void testSetTTLConfiguresMapPolicyIfTTLIsNegative() {
        final IMap<String, String> map = getInstance().getMap("mapWithTTL");
        map.put("tempKey", "tempValue", 10, TimeUnit.SECONDS);
        map.setTtl("tempKey", -1, TimeUnit.SECONDS);
        sleepAtLeastMillis(1000);
        assertNull(map.get("tempKey"));
    }

    @Test
    public void testJsonPutGet() {
        final IMap<String, HazelcastJsonValue> map = getInstance().getMap(randomMapName());
        HazelcastJsonValue value = new HazelcastJsonValue("{ \"age\": 4 }");
        map.put("item1", value);
        HazelcastJsonValue retrieved = map.get("item1");

        assertEquals(value, retrieved);
        assertEquals(4, Json.parse(retrieved.toString()).asObject().get("age").asInt());
    }

    @Test
    public void testMapEntryProcessor() {
        IMap<Integer, Integer> map = getInstance().getMap("testMapEntryProcessor");
        map.put(1, 1);
        SampleEntryProcessor<Integer> entryProcessor = new SampleEntryProcessor<>();
        map.executeOnKey(1, entryProcessor);
        assertEquals(map.get(1), (Object) 2);
    }

    @Test
    public void testIfWeCarryRecordVersionInfoToReplicas() {
        String mapName = randomMapName();
        int mapSize = 1000;
        int expectedRecordVersion = 3;

        HazelcastInstance node1 = instances[1];

        IMap<Integer, Integer> map1 = node1.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            map1.put(i, 0); // version 0
            map1.put(i, 1); // version 1
            map1.put(i, 2); // version 2
            map1.put(i, 3); // version 3
        }
        HazelcastInstance node2 = instances[2];

        node1.shutdown();

        IMap<Integer, Integer> map3 = node2.getMap(mapName);

        for (int i = 0; i < mapSize; i++) {
            EntryView<Integer, Integer> entryView = map3.getEntryView(i);
            assertEquals(expectedRecordVersion, entryView.getVersion());
        }
    }

    @Test
    public void github_11489_verifyNoFailingCastOnValue() {
        // always run map.values on a map proxy backed by the current-version instance otherwise, when running as compatibility
        // test, the TestPagingPredicate will be proxied and fail with a NullPointerException during serialization
        IMap<Integer, Integer> test = instances[instances.length - 1].getMap("github_11489");
        for (int i = 0; i < 100; i++) {
            test.put(i, i);
        }

        Collection<Integer> values = test.values(Predicates.pagingPredicate(100));
        Type genericSuperClass = values.getClass().getGenericSuperclass();
        Type actualType = ((ParameterizedType) genericSuperClass).getActualTypeArguments()[0];
        // Raw class is expected. ParameterizedType-s cause troubles to Jackson serializer.
        assertInstanceOf(Class.class, actualType);
    }

    @Test
    public void testNullChecks() {
        final IMap<String, String> map = getInstance().getMap("testNullChecks");

        Runnable runnable;

        runnable = new Runnable() {
            public void run() {
                map.containsKey(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "containsKey(null)");

        runnable = new Runnable() {
            public void run() {
                map.containsValue(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "containsValue(null)");

        runnable = new Runnable() {
            public void run() {
                map.get(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "get(null)");

        runnable = new Runnable() {
            public void run() {
                map.put(null, "value");
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "put(null, \"value\")");

        runnable = new Runnable() {
            public void run() {
                map.put("key", null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "put(\"key\", null)");

        runnable = new Runnable() {
            public void run() {
                map.remove(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "remove(null)");

        runnable = new Runnable() {
            public void run() {
                map.remove(null, "value");
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "remove(null, \"value\")");

        runnable = new Runnable() {
            public void run() {
                map.remove("key", null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "remove(\"key\", null)");

        runnable = new Runnable() {
            public void run() {
                map.delete(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "delete(null)");

        final Set<String> keys = new HashSet<String>();
        keys.add("key");
        keys.add(null);
        runnable = new Runnable() {
            public void run() {
                map.getAll(keys);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "getAll(keys)");

        runnable = new Runnable() {
            public void run() {
                map.executeOnKeys(keys, new EntryProcessor<String, String, Object>() {
                    @Override
                    public Object process(Map.Entry entry) {
                        return null;
                    }

                    @Override
                    public EntryProcessor<String, String, Object> getBackupProcessor() {
                        return null;
                    }
                });
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "executeOnKeys(keys, entryProcessor)");

        runnable = new Runnable() {
            public void run() {
                map.getAsync(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "getAsync(null)");

        runnable = new Runnable() {
            public void run() {
                map.putAsync(null, "value");
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putAsync(null, \"value\")");

        runnable = new Runnable() {
            public void run() {
                map.putAsync("key", null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putAsync(\"key\", null)");

        runnable = new Runnable() {
            public void run() {
                map.putAsync(null, "value", 1, SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putAsync(null, \"value\", 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.putAsync("key", null, 1, SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putAsync(\"key\", null, 1, TimeUnit.SECONDS)");

        runnable = () -> ((MapProxyImpl<String, String>) map).putIfAbsentAsync(null, "value");
        assertRunnableThrowsNullPointerException(runnable, "putIfAbsentAsync(null, \"value\")");

        runnable = () -> ((MapProxyImpl<String, String>) map).putIfAbsentAsync("key", null);
        assertRunnableThrowsNullPointerException(runnable, "putIfAbsentAsync(\"key\", null)");

        runnable = () -> ((MapProxyImpl<String, String>) map).putIfAbsentAsync(null, "value", 1, SECONDS);
        assertRunnableThrowsNullPointerException(runnable, "putIfAbsentAsync(null, \"value\", 1, TimeUnit.SECONDS)");

        runnable = () -> ((MapProxyImpl<String, String>) map).putIfAbsentAsync("key", null, 1, SECONDS);
        assertRunnableThrowsNullPointerException(runnable, "putIfAbsentAsync(\"key\", null, 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.removeAsync(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "removeAsync(null)");

        runnable = new Runnable() {
            public void run() {
                map.tryRemove(null, 1, SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "tryRemove(null, 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.tryPut(null, "value", 1, SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "tryPut(null, \"value\", 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.tryPut("key", null, 1, SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "tryPut(\"key\", null, 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.putTransient(null, "value", 1, SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putTransient(null, \"value\", 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.putTransient("key", null, 1, SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putTransient(\"key\", null, 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.putIfAbsent(null, "value");
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putIfAbsent(null, \"value\")");

        runnable = new Runnable() {
            public void run() {
                map.putIfAbsent("key", null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putIfAbsent(\"key\", null)");

        runnable = new Runnable() {
            public void run() {
                map.putIfAbsent(null, "value", 1, SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putIfAbsent(null, \"value\", 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.putIfAbsent("key", null, 1, SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putIfAbsent(\"key\", null, 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.replace(null, "oldValue", "newValue");
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "replace(null, \"oldValue\", \"newValue\")");

        runnable = new Runnable() {
            public void run() {
                map.replace("key", null, "newValue");
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "replace(\"key\", null, \"newValue\")");

        runnable = new Runnable() {
            public void run() {
                map.replace("key", "oldValue", null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "replace(\"key\", \"oldValue\", null)");

        runnable = new Runnable() {
            public void run() {
                map.replace(null, "value");
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "replace(null, \"value\")");

        runnable = new Runnable() {
            public void run() {
                map.replace("key", null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "replace(\"key\", null)");

        runnable = new Runnable() {
            public void run() {
                map.set(null, "value");
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "set(null, \"value\")");

        runnable = new Runnable() {
            public void run() {
                map.set("key", null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "set(\"key\", null)");

        runnable = new Runnable() {
            public void run() {
                map.set(null, "value", 1, SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "set(null, \"value\", 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.set("key", null, 1, SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "set(\"key\", null, 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.lock(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "lock(null)");

        runnable = new Runnable() {
            public void run() {
                map.lock(null, 1, SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "lock(null, 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.isLocked(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "isLocked(null)");

        runnable = new Runnable() {
            public void run() {
                map.tryLock(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "tryLock(null)");

        runnable = new Runnable() {
            public void run() {
                try {
                    map.tryLock(null, 1, SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "tryLock(null, 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.unlock(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "unlock(null)");

        runnable = new Runnable() {
            public void run() {
                map.forceUnlock(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "forceUnlock(null)");

        runnable = new Runnable() {
            public void run() {
                map.getEntryView(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "getEntryView(null)");

        runnable = new Runnable() {
            public void run() {
                map.evict(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "evict(null)");

        runnable = new Runnable() {
            public void run() {
                map.executeOnKey(null, new SampleEntryProcessor());
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "executeOnKey(null, entryProcessor)");

        final Map<String, String> mapWithNullKey = new HashMap<String, String>();
        mapWithNullKey.put("key", "value");
        mapWithNullKey.put(null, "nullKey");
        runnable = new Runnable() {
            public void run() {
                map.putAll(mapWithNullKey);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "map.putAll(mapWithNullKey)");

        final Map<String, String> mapWithNullValue = new HashMap<String, String>();
        mapWithNullValue.put("key", "value");
        mapWithNullValue.put("nullValue", null);
        runnable = new Runnable() {
            public void run() {
                map.putAll(mapWithNullValue);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "map.putAll(mapWithNullValue)");

        // we need to run the putAll() tests a second time passing in a map with more than (partitionCount * 3) entries,
        // because MapProxySupport#putAllInternal() takes a different code path if there are more than that many entries
        final int entryLimit = (INSTANCE_COUNT * 3) + 1;

        for (int i = 0; i < entryLimit; i++) {
            mapWithNullKey.put("key" + i, "value" + i);
        }
        runnable = new Runnable() {
            public void run() {
                map.putAll(mapWithNullKey);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "map.putAll(mapWithNullKey)");

        for (int i = 0; i < entryLimit; i++) {
            mapWithNullValue.put("key" + i, "value" + i);
        }
        runnable = new Runnable() {
            public void run() {
                map.putAll(mapWithNullValue);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "map.putAll(mapWithNullValue)");

        runnable = new Runnable() {
            public void run() {
                map.putAll(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "map.putAll(null)");

        runnable = new Runnable() {
            public void run() {
                map.executeOnKeys(null, new EntryProcessor<String, String, Object>() {
                    @Override
                    public Object process(Map.Entry entry) {
                        return null;
                    }

                    @Override
                    public EntryProcessor<String, String, Object> getBackupProcessor() {
                        return null;
                    }
                });
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "map.executeOnKeys(null, entryProcessor)");

    }

    public void assertRunnableThrowsNullPointerException(Runnable runnable, String description) {
        boolean threwNpe = false;
        try {
            runnable.run();
        } catch (NullPointerException npe) {
            threwNpe = true;
        }
        assertTrue(description + " did not throw a NullPointerException.", threwNpe);
    }

    private static class SampleEntryProcessor<K> implements EntryProcessor<K, Integer, Boolean>, Serializable {

        private static final long serialVersionUID = -5735493325953375570L;

        @Override
        public Boolean process(Map.Entry<K, Integer> entry) {
            entry.setValue(entry.getValue() + 1);
            return true;
        }
    }

    @Test
    public void testForEachWithALambdaFunction() {
        final IMap<String, Integer> sourceMap = getSourceMapFor_ForEach_Test();
        final IMap<String, Integer> targetMap = getTargetMapFor_ForEach_Test();

        sourceMap.forEach((k, v) -> {
            targetMap.put(k, v);
        });

        assertEntriesEqual(sourceMap, targetMap);
    }

    @Test
    public void testForEachWithStaticSerializableAction() throws IOException {
        final IMap<String, Integer> sourceMap = getSourceMapFor_ForEach_Test();

        //Create a bi-consumer which writes both args to a file
        File tempFile = File.createTempFile("Map", ".txt");
        tempFile.deleteOnExit();
        StaticSerializableBiConsumer action = new StaticSerializableBiConsumer(tempFile.getAbsolutePath());

        sourceMap.forEach(action);

        //Verify that all map entries got written to the file
        List<String> lines = Files.readAllLines(tempFile.toPath());
        boolean allEntriesProcessed = sourceMap.entrySet().stream().allMatch(e -> lines.contains(e.getKey() + "#" + e.getValue()));
        assertTrue(allEntriesProcessed);
    }

    private IMap<String, Integer> getSourceMapFor_ForEach_Test() {
        final IMap<String, Integer> sourceMap = getInstance().getMap("source_map");
        sourceMap.put("k1", 1);
        sourceMap.put("k2", 2);
        return sourceMap;
    }

    private IMap<String, Integer> getTargetMapFor_ForEach_Test() {
        return getInstance().getMap("target_map");
    }

    private void assertEntriesEqual(IMap<String, Integer> sourceMap, IMap<String, Integer> targetMap) {
        sourceMap.entrySet().forEach(e -> assertEquals(e.getValue(), targetMap.get(e.getKey())));
    }

    @Test
    public void testReplaceAllWithLambdaFunction() {
        final IMap<String, Integer> testMap = getInstance().getMap("test_map");
        testMap.put("k1", 1);
        testMap.put("k2", 2);
        testMap.replaceAll((k, v) -> v * 10);
        assertEquals((int) testMap.get("k1"), 10);
        assertEquals((int) testMap.get("k2"), 20);
    }

    @Test
    public void testReplaceAllWithStaticSerializableFunction() {
        final IMap<String, String> testMap = getInstance().getMap("test_map");
        testMap.put("k1", "v1");
        testMap.put("k2", "v2");
        testMap.replaceAll(new StaticSerializableBiFunction("v_new"));
        assertEquals(testMap.get("k1"), "v_new");
        assertEquals(testMap.get("k2"), "v_new");
    }

}

