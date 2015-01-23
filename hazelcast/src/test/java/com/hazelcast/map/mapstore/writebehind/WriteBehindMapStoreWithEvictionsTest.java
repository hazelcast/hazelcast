package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindMapStoreWithEvictionsTest extends HazelcastTestSupport {

    @Test
    public void testWriteBehind_callEvictBeforePersisting() throws Exception {
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final IMap<Object, Object> map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withBackupCount(0)
                .withWriteDelaySeconds(100)
                .withPartitionCount(1)
                .build();
        final int numberOfItems = 1000;
        populateMap(map, numberOfItems);
        evictMap(map, numberOfItems);

        assertFinalValueEqualsForEachEntry(map, numberOfItems);
    }

    @Test
    public void testWriteBehind_callEvictBeforePersisting_onSameKey() throws Exception {
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final IMap<Object, Object> map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withBackupCount(0)
                .withWriteDelaySeconds(3)
                .withPartitionCount(1)
                .build();
        final int numberOfUpdates = 1000;
        final int key = 0;
        continuouslyUpdateKey(map, numberOfUpdates, key);
        map.evict(0);
        final int expectedLastValue = numberOfUpdates - 1;

        assertFinalValueEquals(expectedLastValue, (Integer) map.get(0));
    }

    @Test
    public void testWriteBehind_callEvictBeforePersisting_onSameKey_thenCallRemove() throws Exception {
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final IMap<Object, Object> map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withBackupCount(0)
                .withWriteDelaySeconds(100)
                .withPartitionCount(1)
                .build();
        final int numberOfUpdates = 1000;
        final int key = 0;
        continuouslyUpdateKey(map, numberOfUpdates, key);
        map.evict(0);
        final Object previousValue = map.remove(0);
        final int expectedLastValue = numberOfUpdates - 1;

        assertFinalValueEquals(expectedLastValue, (Integer) previousValue);
    }

    @Test
    public void testWriteBehind_callEvictBeforePersisting_onSameKey_thenCallRemoveMultipleTimes() throws Exception {
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final IMap<Object, Object> map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withBackupCount(0)
                .withWriteDelaySeconds(100)
                .withPartitionCount(1)
                .build();
        final int numberOfUpdates = 1000;
        final int key = 0;
        continuouslyUpdateKey(map, numberOfUpdates, key);
        map.evict(0);
        map.remove(0);
        final Object previousValue = map.remove(0);

        assertNull(null, previousValue);
    }


    @Test
    public void evict_then_loadAll_onSameKey() throws Exception {
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final IMap<Object, Object> map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withBackupCount(0)
                .withWriteDelaySeconds(100)
                .withPartitionCount(1)
                .build();

        map.put(1, 100);

        final Map<Integer, Integer> fill = new HashMap<Integer, Integer>();
        fill.put(1, -1);
        mapStore.storeAll(fill);

        map.evict(1);

        final Set<Object> loadKeys = new HashSet<Object>();
        loadKeys.add(1);

        map.loadAll(loadKeys, true);

        assertEquals(100, map.get(1));
    }

    @Test
    public void evictAll_then_loadAll_onSameKey() throws Exception {
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final IMap<Object, Object> map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withBackupCount(0)
                .withWriteDelaySeconds(100)
                .withPartitionCount(1)
                .build();

        map.put(1, 100);

        final Map<Integer, Integer> fill = new HashMap<Integer, Integer>();
        fill.put(1, -1);
        mapStore.storeAll(fill);

        map.evictAll();

        final Set<Object> loadKeys = new HashSet<Object>();
        loadKeys.add(1);

        map.loadAll(loadKeys, true);

        assertEquals(100, map.get(1));
    }

    @Test
    public void testWriteBehindFlushPersistsAllRecords_afterShutdownAll() throws Exception {
        int nodeCount = 2;
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final IMap<Object, Object> map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(nodeCount)
                .withNodeFactory(factory)
                .withBackupCount(0)
                .withWriteDelaySeconds(100)
                .withPartitionCount(100)
                .build();
        final int numberOfItems = 1000;

        // add some expiration logic by setting a 10 seconds ttl to puts.
        populateMap(map, numberOfItems, 10);

        factory.shutdownAll();

        assertEquals(numberOfItems, mapStore.countStore.get());
    }


    @Test
    public void testWriteBehind_shouldNotMakeDuplicateStoreOperationForAKey_uponEviction() throws Exception {
        final AtomicInteger storeCount = new AtomicInteger(0);
        MapStore store = createSlowMapStore(storeCount);

        IMap<Object, Object> map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(store)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withBackupCount(0)
                .withWriteDelaySeconds(1)
                .build();

        map.put(1, 1);
        map.evict(1);

        // give some time to process write-behind.
        sleepSeconds(2);

        assertStoreCount(1, storeCount);
    }

    private void assertStoreCount(final int expected, final AtomicInteger storeCount) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, storeCount.get());
            }
        });
    }

    private MapStore createSlowMapStore(final AtomicInteger storeCount) {
        return new MapStoreAdapter<Integer, Integer>() {
            @Override
            public void store(Integer key, Integer value) {
                storeCount.incrementAndGet();
                sleepSeconds(5);
            }
        };
    }

    private void assertFinalValueEquals(final int expected, final int actual) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, actual);
            }
        }, 20);
    }


    private void populateMap(IMap map, int numberOfItems) {
        populateMap(map, numberOfItems, 0);
    }

    private void populateMap(IMap map, int numberOfItems, int ttlSeconds) {
        for (int i = 0; i < numberOfItems; i++) {
            map.put(i, i, ttlSeconds, TimeUnit.SECONDS);
        }
    }

    private void continuouslyUpdateKey(IMap map, int numberOfUpdates, int key) {
        for (int i = 0; i < numberOfUpdates; i++) {
            map.put(key, i);
        }
    }

    private void evictMap(IMap map, int numberOfItems) {
        for (int i = 0; i < numberOfItems; i++) {
            map.evict(i);
        }
    }

    private void assertFinalValueEqualsForEachEntry(IMap map, int numberOfItems) {
        for (int i = 0; i < numberOfItems; i++) {
            assertFinalValueEquals(i, (Integer) map.get(i));
        }
    }

}
