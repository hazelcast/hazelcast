package com.hazelcast.map;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapEvictAllTest extends HazelcastTestSupport {

    @Test
    public void testEvictAll_firesEvent() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1000);
        HazelcastInstance node = createHazelcastInstance();
        IMap<Integer, Integer> map = node.getMap(randomMapName());
        map.addLocalEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void mapEvicted(MapEvent event) {
                int numberOfEntries = event.getNumberOfEntriesAffected();
                for (int i = 0; i < numberOfEntries; i++) {
                    countDownLatch.countDown();
                }
            }
        });

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }
        map.evictAll();

        assertOpenEventually(countDownLatch);
    }

    @Test
    public void testEvictAll_preserveLockedKeys() throws Exception {
        int numberOfEntries = 1000;
        int numberOfLockedKeys = 123;
        int expectedNumberOfEvictedKeys = numberOfEntries - numberOfLockedKeys;
        final CountDownLatch countDownLatch = new CountDownLatch(expectedNumberOfEvictedKeys);
        HazelcastInstance node = createHazelcastInstance();
        IMap<Integer, Integer> map = node.getMap(randomMapName());
        map.addLocalEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void mapEvicted(MapEvent event) {
                int numberOfEntries = event.getNumberOfEntriesAffected();
                for (int i = 0; i < numberOfEntries; i++) {
                    countDownLatch.countDown();
                }
            }
        });

        for (int i = 0; i < numberOfEntries; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < numberOfLockedKeys; i++) {
            map.lock(i);
        }
        map.evictAll();

        assertOpenEventually(countDownLatch);
        assertEquals(0, countDownLatch.getCount());
    }

    /**
     * Fails rarely but randomly at line:
     * assertEquals(0, map1.getLocalMapStats().getHeapCost());
     *
     * java.lang.AssertionError:
     * Expected :0
     * Actual   :value differs, e.g. 13440, 107016, 37296
     */
    @Test
    @Category(ProblematicTest.class)
    public void testEvictAll_onBackup() throws Exception {
        int numberOfEntries = 10000;
        String mapName = randomMapName();
        final CountDownLatch countDownLatch = new CountDownLatch(numberOfEntries);
        TestHazelcastInstanceFactory instanceFactory = new TestHazelcastInstanceFactory(5);
        HazelcastInstance node1 = instanceFactory.newHazelcastInstance();
        HazelcastInstance node2 = instanceFactory.newHazelcastInstance();
        IMap<Integer, Integer> map1 = node1.getMap(mapName);
        IMap<Integer, Integer> map2 = node2.getMap(mapName);
        map1.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void mapEvicted(MapEvent event) {
                int numberOfEntries = event.getNumberOfEntriesAffected();
                for (int i = 0; i < numberOfEntries; i++) {
                    countDownLatch.countDown();
                }
            }
        }, false);

        for (int i = 0; i < numberOfEntries; i++) {
            map1.put(i, i);
        }
        map1.evictAll();

        assertOpenEventually(countDownLatch);
        assertEquals(0, countDownLatch.getCount());
        assertEquals(0, map1.getLocalMapStats().getHeapCost());
        assertEquals(0, map2.getLocalMapStats().getHeapCost());
    }

}