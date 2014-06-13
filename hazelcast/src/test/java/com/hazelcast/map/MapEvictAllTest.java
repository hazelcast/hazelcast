package com.hazelcast.map;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
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
        final HazelcastInstance node = createHazelcastInstance();
        final IMap map = node.getMap(randomMapName());
        map.addLocalEntryListener(new EntryAdapter() {
            @Override
            public void onEvictAll(MapEvent event) {
                final int numberOfEntries = event.getNumberOfEntriesAffected();
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
        final int numberOfEntries = 1000;
        final int numberOfLockedKeys = 123;
        final int expectedNumberOfEvictedKeys = numberOfEntries - numberOfLockedKeys;
        final CountDownLatch countDownLatch = new CountDownLatch(expectedNumberOfEvictedKeys);
        final HazelcastInstance node = createHazelcastInstance();
        final IMap map = node.getMap(randomMapName());
        map.addLocalEntryListener(new EntryAdapter() {
            @Override
            public void onEvictAll(MapEvent event) {
                final int numberOfEntries = event.getNumberOfEntriesAffected();
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


    @Test
    public void testEvictAll_onBackup() throws Exception {
        final String mapName = randomMapName();
        final CountDownLatch countDownLatch = new CountDownLatch(10000);
        final TestHazelcastInstanceFactory instanceFactory = new TestHazelcastInstanceFactory(5);
        final HazelcastInstance node1 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance node2 = instanceFactory.newHazelcastInstance();
        final IMap map = node1.getMap(mapName);
        map.addEntryListener(new EntryAdapter() {
            @Override
            public void onEvictAll(MapEvent event) {
                System.out.println("event = " + event);
                final int numberOfEntries = event.getNumberOfEntriesAffected();
                for (int i = 0; i < numberOfEntries; i++) {
                    countDownLatch.countDown();
                }
            }
        }, false);
        for (int i = 0; i < 10000; i++) {
            map.put(i, i);
        }
        map.evictAll();
        assertOpenEventually(countDownLatch);
        assertEquals(0, countDownLatch.getCount());


    }

}