package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapEvictAllTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void evictAll_firesEvent() throws Exception {
        final String mapName = randomMapName();
        Config config = getConfig();
        hazelcastFactory.newHazelcastInstance(config);
        hazelcastFactory.newHazelcastInstance(config);
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        final IMap<Object, Object> map = client.getMap(mapName);
        final CountDownLatch evictedEntryCount = new CountDownLatch(3);
        map.addEntryListener(new EntryAdapter<Object, Object>() {
            @Override
            public void mapEvicted(MapEvent event) {
                final int affected = event.getNumberOfEntriesAffected();
                for (int i = 0; i < affected; i++) {
                    evictedEntryCount.countDown();
                }

            }
        }, true);

        map.put(1, 1);
        map.put(2, 1);
        map.put(3, 1);
        map.evictAll();

        assertOpenEventually(evictedEntryCount);
        assertEquals(0, map.size());
    }

    @Test
    public void evictAll_firesOnlyOneEvent() throws Exception {
        final String mapName = randomMapName();
        Config config = getConfig();
        hazelcastFactory.newHazelcastInstance(config);
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        final IMap<Object, Object> map = client.getMap(mapName);
        final CountDownLatch eventCount = new CountDownLatch(2);
        map.addEntryListener(new EntryAdapter<Object, Object>() {
            @Override
            public void mapEvicted(MapEvent event) {
                eventCount.countDown();

            }
        }, true);

        map.put(1, 1);
        map.put(2, 1);
        map.put(3, 1);
        map.evictAll();

        assertFalse(eventCount.await(10, TimeUnit.SECONDS));
        assertEquals(1, eventCount.getCount());
    }
}
