package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.Ignore;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
@Ignore
public class ClientMapEvictAllTest extends HazelcastTestSupport {

    @Test
    public void evictAll_firesEvent() throws Exception {
        final String mapName = randomMapName();
        final HazelcastInstance server1 = Hazelcast.newHazelcastInstance(null);
        final HazelcastInstance server2 = Hazelcast.newHazelcastInstance(null);
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();
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
        closeResources(client, server1, server2);
    }

    @Test
    public void evictAll_firesOnlyOneEvent() throws Exception {
        final String mapName = randomMapName();
        final HazelcastInstance server1 = Hazelcast.newHazelcastInstance(null);
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();
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
        closeResources(client, server1);
    }


    private static void closeResources(HazelcastInstance... instances) {
        if (instances == null) {
            return;
        }
        for (HazelcastInstance instance : instances) {
            instance.shutdown();
        }
    }
}
