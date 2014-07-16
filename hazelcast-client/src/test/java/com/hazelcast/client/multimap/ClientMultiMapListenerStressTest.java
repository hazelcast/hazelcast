package com.hazelcast.client.multimap;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.MultiMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.assertJoinable;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static junit.framework.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class ClientMultiMapListenerStressTest {

    private static final long MAX_SECONDS = TimeUnit.MINUTES.toSeconds(10);
    private static final int NUMBER_OF_CLIENTS = 8;
    private static final int THREADS_PER_CLIENT = 8;
    private static final String MAP_NAME = randomString();

    private static HazelcastInstance server;

    @BeforeClass
    public static void beforeClass() {
        Config cfg = new Config();
        cfg.setProperty("hazelcast.event.queue.capacity", "5000000");
        server = Hazelcast.newHazelcastInstance(cfg);
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void listenerAddStressTest() throws InterruptedException {
        final PutItemsThread[] putThreads = new PutItemsThread[NUMBER_OF_CLIENTS * THREADS_PER_CLIENT];

        int index = 0;
        for (int i = 0; i < NUMBER_OF_CLIENTS; i++) {
            HazelcastInstance client = HazelcastClient.newHazelcastClient();
            for (int j = 0; j < THREADS_PER_CLIENT; j++) {
                PutItemsThread t = new PutItemsThread(client);
                putThreads[index++] = t;
            }
        }

        for (PutItemsThread putThread : putThreads) {
            putThread.start();
        }
        MultiMap multiMap = server.getMultiMap(MAP_NAME);

        assertJoinable(MAX_SECONDS, putThreads);
        assertEquals(PutItemsThread.MAX_ITEMS * putThreads.length, multiMap.size());
        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                for (PutItemsThread putThread : putThreads) {
                    putThread.assertResult(PutItemsThread.MAX_ITEMS * putThreads.length);
                }
            }
        });
    }

    private class PutItemsThread extends Thread {
        private static final int MAX_ITEMS = 1000;

        private final String id;
        private final MultiMap<Object, Object> multiMap;
        private final MyEntryListener listener;

        public PutItemsThread(HazelcastInstance client) {
            this.id = randomString();
            this.multiMap = client.getMultiMap(MAP_NAME);
            this.listener = new MyEntryListener();

            multiMap.addEntryListener(listener, true);
        }

        public void run() {
            for (int i = 0; i < MAX_ITEMS; i++) {
                multiMap.put(id + i, id + i);
            }
        }

        public void assertResult(int target) {
            System.out.println("listener " + id + " add events received " + listener.add.get());
            assertEquals(target, listener.add.get());
        }
    }

    private static class MyEntryListener implements EntryListener<Object, Object> {
        private AtomicInteger add = new AtomicInteger(0);

        public void entryAdded(EntryEvent event) {
            add.incrementAndGet();
        }

        public void entryRemoved(EntryEvent event) {
        }

        public void entryUpdated(EntryEvent event) {
        }

        public void entryEvicted(EntryEvent event) {
        }

        @Override
        public void mapEvicted(MapEvent event) {
        }

        @Override
        public void mapCleared(MapEvent event) {
        }
    }
}