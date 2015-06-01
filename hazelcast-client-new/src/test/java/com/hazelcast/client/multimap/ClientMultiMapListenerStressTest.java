package com.hazelcast.client.multimap;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.assertJoinable;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static junit.framework.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class ClientMultiMapListenerStressTest {

    private static final int MAX_SECONDS = 60 * 10;
    private static final int NUMBER_OF_CLIENTS = 8;
    private static final int THREADS_PER_CLIENT = 4;
    private static final String MAP_NAME = randomString();


    static HazelcastInstance server;

    @BeforeClass
    public static void init() {
        Config cfg = new Config();
        cfg.setProperty("hazelcast.event.queue.capacity", "5000000");
        server = Hazelcast.newHazelcastInstance(cfg);
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void listenerAddStressTest() throws InterruptedException {
        final PutItemsThread[] putThreads = new PutItemsThread[NUMBER_OF_CLIENTS * THREADS_PER_CLIENT];

        int idx = 0;
        for (int i = 0; i < NUMBER_OF_CLIENTS; i++) {
            HazelcastInstance client = HazelcastClient.newHazelcastClient();
            for (int j = 0; j < THREADS_PER_CLIENT; j++) {
                PutItemsThread t = new PutItemsThread(client);
                putThreads[idx++] = t;
            }
        }

        for (int i = 0; i < putThreads.length; i++) {
            putThreads[i].start();
        }
        MultiMap multiMap = server.getMultiMap(MAP_NAME);


        assertJoinable(MAX_SECONDS, putThreads);

        final int expectedSize = PutItemsThread.MAX_ITEMS * putThreads.length;
        assertEquals(expectedSize, multiMap.size());
        assertReceivedEventsSize(expectedSize, putThreads);

    }

    private void assertReceivedEventsSize(final int expectedSize, final PutItemsThread[] putThreads) {
        for (int i = 0; i < putThreads.length; i++) {
            putThreads[i].assertResult(expectedSize);
        }
    }

    public class PutItemsThread extends Thread {
        public static final int MAX_ITEMS = 100;

        public final MyEntryListener listener = new MyEntryListener();
        public HazelcastInstance client;
        public MultiMap mm;
        public String id;

        public PutItemsThread(HazelcastInstance client) {
            this.id = randomString();
            this.client = client;
            this.mm = client.getMultiMap(MAP_NAME);
            mm.addEntryListener(listener, true);
        }

        public void run() {
            for (int i = 0; i < MAX_ITEMS; i++) {
                mm.put(id + i, id + i);
            }
        }

        public void assertResult(final int target) {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(target, listener.add.get());
                }
            });
        }
    }

    static class MyEntryListener extends EntryAdapter {
        public AtomicInteger add = new AtomicInteger(0);

        public void entryAdded(EntryEvent event) {
            add.incrementAndGet();
        }
    }

}