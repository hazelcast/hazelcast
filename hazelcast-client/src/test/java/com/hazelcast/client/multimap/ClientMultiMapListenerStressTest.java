package com.hazelcast.client.multimap;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.map.MapWideEvent;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ProblematicTest;
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

    static final int MAX_SECONDS = 60 * 10;
    static final String MAP_NAME = randomString();
    static final int NUMBER_OF_CLIENTS = 8;
    static final int THREADS_PER_CLIENT = 8;

    static HazelcastInstance server;

    @BeforeClass
    public static void init() {
        server = Hazelcast.newHazelcastInstance();
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Category(ProblematicTest.class)
    @Test
    public void listenerAddStressTest() throws InterruptedException {
        final PutItemsThread[] putThreads = new PutItemsThread[NUMBER_OF_CLIENTS * THREADS_PER_CLIENT];

        int idx=0;
        for(int i=0; i<NUMBER_OF_CLIENTS; i++){
            HazelcastInstance client = HazelcastClient.newHazelcastClient();
            for(int j=0; j<THREADS_PER_CLIENT; j++){
                PutItemsThread t = new PutItemsThread(client);
                putThreads[idx++]=t;
            }
        }

        for(int i=0; i<putThreads.length; i++){
            putThreads[i].start();
        }
        MultiMap mm = server.getMultiMap(MAP_NAME);

        assertJoinable(MAX_SECONDS, putThreads );
        assertEquals(PutItemsThread.MAX_ITEMS * putThreads.length, mm.size());
        assertTrueEventually(new AssertTask() {

            public void run() throws Exception {
                for(int i=0; i<putThreads.length; i++){
                    putThreads[i].assertResult(PutItemsThread.MAX_ITEMS * putThreads.length);
                }
            }
        });
    }

    public class PutItemsThread extends Thread{
        public static final int MAX_ITEMS = 1000;

        public final MyEntryListener listener = new MyEntryListener();
        public HazelcastInstance hzInstance;
        public MultiMap mm;
        public String id;

        public PutItemsThread(HazelcastInstance hzInstance){
            this.id = randomString();
            this.hzInstance = hzInstance;
            this.mm = hzInstance.getMultiMap(MAP_NAME);
            mm.addEntryListener(listener, true);
        }

        public void run(){
            for(int i=0; i< MAX_ITEMS; i++){
                mm.put(id+i, id+i);
            }
        }

        public void assertResult(int target){
            System.out.println("listener "+id+" add events received "+listener.add.get());
            assertEquals(target, listener.add.get());
        }
    }

    static class MyEntryListener implements EntryListener {
        public AtomicInteger add = new AtomicInteger(0);

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
        public void evictedAll(MapWideEvent event) {
            // TODO what to do here?
        }
    };
}