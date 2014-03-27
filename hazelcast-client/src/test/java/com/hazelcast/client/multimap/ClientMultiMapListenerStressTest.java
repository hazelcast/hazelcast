package com.hazelcast.client.multimap;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.*;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static junit.framework.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientMultiMapListenerStressTest {

    static final String MAP_NAME = randomString();
    static final int NUMBER_OF_CLIENTS = 1;
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


    @Test
    @Category(ProblematicTest.class)
    public void listenerAddStressTest() throws InterruptedException {

        PutItemsThread[] putThreads = new PutItemsThread[NUMBER_OF_CLIENTS * THREADS_PER_CLIENT];

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

        assertJoinable(putThreads);

        MultiMap mm = server.getMultiMap(MAP_NAME);
        assertEquals(PutItemsThread.maxItems * putThreads.length, mm.size());

        for(int i=0; i<putThreads.length; i++){
            putThreads[i].assertResult(PutItemsThread.maxItems * putThreads.length);
        }
    }

    public class PutItemsThread extends Thread{

        public static final int maxItems = 1000;

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
            for(int i=0; i<maxItems; i++){
                mm.put(id+i, id+i);
            }
        }

        public void assertResult(int target){
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
    };
}