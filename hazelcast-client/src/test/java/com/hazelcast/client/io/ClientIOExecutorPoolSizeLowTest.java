package com.hazelcast.client.io;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by danny on 12/16/13.
 */

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientIOExecutorPoolSizeLowTest {

    static HazelcastInstance server1;
    static HazelcastInstance server2;
    static HazelcastInstance server3;
    static HazelcastInstance client;

    static final int executorPoolSize=1;

    static String entryCounterID;

    @Before
    public void init() {

        Config config = new Config();
        server1 = Hazelcast.newHazelcastInstance(config);
        server2 = Hazelcast.newHazelcastInstance(config);
        server3 = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setExecutorPoolSize(executorPoolSize);
        client = HazelcastClient.newHazelcastClient(clientConfig);

        final IMap<Object, Object> map = client.getMap("map");

        //default to add 1 listener to use the 1 executor thread
        entryCounterID = map.addEntryListener(new EntryCounter(), true);
    }

    @After
    public void destroy() {
        client.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }


    @Test
    public void removeListenerAfterAsyncOpp() throws InterruptedException {

        final IMap<Object, Object> map = client.getMap("map");

        for(int i=0; i<1000; i++){
            map.putAsync(i, i);
            if(i==500){
                map.removeEntryListener(entryCounterID);
            }
        }

        assertTrueEventually(new Runnable(){
            public void run(){
                assertEquals(1000, map.size());
            }
        });

    }



    @Test
    @Ignore("Issue #1391")
    public void moreEntryListenersThanExecutorThreads() throws InterruptedException {

        EntryCounter counter = new EntryCounter();

        final IMap<Object, Object> map = client.getMap("map");
        map.addEntryListener(counter, true);//adding 2nd EntryListener

        for(int i=0; i<1000; i++){
            map.putAsync(i, i);
        }

        assertTrueEventually(new Runnable(){
            public void run(){
                assertEquals(1000, map.size());
            }
        });

        assertEquals(1000, counter.count.get());
    }


    @Test
    @Ignore("Issue #1391")
    public void entryListenerWithAsyncMapOps() throws InterruptedException, ExecutionException {

        final IMap<Object, Object> map = client.getMap("map");

        for(int i=0; i<1000; i++){
            map.putAsync(i,i);
        }

        assertTrueEventually(new Runnable(){
            public void run(){
                assertEquals(1000, map.size());
            }
        });

        for(int i=0; i<1000; i++){
            Future f= map.getAsync(i);
            assertEquals(i, f.get());
        }

        for(int i=0; i<1000; i++){
            map.removeAsync(i);
        }

        assertTrueEventually(new Runnable(){
            public void run(){
                assertEquals(0, map.size());
            }
        });
    }


    @Test
    @Ignore("Issue #1391")
    public void entryListenerWithAsyncQueueOps() throws InterruptedException {

        ItemCounter counter = new ItemCounter();

        final IQueue<Object> q = client.getQueue("Q");
        q.addItemListener(counter, true);


        for(int i=0; i<1000; i++){
            q.offer(i);
        }

        assertTrueEventually(new Runnable(){
            public void run(){
                assertEquals(1000, q.size());
            }
        });

        assertEquals(1000, counter.count.get());

        for(int i=0; i<1000; i++){
            Object o = q.poll();

            assertEquals(i, o);
            q.remove(o);
        }

        assertTrueEventually(new Runnable(){
            public void run(){
                assertEquals(0, q.size());
            }
        });

        assertEquals(0, counter.count.get());
    }



    @Test
    @Ignore("Issue #1391")
    public void entryListenerWithAsyncTopicOps() throws InterruptedException {

        final ITopic<Object> t = client.getTopic("stuff");

        final MsgCounter counter = new MsgCounter();
        t.addMessageListener(counter);

        for(int i=0; i<1000; i++)
            t.publish(i);

        assertTrueEventually(new Runnable(){
            public void run(){
                assertEquals(1000, counter.count.get());
            }
        });

    }





    @Test
    @Ignore("Issue #1391")
    public void entryListenerWithExecutorService() throws ExecutionException, InterruptedException {

        final IMap<Object, Object> map = client.getMap("map");

        IExecutorService executor =  client.getExecutorService("simple");

        final Future<String> f = executor.submit(new BasicTestTask());

        assertTrueEventually(new Runnable(){
            public void run(){
                try {
                    assertEquals(BasicTestTask.RESULT, f.get());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        });
    }


    public static class BasicTestTask implements Callable<String>, Serializable {

        public static String RESULT = "Task completed";

        public String call() throws Exception {
            return RESULT;
        }
    }

    public class MsgCounter implements MessageListener{

        public AtomicInteger count = new AtomicInteger(0);

        public void onMessage(Message message) {
            count.getAndIncrement();
        }
    }


    public class ItemCounter  implements ItemListener {

        public AtomicInteger count = new AtomicInteger(0);

        public void itemAdded(ItemEvent itemEvent) {
            count.getAndIncrement();
        }

        public void itemRemoved(ItemEvent item) {
            count.getAndDecrement();
        }
    };


    public class EntryCounter implements EntryListener {

        public AtomicInteger count = new AtomicInteger(0);

        public void entryAdded(EntryEvent event) {
            count.getAndIncrement();
        }

        public void entryRemoved(EntryEvent event) {
            count.getAndDecrement();
        }

        public void entryUpdated(EntryEvent event) {
        }

        public void entryEvicted(EntryEvent event) {
        }

    }

    public static void assertTrueEventually(Runnable task) {
        AssertionError error = null;
        for (int k = 0; k < 60; k++) {
            try {
                task.run();
                return;
            } catch (AssertionError e) {
                error = e;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        throw error;
    }
}
