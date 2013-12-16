package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import org.junit.*;

import java.io.Serializable;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Created by danny on 12/16/13.
 */
public class ClientIOTest {

    static HazelcastInstance server1;
    static HazelcastInstance server2;
    static HazelcastInstance server3;
    static HazelcastInstance client;

    static final int executorPoolSize=1;

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

        map.addEntryListener(new MyListner(), true);
    }

    @After
    public void destroy() {
        client.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Test
    public void moreEntryListenersThanExecutorThreads() throws InterruptedException {

        final IMap<Object, Object> map = client.getMap("map");

        map.addEntryListener(new MyListner(), true);

        //if no exception is thrown then proabley want to assert a count on the entery listener so that both entery listeners get the event.

        for(int i=0; i<1; i++){
            map.put(i, i);
        }



        assertTrueEventually(new Runnable(){
            public void run(){
                assertEquals(1, map.size());
            }
        });
    }

    @Test
    public void entryListenerWithMapRemoveAsync() throws InterruptedException {

        final IMap<Object, Object> map = client.getMap("map");

        for(int i=0; i<1000; i++){
            map.put(i, i);
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
    public void entryListenerWithAsyncQueueOps() throws InterruptedException {

        final IQueue<Object> q = client.getQueue("Q");

        q.addItemListener(new ItemListener() {

            public void itemAdded(ItemEvent itemEvent) {
                //latch.countDown();
            }

            public void itemRemoved(ItemEvent item) {
            }
        }, true);


        for(int i=0; i<1000; i++){
            q.offer(i);
        }

        assertTrueEventually(new Runnable(){
            public void run(){
                assertEquals(1000, q.size());
            }
        });

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
    }



    @Test
    public void entryListenerWithAsyncTopicOps() throws InterruptedException {

        final ITopic<Object> t = client.getTopic("stuff");

        t.addMessageListener( new MessageListener() {
            public void onMessage(Message message) {
                System.out.println("hi");
            }
        });

        for(int i=0; i<1000; i++)
            t.publish(i);

    }





    @Test
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


    public class MyListner implements ItemListener, EntryListener {

        public void entryAdded(EntryEvent event) {
        }

        public void entryRemoved(EntryEvent event) {
        }

        public void entryUpdated(EntryEvent event) {
        }

        public void entryEvicted(EntryEvent event) {
        }

        public void itemAdded(ItemEvent item) {
        }

        public void itemRemoved(ItemEvent item) {
        }
    }

/*
    abstract public class GatedThread extends Thread{
        private final CyclicBarrier gate;

        public GatedThread(CyclicBarrier gate){
            this.gate = gate;
        }

        public void run(){
            try {
                gate.await();
                go();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }

        abstract public void go();
    }

    private void startGatedThread(GatedThread t){
        t.start();
    }
*/

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
