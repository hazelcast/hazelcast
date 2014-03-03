package com.hazelcast.client.io;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
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

    static final int executorPoolSize = 1;

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
        client.shutdown();
        Hazelcast.shutdownAll();
    }


    @Test(timeout = 1000 * 60)
    @Category(ProblematicTest.class)
    //we destroy all the nodes and clients and then init them again but this time with the client init-ed between the nodes.
    //the test hangs at client.getMap("map");  again only if the pool size is 1
    public void initNodeAndClientOrderTest() throws InterruptedException, ExecutionException {

        destroy();

        server1 = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setExecutorPoolSize(executorPoolSize);
        client = HazelcastClient.newHazelcastClient(clientConfig);

        server2 = Hazelcast.newHazelcastInstance();
        server3 = Hazelcast.newHazelcastInstance();

        final IMap<Object, Object> map = client.getMap("map");

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(1000, map.size());
            }
        });
    }

    @Test(timeout = 1000 * 60)
    @Category(ProblematicTest.class)//the client hangs which executeOnKeyOwner pool size of 1
    public void entryListenerWithMapOps_WithNodeTerminate() throws InterruptedException, ExecutionException {

        final IMap<Object, Object> map = client.getMap("map");

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
            if (i == 500) {
                server2.getLifecycleService().terminate();
            }
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(1000, map.size());
            }
        });
    }

    @Test(timeout = 1000 * 60)
    @Category(ProblematicTest.class)
//the client Throws com.hazelcast.spi.exception.TargetDisconnectedException: Target[Address[127.0.0.1]:5702] disconnected. client seems to connect to server1 every time ?
    public void entryListenerWithMapOps_WithClientConnectedNodeTerminate() throws InterruptedException, ExecutionException {

        final IMap<Object, Object> map = client.getMap("map");

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
            if (i == 500) {
                server1.getLifecycleService().terminate();
            }
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(1000, map.size());
            }
        });
    }

    @Test(timeout = 1000 * 60)
    @Category(ProblematicTest.class)//the client hangs which executeOnKeyOwner pool size of 1
    public void entryListenerWithMapAsyncOps_WithNodeTerminate() throws InterruptedException, ExecutionException {

        final IMap<Object, Object> map = client.getMap("map");

        for (int i = 0; i < 1000; i++) {
            map.putAsync(i, i);
            if (i == 500) {
                server2.getLifecycleService().terminate();
            }
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(1000, map.size());
            }
        });
    }


    @Test(timeout = 1000 * 60)
    @Category(ProblematicTest.class)//the client hangs which executeOnKeyOwner pool size of 1
    public void entryListenerWithAsyncMapOps_WithNodeTerminate() throws InterruptedException, ExecutionException {

        final IMap<Object, Object> map = client.getMap("map");

        for (int i = 0; i < 1000; i++) {
            map.putAsync(i, i);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(1000, map.size());
            }
        });


        for (int i = 0; i < 1000; i++) {

            Future f = map.getAsync(i);
            assertEquals(i, f.get());

            if (i == 500) {
                server2.getLifecycleService().terminate();
            }
        }
    }

    @Test
    public void removeListenerAfterAsyncOpp() throws InterruptedException {

        final IMap<Object, Object> map = client.getMap("map");

        for (int i = 0; i < 1000; i++) {
            map.putAsync(i, i);
            if (i == 500) {
                map.removeEntryListener(entryCounterID);
            }
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(1000, map.size());
            }
        });

    }


    @Test
    public void moreEntryListenersThanExecutorThreads() throws InterruptedException {

        final EntryCounter counter = new EntryCounter();

        final IMap<Object, Object> map = client.getMap("map");
        map.addEntryListener(counter, true);//adding 2nd EntryListener

        for (int i = 0; i < 1000; i++) {
            map.putAsync(i, i);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(1000, map.size());
            }
        });

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(1000, counter.count.get());
            }
        });

    }


    @Test
    public void entryListenerWithAsyncMapOps() throws InterruptedException, ExecutionException {

        final IMap<Object, Object> map = client.getMap("map");

        for (int i = 0; i < 1000; i++) {
            map.putAsync(i, i);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(1000, map.size());
            }
        });

        for (int i = 0; i < 1000; i++) {
            Future f = map.getAsync(i);
            assertEquals(i, f.get());
        }

        for (int i = 0; i < 1000; i++) {
            map.removeAsync(i);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(0, map.size());
            }
        });
    }

    @Test
    public void entryListenerWithAsyncTopicOps() throws InterruptedException {

        final ITopic<Object> t = client.getTopic("stuff");

        final MsgCounter counter = new MsgCounter();
        t.addMessageListener(counter);

        for (int i = 0; i < 1000; i++)
            t.publish(i);

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(1000, counter.count.get());
            }
        });

    }


    @Test
    public void entryListenerWithExecutorService() throws ExecutionException, InterruptedException {

        IExecutorService executor = client.getExecutorService("simple");

        final Future<String> f = executor.submit(new BasicTestTask());

        assertTrueEventually(new AssertTask() {
            public void run() {
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

    public class MsgCounter implements MessageListener {

        public AtomicInteger count = new AtomicInteger(0);

        public void onMessage(Message message) {
            count.getAndIncrement();
        }
    }


    public class ItemCounter implements ItemListener {

        public AtomicInteger count = new AtomicInteger(0);

        public void itemAdded(ItemEvent itemEvent) {
            count.getAndIncrement();
        }

        public void itemRemoved(ItemEvent item) {
            count.getAndDecrement();
        }
    }

    ;


    public class EntryCounter extends EntryAdapter {

        public AtomicInteger count = new AtomicInteger(0);

        public void entryAdded(EntryEvent event) {
            count.getAndIncrement();
        }

        public void entryRemoved(EntryEvent event) {
            count.getAndDecrement();
        }
    }
}
