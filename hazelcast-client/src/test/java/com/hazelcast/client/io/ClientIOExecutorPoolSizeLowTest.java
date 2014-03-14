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
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientIOExecutorPoolSizeLowTest {

    static final int COUNT = 1000;
    static final int TIMEOUT = 60 * 1000;
    static HazelcastInstance server1;
    static HazelcastInstance server2;
    static HazelcastInstance client;
    static IMap map;

    static final int executorPoolSize = 1;

    static String entryCounterID;

    @Before
    public void init() {
        Config config = new Config();
        server1 = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setExecutorPoolSize(executorPoolSize);
        client = HazelcastClient.newHazelcastClient(clientConfig);

        server2 = Hazelcast.newHazelcastInstance(config);

        map = client.getMap("map");
        entryCounterID = map.addEntryListener(new EntryCounter(), true);
    }

    @After
    public void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(timeout = TIMEOUT)
    public void entryListenerWithMapOps_WithNodeTerminate() throws InterruptedException, ExecutionException {
        for (int i = 0; i < COUNT; i++) {
            map.put(i, i);
            if (i == COUNT / 2) {
                server2.getLifecycleService().terminate();
            }
        }
        assertEquals(COUNT, map.size());
    }

    @Test(timeout = TIMEOUT)
    public void entryListenerWithMapOps_WithClientConnectedNodeTerminate() throws InterruptedException, ExecutionException {
        for (int i = 0; i < COUNT; i++) {
            map.put(i, i);
            if (i == COUNT / 2) {
                server1.getLifecycleService().terminate();
            }
        }
        assertEquals(COUNT, map.size());
    }

    @Test(timeout = TIMEOUT)
    public void entryListenerWithMapAsyncOps_WithNodeTerminate() throws InterruptedException, ExecutionException {
        final ArrayList<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < COUNT; i++) {
            final Future<Object> f = map.putAsync(i, i);
            futures.add(f);
            if (i == COUNT / 2) {
                server2.getLifecycleService().terminate();
            }
        }
        int total = 0;
        for (Future f : futures) {
            try {
                f.get();
                total++;
            } catch (Exception ignored) {
            }
        }
        assertTrue(map.size() >= total);
    }


    @Test(timeout = TIMEOUT)
    public void entryListenerWithAsyncMapOps_WithNodeTerminate() throws InterruptedException, ExecutionException {
        for (int i = 0; i < COUNT; i++) {
            map.putAsync(i, i);
        }
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(COUNT, map.size());
            }
        });
        for (int i = 0; i < COUNT; i++) {
            Future f = map.getAsync(i);
            assertEquals(i, f.get());
            if (i == 500) {
                server2.getLifecycleService().terminate();
            }
        }
    }

    @Test
    public void removeListenerAfterAsyncOpp() throws InterruptedException {
        for (int i = 0; i < COUNT; i++) {
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
        map.addEntryListener(counter, true);//adding 2nd EntryListener
        for (int i = 0; i < COUNT; i++) {
            map.putAsync(i, i);
        }
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(COUNT, map.size());
            }
        });
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(COUNT, counter.count.get());
            }
        });
    }


    @Test
    public void entryListenerWithAsyncMapOps() throws InterruptedException, ExecutionException {
        for (int i = 0; i < COUNT; i++) {
            map.putAsync(i, i);
        }
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(1000, map.size());
            }
        });
        for (int i = 0; i < COUNT; i++) {
            Future f = map.getAsync(i);
            assertEquals(i, f.get());
        }
        for (int i = 0; i < COUNT; i++) {
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
        final ITopic<Object> topic = client.getTopic(randomString());
        final MsgCounter counter = new MsgCounter();
        topic.addMessageListener(counter);
        for (int i = 0; i < COUNT; i++) {
            topic.publish(i);
        }
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(COUNT, counter.count.get());
            }
        });
    }


    @Test
    public void entryListenerWithExecutorService() throws ExecutionException, InterruptedException {
        IExecutorService executor = client.getExecutorService(randomString());
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
