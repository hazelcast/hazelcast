/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.queue;

import com.hazelcast.client.ClientTestSupport;
import com.hazelcast.client.SimpleClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.queue.impl.client.AddAllRequest;
import com.hazelcast.queue.impl.client.AddListenerRequest;
import com.hazelcast.queue.impl.client.ClearRequest;
import com.hazelcast.queue.impl.client.CompareAndRemoveRequest;
import com.hazelcast.queue.impl.client.ContainsRequest;
import com.hazelcast.queue.impl.client.DrainRequest;
import com.hazelcast.queue.impl.client.IsEmptyRequest;
import com.hazelcast.queue.impl.client.IteratorRequest;
import com.hazelcast.queue.impl.client.OfferRequest;
import com.hazelcast.queue.impl.client.PeekRequest;
import com.hazelcast.queue.impl.client.PollRequest;
import com.hazelcast.queue.impl.client.RemainingCapacityRequest;
import com.hazelcast.queue.impl.client.RemoveListenerRequest;
import com.hazelcast.queue.impl.client.RemoveRequest;
import com.hazelcast.queue.impl.client.SizeRequest;
import com.hazelcast.spi.impl.PortableCollection;
import com.hazelcast.spi.impl.PortableItemEvent;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class QueueClientRequestTest extends ClientTestSupport {

    static final String queueName = "test";
    static final SerializationService ss = new DefaultSerializationServiceBuilder().build();

    protected Config createConfig() {
        Config config = new Config();
        QueueConfig queueConfig = config.getQueueConfig(queueName);
        queueConfig.setMaxSize(6);
        return config;
    }

    private IQueue<Object> getQueue() {
        return getInstance().getQueue(queueName);
    }

    @Test
    public void testAddAll() throws IOException {
        List<Data> list = new ArrayList<Data>();
        list.add(ss.toData("item1"));
        list.add(ss.toData("item2"));
        list.add(ss.toData("item3"));
        list.add(ss.toData("item4"));
        final SimpleClient client = getClient();
        client.send(new AddAllRequest(queueName, list));
        Object result = client.receive();
        assertTrue((Boolean) result);
        int size = getQueue().size();
        assertEquals(size, list.size());
    }

    @Test
    public void testAddListener() throws IOException {
        final SimpleClient client = getClient();
        client.send(new AddListenerRequest(queueName, true));
        client.receive();
        getQueue().offer("item");

        PortableItemEvent result = (PortableItemEvent) client.receive();
        assertEquals("item", ss.toObject(result.getItem()));
        assertEquals(ItemEventType.ADDED, result.getEventType());
    }

    @Test
    public void testClear() throws Exception {
        IQueue q = getQueue();
        q.offer("item1");
        q.offer("item2");
        q.offer("item3");

        final SimpleClient client = getClient();
        client.send(new ClearRequest(queueName));
        Object result = client.receive();
        assertTrue((Boolean) result);
        assertEquals(0, q.size());
    }

    @Test
    public void testCompareAndRemove() throws IOException {
        IQueue q = getQueue();
        q.offer("item1");
        q.offer("item2");
        q.offer("item3");
        q.offer("item4");
        q.offer("item5");

        List<Data> list = new ArrayList<Data>();
        list.add(ss.toData("item1"));
        list.add(ss.toData("item2"));

        final SimpleClient client = getClient();
        client.send(new CompareAndRemoveRequest(queueName, list, true));
        Boolean result = (Boolean) client.receive();
        assertTrue(result);
        assertEquals(2, q.size());

        client.send(new CompareAndRemoveRequest(queueName, list, false));
        result = (Boolean) client.receive();
        assertTrue(result);
        assertEquals(0, q.size());
    }

    @Test
    public void testContains() throws IOException {
        IQueue q = getQueue();
        q.offer("item1");
        q.offer("item2");
        q.offer("item3");
        q.offer("item4");
        q.offer("item5");

        List<Data> list = new ArrayList<Data>();
        list.add(ss.toData("item1"));
        list.add(ss.toData("item2"));

        final SimpleClient client = getClient();
        client.send(new ContainsRequest(queueName, list));
        Boolean result = (Boolean) client.receive();
        assertTrue(result);

        list.add(ss.toData("item0"));

        client.send(new ContainsRequest(queueName, list));
        result = (Boolean) client.receive();
        assertFalse(result);
    }

    @Test
    public void testDrain() throws IOException {
        IQueue q = getQueue();
        q.offer("item1");
        q.offer("item2");
        q.offer("item3");
        q.offer("item4");
        q.offer("item5");

        final SimpleClient client = getClient();
        client.send(new DrainRequest(queueName, 1));
        PortableCollection result = (PortableCollection) client.receive();
        Collection<Data> coll = result.getCollection();
        assertEquals(1, coll.size());
        assertEquals("item1", ss.toObject(coll.iterator().next()));
        assertEquals(4, q.size());
    }

    @Test
    public void testIterator() throws IOException {
        IQueue q = getQueue();
        q.offer("item1");
        q.offer("item2");
        q.offer("item3");
        q.offer("item4");
        q.offer("item5");

        final SimpleClient client = getClient();
        client.send(new IteratorRequest(queueName));
        PortableCollection result = (PortableCollection) client.receive();
        Collection<Data> coll = result.getCollection();
        int i = 1;
        for (Data data : coll) {
            assertEquals("item" + i, ss.toObject(data));
            i++;
        }
    }

    @Test
    public void testOffer() throws IOException, InterruptedException {
        final IQueue q = getQueue();

        final SimpleClient client = getClient();
        client.send(new OfferRequest(queueName, ss.toData("item1")));
        Object result = client.receive();
        assertTrue((Boolean) result);
        Object item = q.peek();
        assertEquals(item, "item1");

        q.offer("item2");
        q.offer("item3");
        q.offer("item4");
        q.offer("item5");
        q.offer("item6");

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    latch.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                q.poll();
            }
        }.start();

        client.send(new OfferRequest(queueName, 500, ss.toData("item7")));
        result = client.receive();
        assertFalse((Boolean) result);

        client.send(new OfferRequest(queueName, 10 * 1000, ss.toData("item7")));
        Thread.sleep(1000);
        latch.countDown();
        result = client.receive();
        assertTrue((Boolean) result);
    }

    @Test
    public void testPeek() throws IOException {
        IQueue q = getQueue();

        final SimpleClient client = getClient();
        client.send(new PeekRequest(queueName));
        Object result = client.receive();
        assertNull(result);

        q.offer("item1");
        client.send(new PeekRequest(queueName));
        result = client.receive();
        assertEquals("item1", result);
        assertEquals(1, q.size());
    }

    @Test
    public void testPoll() throws IOException {
        final IQueue q = getQueue();
        final SimpleClient client = getClient();
        client.send(new PollRequest(queueName));
        Object result = client.receive();
        assertNull(result);

        q.offer("item1");
        client.send(new PollRequest(queueName));
        result = client.receive();
        assertEquals("item1", result);
        assertEquals(0, q.size());

        new Thread() {
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                q.offer("item2");
            }
        }.start();
        client.send(new PollRequest(queueName, 10 * 1000));
        result = client.receive();
        assertEquals("item2", result);
        assertEquals(0, q.size());
    }

    @Test
    public void testRemove() throws IOException {

        final IQueue q = getQueue();
        q.offer("item1");
        q.offer("item2");
        q.offer("item3");

        final SimpleClient client = getClient();
        client.send(new RemoveRequest(queueName, ss.toData("item2")));
        Boolean result = (Boolean) client.receive();
        assertTrue(result);
        assertEquals(2, q.size());

        client.send(new RemoveRequest(queueName, ss.toData("item2")));
        result = (Boolean) client.receive();
        assertFalse(result);
        assertEquals(2, q.size());
    }

    @Test
    public void testSize() throws IOException {
        final IQueue q = getQueue();
        q.offer("item1");
        q.offer("item2");
        q.offer("item3");

        final SimpleClient client = getClient();
        client.send(new SizeRequest(queueName));
        int result = (Integer) client.receive();
        assertEquals(result, q.size());
    }

    @Test
    public void testIsEmpty() throws IOException {
        final IQueue queue = getQueue();

        final SimpleClient client = getClient();
        client.send(new IsEmptyRequest(queueName));
        boolean result = (Boolean) client.receive();
        assertEquals(0, queue.size());
        assertTrue(result);
    }

    @Test
    public void testRemainingCapacity() throws IOException {
        createConfig();
        IQueue queue = getQueue();
        queue.offer("item1");
        queue.offer("item2");

        final SimpleClient client = getClient();
        client.send(new RemainingCapacityRequest(queueName));
        int result = (Integer) client.receive();
        assertEquals(4, result);
    }

    @Test
    public void testRemoveListener() throws IOException {
        IQueue queue = getQueue();
        final AtomicInteger addCounter = new AtomicInteger(0);
        TestListener listener = new TestListener(addCounter);
        String registrationId = queue.addItemListener(listener, false);
        queue.add("item1");
        queue.add("item2");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, addCounter.get());
            }
        });

        final SimpleClient client = getClient();
        client.send(new RemoveListenerRequest(queueName, registrationId));
        boolean result = (Boolean) client.receive();

        assertTrue(result);
        queue.offer("item3");
        assertSizeEventually(3, queue);
        assertNotEquals(3, addCounter.get());
    }

    private static class TestListener implements ItemListener {
        private final AtomicInteger addCounter;

        public TestListener(AtomicInteger addCounter) {
            this.addCounter = addCounter;
        }

        public void itemAdded(ItemEvent item) {
            addCounter.getAndIncrement();
        }

        public void itemRemoved(ItemEvent item) {
        }
    }


}
