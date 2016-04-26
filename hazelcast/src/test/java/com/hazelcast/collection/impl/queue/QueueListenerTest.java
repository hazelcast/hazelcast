/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.config.Config;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueueListenerTest extends HazelcastTestSupport {

    private static final String Q_NAME = "Q";
    private static final int TOTAL_QPUT = 2000;

    private final Config cfg = new Config();
    private final SimpleItemListener simpleItemListener = new SimpleItemListener(TOTAL_QPUT);
    private final ItemListenerConfig itemListenerConfig = new ItemListenerConfig();
    private final QueueConfig qConfig = new QueueConfig();

    //TODO: Don't add a number to a test to test a diferent case. I guess it was already in the system.
    @Test
    public void testQueueEviction2() throws Exception {
        final Config config = new Config();
        config.getQueueConfig("q2").setEmptyQueueTtl(0);
        final HazelcastInstance hz = createHazelcastInstance(config);
        final CountDownLatch latch = new CountDownLatch(2);
        hz.addDistributedObjectListener(new DistributedObjectListener() {
            @Override
            public void distributedObjectCreated(DistributedObjectEvent event) {
                latch.countDown();
            }

            @Override
            public void distributedObjectDestroyed(DistributedObjectEvent event) {
                latch.countDown();
            }
        });

        final IQueue<Object> q = hz.getQueue("q2");
        q.offer("item");
        q.poll();

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testConfigListenerRegistration() throws InterruptedException {
        final Config config = new Config();
        final String name = "queue";
        final QueueConfig queueConfig = config.getQueueConfig(name);
        final DummyListener dummyListener = new DummyListener();
        final ItemListenerConfig itemListenerConfig = new ItemListenerConfig(dummyListener, true);
        queueConfig.addItemListenerConfig(itemListenerConfig);
        final HazelcastInstance instance = createHazelcastInstance(config);
        final IQueue<String> queue = instance.getQueue(name);
        queue.offer("item");
        queue.poll();
        assertTrue(dummyListener.latch.await(10, TimeUnit.SECONDS));
    }

    private static class DummyListener implements ItemListener, Serializable {
        final CountDownLatch latch = new CountDownLatch(2);

        DummyListener() {
        }

        @Override
        public void itemAdded(ItemEvent item) {
            latch.countDown();
        }

        @Override
        public void itemRemoved(ItemEvent item) {
            latch.countDown();
        }
    }

    @Test
    public void testItemListener_addedToQueueConfig_Issue366() throws InterruptedException {
        itemListenerConfig.setImplementation(simpleItemListener);
        itemListenerConfig.setIncludeValue(true);

        qConfig.setName(Q_NAME);
        qConfig.addItemListenerConfig(itemListenerConfig);
        cfg.addQueueConfig(qConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance node1 = factory.newHazelcastInstance(cfg);
        IQueue<Integer> queue = node1.getQueue(Q_NAME);
        for (int i = 0; i < TOTAL_QPUT / 2; i++) {
            queue.put(i);
        }
        HazelcastInstance node2 = factory.newHazelcastInstance(cfg);
        for (int i = 0; i < TOTAL_QPUT / 4; i++) {
            queue.put(i);
        }
        assertTrue(simpleItemListener.added.await(3, TimeUnit.SECONDS));
    }

    @Test
    public void testListeners() throws InterruptedException {
        final String name = randomString();
        HazelcastInstance instance = createHazelcastInstance();
        final CountDownLatch latch = new CountDownLatch(20);

        IQueue<String> queue = instance.getQueue(name);
        TestItemListener listener = new TestItemListener(latch);
        final String id = queue.addItemListener(listener, true);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        for (int i = 0; i < 10; i++) {
            queue.poll();
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        queue.removeItemListener(id);
        queue.offer("item-a");
        queue.poll();
        assertTrue(listener.notCalled.get());
    }

    private static class TestItemListener implements ItemListener<String> {
        int offer;
        int poll;
        CountDownLatch latch;
        AtomicBoolean notCalled;

        TestItemListener(CountDownLatch latch) {
            this.latch = latch;
            this.notCalled = new AtomicBoolean(true);
        }

        @Override
        public void itemAdded(ItemEvent item) {
            if (item.getItem().equals("item" + offer++)) {
                latch.countDown();
            } else {
                notCalled.set(false);
            }
        }

        @Override
        public void itemRemoved(ItemEvent item) {
            if (item.getItem().equals("item" + poll++)) {
                latch.countDown();
            } else {
                notCalled.set(false);
            }
        }
    }

    private static class SimpleItemListener implements ItemListener {
        public CountDownLatch added;

        public SimpleItemListener(int CountDown) {
            added = new CountDownLatch(CountDown);
        }

        @Override
        public void itemAdded(ItemEvent itemEvent) {
            added.countDown();
        }

        @Override
        public void itemRemoved(ItemEvent itemEvent) {
        }
    }
}
