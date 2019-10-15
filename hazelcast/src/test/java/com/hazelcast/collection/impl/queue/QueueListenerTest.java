/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueListenerTest extends HazelcastTestSupport {

    private static final String QUEUE_NAME = "Q";
    private static final int TOTAL_QUEUE_PUT = 2000;

    private final CountdownItemListener countdownItemListener = new CountdownItemListener(TOTAL_QUEUE_PUT, 0);
    private final ItemListenerConfig itemListenerConfig = new ItemListenerConfig();
    private final QueueConfig queueConfig = new QueueConfig();
    private final Config config = new Config();

    @Test
    public void testListener_withEvictionViaTTL() throws Exception {
        Config config = new Config();
        config.getQueueConfig("queueWithTTL").setEmptyQueueTtl(0);
        HazelcastInstance hz = createHazelcastInstance(config);

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

        IQueue<Object> queue = hz.getQueue("queueWithTTL");
        queue.offer("item");
        queue.poll();

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testConfigListenerRegistration() throws Exception {
        Config config = new Config();
        String name = "queue";
        QueueConfig queueConfig = config.getQueueConfig(name);
        CountdownItemListener listener = new CountdownItemListener(1, 1);
        ItemListenerConfig itemListenerConfig = new ItemListenerConfig(listener, true);
        queueConfig.addItemListenerConfig(itemListenerConfig);
        HazelcastInstance instance = createHazelcastInstance(config);

        IQueue<String> queue = instance.getQueue(name);
        queue.offer("item");
        queue.poll();

        assertTrue(listener.added.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testItemListener_addedToQueueConfig_Issue366() throws Exception {
        itemListenerConfig.setImplementation(countdownItemListener);
        itemListenerConfig.setIncludeValue(true);

        queueConfig.setName(QUEUE_NAME);
        queueConfig.addItemListenerConfig(itemListenerConfig);
        config.addQueueConfig(queueConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        IQueue<Integer> queue = instance.getQueue(QUEUE_NAME);
        for (int i = 0; i < TOTAL_QUEUE_PUT / 2; i++) {
            queue.put(i);
        }
        factory.newHazelcastInstance(config);
        for (int i = 0; i < TOTAL_QUEUE_PUT / 4; i++) {
            queue.put(i);
        }
        assertOpenEventually(countdownItemListener.added);
    }

    @Test
    public void testListeners() throws Exception {
        String queueName = randomString();
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(queueName);

        TestItemListener listener = new TestItemListener(TOTAL_QUEUE_PUT);
        UUID listenerId = queue.addItemListener(listener, true);

        for (int i = 0; i < TOTAL_QUEUE_PUT / 2; i++) {
            queue.offer("item-" + i);
        }
        for (int i = 0; i < TOTAL_QUEUE_PUT / 2; i++) {
            queue.poll();
        }
        assertTrue(listener.latch.await(5, TimeUnit.SECONDS));

        queue.removeItemListener(listenerId);
        queue.offer("item-a");
        queue.poll();
        assertTrue(listener.notCalled.get());
    }

    private static class TestItemListener implements ItemListener<String> {

        CountDownLatch latch;
        AtomicBoolean notCalled;
        int offer;
        int poll;

        TestItemListener(int count) {
            this.latch = new CountDownLatch(count);
            this.notCalled = new AtomicBoolean(true);
        }

        @Override
        public void itemAdded(ItemEvent item) {
            if (item.getItem().equals("item-" + offer++)) {
                latch.countDown();
            } else {
                notCalled.set(false);
            }
        }

        @Override
        public void itemRemoved(ItemEvent item) {
            if (item.getItem().equals("item-" + poll++)) {
                latch.countDown();
            } else {
                notCalled.set(false);
            }
        }
    }

    private static class CountdownItemListener implements ItemListener {

        public CountDownLatch added;
        public CountDownLatch removed;

        CountdownItemListener(int expectedAdded, int expectedRemoved) {
            added = new CountDownLatch(expectedAdded);
            removed = new CountDownLatch(expectedRemoved);
        }

        @Override
        public void itemAdded(ItemEvent itemEvent) {
            added.countDown();
        }

        @Override
        public void itemRemoved(ItemEvent itemEvent) {
            removed.countDown();
        }
    }
}
