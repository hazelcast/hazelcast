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

import com.hazelcast.config.Config;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.Assert.assertTrue;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class QueueListenerTest extends HazelcastTestSupport {

    private String Qname = "Q";
    private int totalQput = 2000;

    private Config cfg = new Config();
    private SimpleItemListener simpleItemListener = new SimpleItemListener(totalQput);
    private ItemListenerConfig itemListenerConfig = new ItemListenerConfig();
    private QueueConfig qConfig = new QueueConfig();

    @Test
    public void testItemListener_addedToQueueConfig_Issue366() throws InterruptedException {

        itemListenerConfig.setImplementation(simpleItemListener);
        itemListenerConfig.setIncludeValue(true);

        qConfig.setName(Qname);
        qConfig.addItemListenerConfig(itemListenerConfig);
        cfg.addQueueConfig(qConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance node1 = factory.newHazelcastInstance(cfg);
        IQueue queue = node1.getQueue(Qname);
        for (int i = 0; i < totalQput / 2; i++) {
            queue.put(i);
        }

        HazelcastInstance node2 = factory.newHazelcastInstance(cfg);

        for (int i = 0; i < totalQput / 4; i++) {
            queue.put(i);
        }

        assertTrue(simpleItemListener.added.await(3, TimeUnit.SECONDS));
    }

    @Test
    public void testListeners() throws InterruptedException {
        final String name = randomString();
        HazelcastInstance instance = createHazelcastInstance();
        final CountDownLatch latch = new CountDownLatch(20);
        final AtomicBoolean notCalled = new AtomicBoolean(true);

        IQueue queue = instance.getQueue(name);
        TestItemListener listener = new TestItemListener(latch, notCalled);
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
        assertTrue(notCalled.get());
    }

    private static class TestItemListener implements ItemListener {
        int offer;
        int poll;
        CountDownLatch latch;
        AtomicBoolean notCalled;

        TestItemListener(CountDownLatch latch, AtomicBoolean notCalled) {
            this.latch = latch;
            this.notCalled = new AtomicBoolean(true);
        }

        public void itemAdded(ItemEvent item) {
            if (item.getItem().equals("item" + offer++)) {
                latch.countDown();
            } else {
                notCalled.set(false);
            }
        }

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

        public void itemAdded(ItemEvent itemEvent) {
            added.countDown();
        }

        public void itemRemoved(ItemEvent itemEvent) {
        }
    }
}
