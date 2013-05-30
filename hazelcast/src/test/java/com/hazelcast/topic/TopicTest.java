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

package com.hazelcast.topic;

import com.hazelcast.config.Config;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;


@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class TopicTest extends HazelcastTestSupport {

    @Test
    public void testTopicPublishingMember() {
        final Config config = new Config();
        config.getTopicConfig("default").setGlobalOrderingEnabled(true);
        final int k = 3;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(config);

        final CountDownLatch mainLatch = new CountDownLatch(k);
        final AtomicInteger count = new AtomicInteger(0);
        final AtomicInteger count1 = new AtomicInteger(0);
        final AtomicInteger count2 = new AtomicInteger(0);
        final String name = "testTopicPublishingMember";

        for (int i = 0; i < k; i++) {
            final HazelcastInstance instance = instances[i];
            new Thread(new Runnable() {
                public void run() {
                    ITopic<Long> topic = instance.getTopic(name);
                    topic.addMessageListener(new MessageListener<Long>() {
                        public void onMessage(Message<Long> message) {
                            if (message.getPublishingMember().equals(instance.getCluster().getLocalMember()))
                                count.incrementAndGet();
                            if (message.getPublishingMember().equals(message.getMessageObject()))
                                count1.incrementAndGet();
                            if (message.getPublishingMember().localMember())
                                count2.incrementAndGet();
                        }
                    });
                    mainLatch.countDown();
                }
            }).start();

        }
        try {
            mainLatch.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < k; i++) {
            final HazelcastInstance instance = instances[i];
            instance.getTopic(name).publish(instance.getCluster().getLocalMember());
        }
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertEquals(k, count.get());
        Assert.assertEquals(k * k, count1.get());
        Assert.assertEquals(k, count2.get());
    }

    @Test
    public void testTopicTotalOrder() throws Exception {
        final Config config = new Config();
        config.getTopicConfig("default").setGlobalOrderingEnabled(true);

        final int k = 4;

        final Map<Long, String> stringMap = new HashMap<Long, String>();
        final CountDownLatch countDownLatch = new CountDownLatch(k);
        final CountDownLatch mainLatch = new CountDownLatch(k);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(config);

        Assert.assertEquals(true, instances[0].getConfig().getTopicConfig("default").isGlobalOrderingEnabled());

        for (int i = 0; i < k; i++) {
            final HazelcastInstance hazelcastInstance = instances[i];
            new Thread(new Runnable() {

                public void run() {
                    ITopic<Long> topic = hazelcastInstance.getTopic("first");
                    final long threadId = Thread.currentThread().getId();
                    topic.addMessageListener(new MessageListener<Long>() {

                        public void onMessage(Message<Long> message) {
                            String str = stringMap.get(threadId) + message.getMessageObject().toString();
                            stringMap.put(threadId, str);
                        }
                    });
                    countDownLatch.countDown();
                    try {
                        countDownLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    for (int j = 0; j < 20; j++) {
                        if (threadId % 2 == 0)
                            topic.publish((long) j);
                        else
                            topic.publish(Long.valueOf(-1));
                    }
                    mainLatch.countDown();
                }
            }, String.valueOf(i)).start();

        }
        mainLatch.await();
        Thread.sleep(500);

        String ref = stringMap.values().iterator().next();
        for (String s : stringMap.values()) {
            if (!ref.equals(s)) {
                assertFalse("no total order", true);
                return;
            }
        }
        assertTrue("total order", true);
    }

    @Test
    public void testName() {
        HazelcastInstance hClient = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        ITopic<?> topic = hClient.getTopic("testName");
        Assert.assertEquals("testName", topic.getName());
    }

    @Test
    public void addMessageListener() throws InterruptedException {
        HazelcastInstance hClient = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        ITopic<String> topic = hClient.getTopic("addMessageListener");
        final CountDownLatch latch = new CountDownLatch(1);
        final String message = "Hazelcast Rocks!";
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message<String> msg) {
                if (msg.getMessageObject().equals(message)) {
                    latch.countDown();
                }
            }
        });
        topic.publish(message);
        assertTrue(latch.await(10000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void addTwoMessageListener() throws InterruptedException {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        ITopic<String> topic = hazelcastInstance.getTopic("addTwoMessageListener");
        final CountDownLatch latch = new CountDownLatch(2);
        final String message = "Hazelcast Rocks!";
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message<String> msg) {
                if (msg.getMessageObject().equals(message)) {
                    latch.countDown();
                }
            }
        });
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message<String> msg) {
                if (msg.getMessageObject().equals(message)) {
                    latch.countDown();
                }
            }
        });
        topic.publish(message);
        assertTrue(latch.await(10000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void removeMessageListener() throws InterruptedException {
        try {
            HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
            ITopic<String> topic = hazelcastInstance.getTopic("removeMessageListener");
            final CountDownLatch latch = new CountDownLatch(2);
            final CountDownLatch cp = new CountDownLatch(1);

            MessageListener<String> messageListener = new MessageListener<String>() {
                public void onMessage(Message<String> msg) {
                    latch.countDown();
                    cp.countDown();

                }
            };
            final String message = "message_" + messageListener.hashCode() + "_";
            final String id = topic.addMessageListener(messageListener);
            topic.publish(message + "1");
            cp.await();
            topic.removeMessageListener(id);
            topic.publish(message + "2");
            Thread.sleep(50);
            Assert.assertEquals(1, latch.getCount());
        } finally {
            shutdownNodeFactory();
        }
    }

    @Test
    public void test10TimesRemoveMessageListener() throws InterruptedException {
        ExecutorService ex = Executors.newFixedThreadPool(1);
        final CountDownLatch latch = new CountDownLatch(10);
        try {
            ex.execute(new Runnable() {
                public void run() {
                    for (int i = 0; i < 10; i++) {
                        try {
                            removeMessageListener();
                            latch.countDown();
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                }
            });
            assertTrue(latch.await(30, TimeUnit.SECONDS));
        } finally {
            ex.shutdownNow();
        }
    }

    @Test
    public void testPerformance() throws InterruptedException {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        int count = 10000;
        final ITopic topic = hazelcastInstance.getTopic("perf");
        ExecutorService ex = Executors.newFixedThreadPool(10);
        final CountDownLatch l = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            ex.submit(new Runnable() {
                public void run() {
                    topic.publish("my object");
                    l.countDown();
                }
            });
        }
        assertTrue(l.await(20, TimeUnit.SECONDS));
    }

    @Test
    public void add2listenerAndRemoveOne() throws InterruptedException {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        ITopic<String> topic = hazelcastInstance.getTopic("removeMessageListener");
        final CountDownLatch latch = new CountDownLatch(4);
        final CountDownLatch cp = new CountDownLatch(2);
        final String message = "Hazelcast Rocks!";
        MessageListener<String> messageListener1 = new MessageListener<String>() {
            public void onMessage(Message<String> msg) {
                if (msg.getMessageObject().startsWith(message)) {
                    latch.countDown();
                    cp.countDown();
                }
            }
        };
        MessageListener<String> messageListener2 = new

                MessageListener<String>() {
                    public void onMessage(Message<String> msg) {
                        if (msg.getMessageObject().startsWith(message)) {
                            latch.countDown();
                            cp.countDown();
                        }
                    }
                };
        final String id1 = topic.addMessageListener(messageListener1);
        topic.addMessageListener(messageListener2);
        topic.publish(message + "1");
        Thread.sleep(50);
        topic.removeMessageListener(id1);
        cp.await();
        topic.publish(message + "2");
        Thread.sleep(100);
        Assert.assertEquals(1, latch.getCount());
    }

    /**
     * Testing if topic can properly listen messages
     * and if topic has any issue after a shutdown.
     */
    @Test
    public void testTopicCluster() {
        final Config cfg = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances(cfg);
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];
        String topicName = "TestMessages";
        ITopic<String> topic1 = h1.getTopic(topicName);
        final CountDownLatch latch1 = new CountDownLatch(1);
        topic1.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message msg) {
                assertEquals("Test1", msg.getMessageObject());
                latch1.countDown();
            }
        });
        ITopic<String> topic2 = h2.getTopic(topicName);
        final CountDownLatch latch2 = new CountDownLatch(2);
        topic2.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message msg) {
                assertEquals("Test1", msg.getMessageObject());
                latch2.countDown();
            }
        });
        topic1.publish("Test1");
        h1.getLifecycleService().shutdown();
        topic2.publish("Test1");
        try {
            assertTrue(latch1.await(5, TimeUnit.SECONDS));
            assertTrue(latch2.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException ignored) {
        }
    }

    @Test
    public void testTopicStats() throws InterruptedException {
        HazelcastInstance hazelcastInstance = createHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        ITopic<String> topic = hazelcastInstance.getTopic("testTopicStats");

        final CountDownLatch latch1 = new CountDownLatch(1000);
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message msg) {
                latch1.countDown();
            }
        });
        final CountDownLatch latch2 = new CountDownLatch(2);
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message msg) {
                latch2.countDown();
            }
        });

        for (int i = 0; i < 1000; i++) {
            topic.publish("sancar");
        }

        latch1.await();
        latch2.await();
        LocalTopicStatsImpl stats = (LocalTopicStatsImpl) topic.getLocalTopicStats();
        Assert.assertEquals(1000, stats.getPublishOperationCount());
        Assert.assertEquals(2000, stats.getReceiveOperationCount());
    }
}
