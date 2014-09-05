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
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.util.UuidUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class TopicTest extends HazelcastTestSupport {

    @Test
    public void testDestroyTopicRemovesStatistics() {
        HazelcastInstance instance = createHazelcastInstance();
        final ITopic topic = instance.getTopic("foo");
        topic.publish("foobar");

        //we need to give the message the chance to be processed, else the topic statistics are recreated.
        //so in theory the destroy for the topic is broken.
        sleepSeconds(1);

        topic.destroy();

        final TopicService topicService = getNode(instance).nodeEngine.getService(TopicService.SERVICE_NAME);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                boolean containsStats = topicService.getStatsMap().containsKey(topic.getName());
                assertFalse(containsStats);
            }
        });
    }

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
                            Member publishingMember = message.getPublishingMember();
                            if (publishingMember.equals(instance.getCluster().getLocalMember()))
                                count.incrementAndGet();
                            if (publishingMember.equals(message.getMessageObject()))
                                count1.incrementAndGet();
                            if (publishingMember.localMember())
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
            fail();
        }

        for (int i = 0; i < k; i++) {
            HazelcastInstance instance = instances[i];
            instance.getTopic(name).publish(instance.getCluster().getLocalMember());
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(k, count.get());
                assertEquals(k * k, count1.get());
                assertEquals(k, count2.get());
            }
        });
    }

    @Test
    public void testTopicLocalOrder() throws Exception {
        final int k = 5;
        final int count = 1000;
        final CountDownLatch startLatch = new CountDownLatch(k);
        final CountDownLatch messageLatch = new CountDownLatch(k * k * count);
        final CountDownLatch publishLatch = new CountDownLatch(k * count);
        final Config config = new Config();
        config.getTopicConfig("default").setGlobalOrderingEnabled(false);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final List<TestMessage>[] messageLists = new List[k];
        for (int i = 0; i < k; i++) {
            messageLists[i] = new CopyOnWriteArrayList<TestMessage>();
        }

        ExecutorService ex = Executors.newFixedThreadPool(k);
        for (int i = 0; i < k; i++) {
            final int finalI = i;
            ex.execute(new Runnable() {
                public void run() {
                    final List<TestMessage> messages = messageLists[finalI];
                    HazelcastInstance hz = instances[finalI];
                    ITopic<TestMessage> topic = hz.getTopic("default");
                    topic.addMessageListener(new MessageListener<TestMessage>() {
                        public void onMessage(Message<TestMessage> message) {
                            messages.add(message.getMessageObject());
                            messageLatch.countDown();
                        }
                    });

                    startLatch.countDown();
                    try {
                        startLatch.await(1, TimeUnit.MINUTES);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        return;
                    }

                    Member localMember = hz.getCluster().getLocalMember();
                    for (int j = 0; j < count; j++) {
                        topic.publish(new TestMessage(localMember, UuidUtil.buildRandomUuidString()));
                        publishLatch.countDown();
                    }
                }
            });
        }

        try {
            assertTrue(publishLatch.await(2, TimeUnit.MINUTES));
            assertTrue(messageLatch.await(5, TimeUnit.MINUTES));
            TestMessage[] ref = new TestMessage[messageLists[0].size()];
            messageLists[0].toArray(ref);

            Comparator<TestMessage> comparator = new Comparator<TestMessage>() {
                public int compare(TestMessage m1, TestMessage m2) {
                    // sort only publisher blocks. if publishers are the same, leave them as they are.
                    return m1.publisher.getUuid().compareTo(m2.publisher.getUuid());
                }
            };
            Arrays.sort(ref, comparator);

            for (int i = 1; i < k; i++) {
                TestMessage[] messages = new TestMessage[messageLists[i].size()];
                messageLists[i].toArray(messages);
                Arrays.sort(messages, comparator);
                assertArrayEquals(ref, messages);
            }
        } finally {
            ex.shutdownNow();
        }
    }

    @Test
    public void testTopicGlobalOrder() throws Exception {
        final int k = 5;
        final int count = 1000;
        final CountDownLatch startLatch = new CountDownLatch(k);
        final CountDownLatch messageLatch = new CountDownLatch(k * k * count);
        final CountDownLatch publishLatch = new CountDownLatch(k * count);
        final Config config = new Config();
        config.getTopicConfig("default").setGlobalOrderingEnabled(true);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final List<TestMessage>[] messageLists = new List[k];
        for (int i = 0; i < k; i++) {
            messageLists[i] = new CopyOnWriteArrayList<TestMessage>();
        }

        ExecutorService ex = Executors.newFixedThreadPool(k);
        for (int i = 0; i < k; i++) {
            final int finalI = i;
            ex.execute(new Runnable() {
                public void run() {
                    final List<TestMessage> messages = messageLists[finalI];
                    HazelcastInstance hz = instances[finalI];
                    ITopic<TestMessage> topic = hz.getTopic("default");
                    topic.addMessageListener(new MessageListener<TestMessage>() {
                        public void onMessage(Message<TestMessage> message) {
                            messages.add(message.getMessageObject());
                            messageLatch.countDown();
                        }
                    });

                    startLatch.countDown();
                    try {
                        startLatch.await(1, TimeUnit.MINUTES);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        return;
                    }

                    Member localMember = hz.getCluster().getLocalMember();
                    for (int j = 0; j < count; j++) {
                        topic.publish(new TestMessage(localMember, UUID.randomUUID().toString()));
                        publishLatch.countDown();
                    }
                }
            });
        }

        try {
            assertTrue(publishLatch.await(2, TimeUnit.MINUTES));
            assertTrue(messageLatch.await(5, TimeUnit.MINUTES));
            TestMessage[] ref = new TestMessage[messageLists[0].size()];
            messageLists[0].toArray(ref);

            for (int i = 1; i < k; i++) {
                TestMessage[] messages = new TestMessage[messageLists[i].size()];
                messageLists[i].toArray(messages);

                assertArrayEquals(ref, messages);
            }
        } finally {
            ex.shutdownNow();
        }
    }

    static class TestMessage implements DataSerializable {
        Member publisher;
        String data;

        TestMessage() {
        }

        TestMessage(Member publisher, String data) {
            this.publisher = publisher;
            this.data = data;
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            publisher.writeData(out);
            out.writeUTF(data);
        }

        public void readData(ObjectDataInput in) throws IOException {
            publisher = new MemberImpl();
            publisher.readData(in);
            data = in.readUTF();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestMessage that = (TestMessage) o;

            if (data != null ? !data.equals(that.data) : that.data != null) return false;
            if (publisher != null ? !publisher.equals(that.publisher) : that.publisher != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = publisher != null ? publisher.hashCode() : 0;
            result = 31 * result + (data != null ? data.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("TestMessage{");
            sb.append("publisher=").append(publisher);
            sb.append(", data='").append(data).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

    @Test
    public void testName() {
        HazelcastInstance hClient = createHazelcastInstance();
        ITopic<?> topic = hClient.getTopic("testName");
        assertEquals("testName", topic.getName());
    }

    @Test
    public void addMessageListener() throws InterruptedException {
        HazelcastInstance hClient = createHazelcastInstance();
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
    public void testConfigListenerRegistration() throws InterruptedException {
        Config config = new Config();
        final String name = "default";
        final CountDownLatch latch = new CountDownLatch(1);
        config.getTopicConfig(name).addMessageListenerConfig(new ListenerConfig().setImplementation(new MessageListener() {
            public void onMessage(Message message) {
                latch.countDown();
            }
        }));
        final HazelcastInstance hz = createHazelcastInstance(config);
        hz.getTopic(name).publish(1);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void addTwoMessageListener() throws InterruptedException {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
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
            HazelcastInstance hazelcastInstance = createHazelcastInstance();
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

            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    assertEquals(1, latch.getCount());
                }
            });
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
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
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
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        ITopic<String> topic = hazelcastInstance.getTopic("add2listenerAndRemoveOne");
        final CountDownLatch latch = new CountDownLatch(3);
        final CountDownLatch cp = new CountDownLatch(2);
        final AtomicInteger atomicInteger = new AtomicInteger();
        final String message = "Hazelcast Rocks!";
        MessageListener<String> messageListener1 = new MessageListener<String>() {
            public void onMessage(Message<String> msg) {
                atomicInteger.incrementAndGet();
                latch.countDown();
                cp.countDown();
            }
        };
        MessageListener<String> messageListener2 = new MessageListener<String>() {
            public void onMessage(Message<String> msg) {
                atomicInteger.incrementAndGet();
                latch.countDown();
                cp.countDown();
            }
        };
        final String id1 = topic.addMessageListener(messageListener1);
        topic.addMessageListener(messageListener2);
        topic.publish(message);
        assertOpenEventually(cp);
        topic.removeMessageListener(id1);
        topic.publish(message);

        assertOpenEventually(latch);
        assertEquals(3, atomicInteger.get());
    }

    /**
     * Testing if topic can properly listen messages
     * and if topic has any issue after a shutdown.
     */
    @Test
    public void testTopicCluster() throws InterruptedException {
        final Config cfg = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances(cfg);
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];
        String topicName = "TestMessages";
        ITopic<String> topic1 = h1.getTopic(topicName);
        final CountDownLatch latch1 = new CountDownLatch(1);
        final String message = "Test1";
        topic1.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message msg) {
                assertEquals(message, msg.getMessageObject());
                latch1.countDown();
            }
        });
        ITopic<String> topic2 = h2.getTopic(topicName);
        final CountDownLatch latch2 = new CountDownLatch(2);
        topic2.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message msg) {
                assertEquals(message, msg.getMessageObject());
                latch2.countDown();
            }
        });

        topic1.publish(message);
        assertTrue(latch1.await(5, TimeUnit.SECONDS));

        h1.shutdown();
        topic2.publish(message);
        assertTrue(latch2.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testTopicStats() throws InterruptedException {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        ITopic<String> topic = hazelcastInstance.getTopic("testTopicStats");

        final CountDownLatch latch1 = new CountDownLatch(1000);
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message msg) {
                latch1.countDown();
            }
        });
        final CountDownLatch latch2 = new CountDownLatch(1000);
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message msg) {
                latch2.countDown();
            }
        });

        for (int i = 0; i < 1000; i++) {
            topic.publish("sancar");
        }

        assertTrue(latch1.await(1, TimeUnit.MINUTES));
        assertTrue(latch2.await(1, TimeUnit.MINUTES));

        LocalTopicStatsImpl stats = (LocalTopicStatsImpl) topic.getLocalTopicStats();
        assertEquals(1000, stats.getPublishOperationCount());
        assertEquals(2000, stats.getReceiveOperationCount());
    }
}
