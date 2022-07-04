/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.impl.TopicService;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.Accessors.getNode;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TopicTest extends HazelcastTestSupport {

    @Test
    public void testDestroyTopicRemovesStatistics() {
        String randomTopicName = randomString();

        HazelcastInstance instance = createHazelcastInstance();
        final ITopic<String> topic = instance.getTopic(randomTopicName);
        topic.publish("foobar");

        // we need to give the message the chance to be processed, else the topic statistics are recreated
        // so in theory the destroy for the topic is broken
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
        final int nodeCount = 3;
        final String randomName = "testTopicPublishingMember" + generateRandomString(5);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        HazelcastInstance[] instances = factory.newInstances();

        final AtomicInteger count1 = new AtomicInteger(0);
        final AtomicInteger count2 = new AtomicInteger(0);
        final AtomicInteger count3 = new AtomicInteger(0);

        for (int i = 0; i < nodeCount; i++) {
            final HazelcastInstance instance = instances[i];
            ITopic<Member> topic = instance.getTopic(randomName);
            topic.addMessageListener(new MessageListener<Member>() {
                public void onMessage(Message<Member> message) {
                    Member publishingMember = message.getPublishingMember();
                    if (publishingMember.equals(instance.getCluster().getLocalMember())) {
                        count1.incrementAndGet();
                    }

                    Member messageObject = message.getMessageObject();
                    if (publishingMember.equals(messageObject)) {
                        count2.incrementAndGet();
                    }
                    if (publishingMember.localMember()) {
                        count3.incrementAndGet();
                    }
                }
            });
        }

        for (int i = 0; i < nodeCount; i++) {
            HazelcastInstance instance = instances[i];
            instance.getTopic(randomName).publish(instance.getCluster().getLocalMember());
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(nodeCount, count1.get());
                assertEquals(nodeCount * nodeCount, count2.get());
                assertEquals(nodeCount, count3.get());
            }
        });
    }

    @Test
    public void testTopicPublishAsync() throws Exception {
        final String randomName = "testTopicPublishAsync" + generateRandomString(5);
        final AtomicInteger count = new AtomicInteger(0);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance();
        ITopic<String> topic = instance.getTopic(randomName);
        topic.addMessageListener(new MessageListener<String>() {

            @Override
            public void onMessage(Message<String> message) {
                count.incrementAndGet();
            }
        });

        final CompletableFuture<Void> f = topic.publishAsync("TestMessage").toCompletableFuture();
        assertCompletesEventually(f);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, count.get());
            }
        });
    }

    @Test
    public void testTopicPublishAll() throws ExecutionException, InterruptedException {
        final String randomName = "testTopicPublishAll" + generateRandomString(5);
        final AtomicInteger count = new AtomicInteger(0);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance();
        ITopic<String> topic = instance.getTopic(randomName);
        topic.addMessageListener(new MessageListener<String>() {

            @Override
            public void onMessage(Message<String> message) {
                count.incrementAndGet();
            }
        });

        final List<String> messages = Arrays.asList("message 1", "message 2", "messgae 3");
        topic.publishAll(messages);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(messages.size(), count.get());
            }
        });
    }


    @Test
    public void testTopicPublishingAllAsync() throws Exception {
        final String randomName = "testTopicPublishingAllAsync" + generateRandomString(5);
        final AtomicInteger count = new AtomicInteger(0);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance();
        ITopic<String> topic = instance.getTopic(randomName);
        topic.addMessageListener(new MessageListener<String>() {

            @Override
            public void onMessage(Message<String> message) {
                count.incrementAndGet();
            }
        });
        final List<String> messages = Arrays.asList("message 1", "message 2", "messgae 3");
        final CompletableFuture<Void> f = topic.publishAllAsync(messages).toCompletableFuture();
        assertCompletesEventually(f);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(messages.size(), count.get());
            }
        });
    }

    @Test
    public void testBlockingAsync() {
        AtomicInteger count = new AtomicInteger(0);
        final String randomName = "testTopicPublishingAllAsync" + generateRandomString(5);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance();
        ITopic<String> topic = instance.getTopic(randomName);
        topic.addMessageListener(message -> count.incrementAndGet());
        for (int i = 0; i < 10; i++) {
            topic.publish("message");
        }
        assertTrueEventually(() -> assertEquals(10, count.get()));
        final List<String> data = Arrays.asList("msg 1", "msg 2", "msg 3", "msg 4", "msg 5");
        assertCompletesEventually(topic.publishAllAsync(data).toCompletableFuture());
        assertTrueEventually(() -> assertEquals(15, count.get()));
    }

    @Test(expected = NullPointerException.class)
    public void testTopicPublishingAllException() throws ExecutionException, InterruptedException {
        final int nodeCount = 1;
        final String randomName = "testTopicPublishingAllException" + generateRandomString(5);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        HazelcastInstance[] instances = factory.newInstances();

        Collection<Integer> messages = new ArrayList<>();
        messages.add(1);
        messages.add(null);
        messages.add(3);

        for (int i = 0; i < nodeCount; i++) {
            HazelcastInstance instance = instances[i];
            instance.getTopic(randomName).publishAll(messages);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTopicLocalOrder() throws Exception {
        final int nodeCount = 5;
        final int count = 1000;
        final String randomTopicName = randomString();

        Config config = new Config();
        config.getTopicConfig(randomTopicName).setGlobalOrderingEnabled(false);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final HazelcastInstance[] instances = factory.newInstances(config);

        final List<TestMessage>[] messageLists = new List[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            messageLists[i] = new CopyOnWriteArrayList<TestMessage>();
        }

        final CountDownLatch startLatch = new CountDownLatch(nodeCount);
        final CountDownLatch messageLatch = new CountDownLatch(nodeCount * nodeCount * count);
        final CountDownLatch publishLatch = new CountDownLatch(nodeCount * count);

        ExecutorService ex = Executors.newFixedThreadPool(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            final int finalI = i;
            ex.execute(new Runnable() {
                public void run() {
                    final List<TestMessage> messages = messageLists[finalI];
                    HazelcastInstance hz = instances[finalI];
                    ITopic<TestMessage> topic = hz.getTopic(randomTopicName);
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
                        topic.publish(new TestMessage(localMember, UuidUtil.newUnsecureUuidString()));
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
                    // sort only publisher blocks. if publishers are the same, leave them as they are
                    return m1.publisher.getUuid().compareTo(m2.publisher.getUuid());
                }
            };
            Arrays.sort(ref, comparator);

            for (int i = 1; i < nodeCount; i++) {
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
    @SuppressWarnings("unchecked")
    public void testTopicGlobalOrder() throws Exception {
        final int nodeCount = 5;
        final int count = 1000;
        final String randomTopicName = randomString();

        Config config = new Config();
        config.getTopicConfig(randomTopicName).setGlobalOrderingEnabled(true);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final HazelcastInstance[] nodes = factory.newInstances(config);

        final List<TestMessage>[] messageListPerNode = new List[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            messageListPerNode[i] = new CopyOnWriteArrayList<TestMessage>();
        }

        final CountDownLatch messageLatch = new CountDownLatch(nodeCount * count);
        // add message listeners
        for (int i = 0; i < nodes.length; i++) {
            final int nodeIndex = i;
            ITopic<TestMessage> topic = nodes[i].getTopic(randomTopicName);
            topic.addMessageListener(new MessageListener<TestMessage>() {
                public void onMessage(Message<TestMessage> message) {
                    messageListPerNode[nodeIndex].add(message.getMessageObject());
                    messageLatch.countDown();
                }
            });
        }

        // publish messages
        for (HazelcastInstance node : nodes) {
            Member localMember = node.getCluster().getLocalMember();
            for (int j = 0; j < count; j++) {
                TestMessage message = new TestMessage(localMember, UUID.randomUUID().toString());
                ITopic<Object> topic = node.getTopic(randomTopicName);
                topic.publish(message);
            }
        }

        // all messages in nodes messageLists should be equal
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int i = 0;
                do {
                    assertEquals(messageListPerNode[i], messageListPerNode[i++]);
                } while (i < nodeCount);
            }
        });
    }

    private static class TestMessage implements DataSerializable {
        Member publisher;
        String data;

        @SuppressWarnings("unused")
        TestMessage() {
        }

        TestMessage(Member publisher, String data) {
            this.publisher = publisher;
            this.data = data;
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            publisher.writeData(out);
            out.writeString(data);
        }

        public void readData(ObjectDataInput in) throws IOException {
            publisher = new MemberImpl();
            publisher.readData(in);
            data = in.readString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestMessage that = (TestMessage) o;

            if (data != null ? !data.equals(that.data) : that.data != null) {
                return false;
            }
            if (publisher != null ? !publisher.equals(that.publisher) : that.publisher != null) {
                return false;
            }

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
            return "TestMessage{" + "publisher=" + publisher + ", data='" + data + "'}";
        }
    }

    @Test
    public void testName() {
        String randomTopicName = randomString();

        HazelcastInstance hClient = createHazelcastInstance();
        ITopic<?> topic = hClient.getTopic(randomTopicName);

        assertEquals(randomTopicName, topic.getName());
    }

    @Test
    public void addMessageListener() throws InterruptedException {
        String randomTopicName = "addMessageListener" + generateRandomString(5);

        HazelcastInstance instance = createHazelcastInstance();
        ITopic<String> topic = instance.getTopic(randomTopicName);

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

        assertTrue(latch.await(10000, MILLISECONDS));
    }

    @Test
    public void testConfigListenerRegistration() throws InterruptedException {
        String topicName = "default";

        Config config = new Config();

        final CountDownLatch latch = new CountDownLatch(1);
        config.getTopicConfig(topicName).addMessageListenerConfig(new ListenerConfig().setImplementation(new MessageListener() {
            public void onMessage(Message message) {
                latch.countDown();
            }
        }));

        HazelcastInstance instance = createHazelcastInstance(config);
        instance.getTopic(topicName).publish(1);

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void addTwoMessageListener() throws InterruptedException {
        String topicName = "addTwoMessageListener" + generateRandomString(5);

        HazelcastInstance instance = createHazelcastInstance();
        ITopic<String> topic = instance.getTopic(topicName);

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

        assertTrue(latch.await(10000, MILLISECONDS));
    }

    @Test
    public void removeMessageListener() throws InterruptedException {
        String topicName = "removeMessageListener" + generateRandomString(5);

        try {
            HazelcastInstance instance = createHazelcastInstance();
            ITopic<String> topic = instance.getTopic(topicName);

            final AtomicInteger onMessageCount = new AtomicInteger(0);
            final CountDownLatch onMessageInvoked = new CountDownLatch(1);

            MessageListener<String> messageListener = new MessageListener<String>() {
                public void onMessage(Message<String> msg) {
                    onMessageCount.incrementAndGet();
                    onMessageInvoked.countDown();
                }
            };

            final String message = "message_" + messageListener.hashCode() + "_";
            final UUID id = topic.addMessageListener(messageListener);
            topic.publish(message + "1");
            onMessageInvoked.await();
            assertTrue(topic.removeMessageListener(id));
            topic.publish(message + "2");

            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    assertEquals(1, onMessageCount.get());
                }
            });
        } finally {
            shutdownNodeFactory();
        }
    }

    @Test
    public void testPerformance() throws InterruptedException {
        int count = 10000;
        String randomTopicName = randomString();

        HazelcastInstance instance = createHazelcastInstance();
        ExecutorService ex = Executors.newFixedThreadPool(10);

        final ITopic<String> topic = instance.getTopic(randomTopicName);
        final CountDownLatch latch = new CountDownLatch(count);

        for (int i = 0; i < count; i++) {
            ex.submit(new Runnable() {
                public void run() {
                    topic.publish("my object");
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(20, TimeUnit.SECONDS));
    }

    @Test
    public void addTwoListenerAndRemoveOne() throws InterruptedException {
        String topicName = "addTwoListenerAndRemoveOne" + generateRandomString(5);

        HazelcastInstance instance = createHazelcastInstance();
        ITopic<String> topic = instance.getTopic(topicName);

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

        UUID messageListenerId = topic.addMessageListener(messageListener1);
        topic.addMessageListener(messageListener2);
        topic.publish(message);
        assertOpenEventually(cp);
        topic.removeMessageListener(messageListenerId);
        topic.publish(message);

        assertOpenEventually(latch);
        assertEquals(3, atomicInteger.get());
    }

    /**
     * Testing if topic can properly listen messages and if topic has any issue after a shutdown.
     */
    @Test
    public void testTopicCluster() throws InterruptedException {
        String topicName = "TestMessages" + generateRandomString(5);
        Config cfg = new Config();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances(cfg);
        HazelcastInstance instance1 = instances[0];
        HazelcastInstance instance2 = instances[1];

        ITopic<String> topic1 = instance1.getTopic(topicName);
        final CountDownLatch latch1 = new CountDownLatch(1);
        final String message = "Test" + randomString();

        topic1.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message msg) {
                assertEquals(message, msg.getMessageObject());
                latch1.countDown();
            }
        });

        ITopic<String> topic2 = instance2.getTopic(topicName);
        final CountDownLatch latch2 = new CountDownLatch(2);
        topic2.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message msg) {
                assertEquals(message, msg.getMessageObject());
                latch2.countDown();
            }
        });

        topic1.publish(message);
        assertOpenEventually(latch1);

        instance1.shutdown();
        topic2.publish(message);
        assertOpenEventually(latch2);
    }

    @Test
    public void testTopicStats() throws InterruptedException {
        String topicName = "testTopicStats" + generateRandomString(5);

        HazelcastInstance instance = createHazelcastInstance();
        ITopic<String> topic = instance.getTopic(topicName);

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

    @Test
    @Category(NightlyTest.class)
    @SuppressWarnings("unchecked")
    public void testTopicMultiThreading() throws Exception {
        final int nodeCount = 5;
        final int count = 1000;
        final String randomTopicName = randomString();

        Config config = new Config();
        config.getTopicConfig(randomTopicName).setGlobalOrderingEnabled(false);
        config.getTopicConfig(randomTopicName).setMultiThreadingEnabled(true);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final HazelcastInstance[] instances = factory.newInstances(config);

        final Set<String>[] threads = new Set[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            threads[i] = new HashSet<String>();
        }

        final CountDownLatch startLatch = new CountDownLatch(nodeCount);
        final CountDownLatch messageLatch = new CountDownLatch(nodeCount * nodeCount * count);
        final CountDownLatch publishLatch = new CountDownLatch(nodeCount * count);

        ExecutorService ex = Executors.newFixedThreadPool(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            final int finalI = i;
            ex.execute(new Runnable() {
                public void run() {
                    final Set<String> thNames = threads[finalI];
                    HazelcastInstance hz = instances[finalI];
                    ITopic<TestMessage> topic = hz.getTopic(randomTopicName);
                    topic.addMessageListener(new MessageListener<TestMessage>() {
                        public void onMessage(Message<TestMessage> message) {
                            thNames.add(Thread.currentThread().getName());
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
                        topic.publish(new TestMessage(localMember, UuidUtil.newUnsecureUuidString()));
                        publishLatch.countDown();
                    }
                }
            });
        }

        try {
            assertTrue(publishLatch.await(2, TimeUnit.MINUTES));
            assertTrue(messageLatch.await(5, TimeUnit.MINUTES));

            boolean passed = false;
            for (int i = 0; i < nodeCount; i++) {
                if (threads[i].size() > 1) {
                    passed = true;
                }
            }
            assertTrue("All listeners received messages in single thread. Expecting more threads involved", passed);
        } finally {
            ex.shutdownNow();
        }
    }

    @Test
    public void givenTopicHasNoSubscriber_whenMessageIsPublished_thenNoSerialializationIsInvoked() {
        final int nodeCount = 2;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final HazelcastInstance[] instances = factory.newInstances();
        ITopic<SerializationCounting> topic = instances[0].getTopic(randomString());

        SerializationCounting message = new SerializationCounting();
        topic.publish(message);

        assertNoSerializationInvoked(message);
    }

    private void assertNoSerializationInvoked(SerializationCounting localMessage) {
        assertEquals(0, localMessage.getSerializationCount());
    }

    public static class SerializationCounting implements DataSerializable {
        private AtomicInteger counter = new AtomicInteger();

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            counter.incrementAndGet();
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {

        }

        public int getSerializationCount() {
            return counter.get();
        }
    }

}
