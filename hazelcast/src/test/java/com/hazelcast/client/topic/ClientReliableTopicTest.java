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

package com.hazelcast.client.topic;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.impl.proxy.ClientReliableTopicProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.topic.impl.reliable.DurableSubscriptionTest;
import com.hazelcast.topic.impl.reliable.ReliableMessageListenerMock;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientReliableTopicTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;

    @Before
    public void setup() {
        Config config = new Config();
        hazelcastFactory.newHazelcastInstance(config);
        client = hazelcastFactory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testCreation() {
        ITopic topic = client.getReliableTopic(randomString());
        assertInstanceOf(ClientReliableTopicProxy.class, topic);
    }

    // ============== addMessageListener ==============================

    @Test
    public void addMessageListener() {
        ITopic topic = client.getReliableTopic(randomString());
        UUID id = topic.addMessageListener(new ReliableMessageListenerMock());
        assertNotNull(id);
    }

    // ============== removeMessageListener ==============================

    @Test
    public void removeMessageListener_whenExisting() {
        ITopic topic = client.getReliableTopic(randomString());
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        UUID id = topic.addMessageListener(listener);

        boolean removed = topic.removeMessageListener(id);
        assertTrue(removed);
        topic.publish("1");

        // it should not receive any events.
        assertTrueDelayed5sec(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, listener.objects.size());
            }
        });
    }

    @Test
    public void removeMessageListener_whenNonExisting() {
        ITopic topic = client.getReliableTopic(randomString());
        boolean result = topic.removeMessageListener(UUID.randomUUID());

        assertFalse(result);
    }

    @Test
    public void removeMessageListener_whenAlreadyRemoved() {
        ITopic topic = client.getReliableTopic(randomString());
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        UUID id = topic.addMessageListener(listener);
        topic.removeMessageListener(id);

        boolean result = topic.removeMessageListener(id);
        assertFalse(result);

        topic.publish("1");

        // it should not receive any events.
        assertTrueDelayed5sec(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, listener.objects.size());
            }
        });
    }

    // ============================================

    @Test
    public void publishSingle() throws InterruptedException {
        ITopic topic = client.getReliableTopic(randomString());
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        topic.addMessageListener(listener);
        final String msg = "foobar";
        topic.publish(msg);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertContains(listener.objects, msg);
            }
        });
    }

    @Test
    public void publishMultiple() throws InterruptedException {
        ITopic topic = client.getReliableTopic(randomString());
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        topic.addMessageListener(listener);

        final List<String> items = new ArrayList<String>();
        for (int k = 0; k < 5; k++) {
            items.add("" + k);
        }

        for (String item : items) {
            topic.publish(item);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(items, Arrays.asList(listener.objects.toArray()));
            }
        });
    }

    @Test
    public void testMessageFieldSetCorrectly() {
        ITopic topic = client.getReliableTopic(randomString());
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        topic.addMessageListener(listener);

        final long beforePublishTime = Clock.currentTimeMillis();
        final String messageStr = randomString();
        topic.publish(messageStr);
        final long afterPublishTime = Clock.currentTimeMillis();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.messages.size());
                Message<String> message = listener.messages.get(0);

                assertEquals(messageStr, message.getMessageObject());
                assertNull(message.getPublishingMember());

                long actualPublishTime = message.getPublishTime();
                assertTrue(actualPublishTime >= beforePublishTime);
                assertTrue(actualPublishTime <= afterPublishTime);
            }
        });
    }

    // makes sure that when a listener is register, we don't see any messages being published before
    // it got registered. We'll only see the messages after it got registered.
    @Test
    public void testAlwaysStartAfterTail() {
        final ITopic topic = client.getReliableTopic(randomString());
        topic.publish("1");
        topic.publish("2");
        topic.publish("3");

        spawn(new Runnable() {
            @Override
            public void run() {
                sleepSeconds(5);
                topic.publish("4");
                topic.publish("5");
                topic.publish("6");
            }
        });

        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        topic.addMessageListener(listener);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(asList("4", "5", "6"), Arrays.asList(listener.objects.toArray()));
            }
        });
    }

    @Test
    public void testListener() throws InterruptedException {
        ITopic topic = client.getReliableTopic(randomString());
        int messageCount = 10;
        final CountDownLatch latch = new CountDownLatch(messageCount);
        MessageListener listener = new MessageListener() {
            public void onMessage(Message message) {
                latch.countDown();
            }
        };
        topic.addMessageListener(listener);

        for (int i = 0; i < messageCount; i++) {
            topic.publish(i);
        }
        assertTrue(latch.await(20, TimeUnit.SECONDS));
    }

    @Test
    public void testRemoveListener() {
        ITopic topic = client.getReliableTopic(randomString());

        MessageListener listener = new MessageListener() {
            public void onMessage(Message message) {
            }
        };
        UUID id = topic.addMessageListener(listener);

        assertTrue(topic.removeMessageListener(id));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalTopicStats() throws Exception {
        ITopic topic = client.getReliableTopic(randomString());

        topic.getLocalTopicStats();
    }

    @Test
    public void shouldNotBeTerminated_whenClientIsOffline() {
        final HazelcastInstance ownerMember = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.ASYNC);
        String topicName = "topic";
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        int publishCount = 1000;

        final CountDownLatch messageArrived = new CountDownLatch(publishCount);
        ITopic<String> topic = client.getReliableTopic(topicName);
        final UUID id = topic.addMessageListener(new DurableSubscriptionTest.DurableMessageListener<String>() {
            @Override
            public void onMessage(Message<String> message) {
                messageArrived.countDown();
            }

        });

        HazelcastInstance member2 = hazelcastFactory.newHazelcastInstance();
        waitAllForSafeState(ownerMember, member2);

        ITopic<Object> reliableTopic = member2.getReliableTopic(topicName);

        //kill the the owner member, while messages are coming
        new Thread(new Runnable() {
            @Override
            public void run() {
                sleepMillis(1);
                ownerMember.shutdown();
            }
        }).start();

        for (int i = 0; i < publishCount; i++) {
            reliableTopic.publish("msg " + (i + 100));
        }

        assertOpenEventually(messageArrived);
        TestCase.assertTrue(topic.removeMessageListener(id));
    }
}
