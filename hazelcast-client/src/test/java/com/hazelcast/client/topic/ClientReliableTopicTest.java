/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.proxy.ClientReliableTopicProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.impl.reliable.ReliableMessageListenerMock;
import com.hazelcast.util.Clock;
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
@Category({QuickTest.class, ParallelTest.class})
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

    @Test(expected = NullPointerException.class)
    public void addMessageListener_whenNull() {
        ITopic topic = client.getReliableTopic(randomString());
        topic.addMessageListener(null);
    }

    @Test
    public void addMessageListener() {
        ITopic topic = client.getReliableTopic(randomString());
        String id = topic.addMessageListener(new ReliableMessageListenerMock());
        assertNotNull(id);
    }

    // ============== removeMessageListener ==============================

    @Test(expected = NullPointerException.class)
    public void removeMessageListener_whenNull() {
        ITopic topic = client.getReliableTopic(randomString());
        topic.removeMessageListener(null);
    }

    @Test
    public void removeMessageListener_whenExisting() {
        ITopic topic = client.getReliableTopic(randomString());
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        String id = topic.addMessageListener(listener);

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
        boolean result = topic.removeMessageListener(UUID.randomUUID().toString());

        assertFalse(result);
    }

    @Test
    public void removeMessageListener_whenAlreadyRemoved() {
        ITopic topic = client.getReliableTopic(randomString());
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        String id = topic.addMessageListener(listener);
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
    public void publishNull() throws InterruptedException {
        ITopic topic = client.getReliableTopic(randomString());
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        topic.addMessageListener(listener);
        topic.publish(null);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertContains(listener.objects, null);
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
        String id = topic.addMessageListener(listener);

        assertTrue(topic.removeMessageListener(id));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalTopicStats() throws Exception {
        ITopic topic = client.getReliableTopic(randomString());

        topic.getLocalTopicStats();
    }
}
