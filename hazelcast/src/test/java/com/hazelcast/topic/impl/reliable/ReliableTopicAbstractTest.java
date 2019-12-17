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

package com.hazelcast.topic.impl.reliable;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.LocalTopicStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.internal.util.Clock;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class ReliableTopicAbstractTest extends HazelcastTestSupport {

    private static final int CAPACITY = 10;

    private ReliableTopicProxy<String> topic;
    private HazelcastInstance local;

    @Before
    public void setup() {
        ReliableTopicConfig topicConfig = new ReliableTopicConfig("reliableTopic*");

        RingbufferConfig ringbufferConfig = new RingbufferConfig(topicConfig.getName());
        ringbufferConfig.setCapacity(CAPACITY);

        Config config = new Config();
        config.addReliableTopicConfig(topicConfig);
        config.addRingBufferConfig(ringbufferConfig);

        HazelcastInstance[] instances = newInstances(config);
        local = instances[0];
        HazelcastInstance target = instances[instances.length - 1];

        String name = randomNameOwnedBy(target, "reliableTopic");
        topic = (ReliableTopicProxy<String>) local.<String>getReliableTopic(name);
    }

    protected abstract HazelcastInstance[] newInstances(Config config);

    // ============== addMessageListener ==============================

    @Test
    public void addMessageListener() {
        UUID id = topic.addMessageListener(new ReliableMessageListenerMock());

        assertNotNull(id);
    }

    // ============== removeMessageListener ==============================

    @Test
    public void removeMessageListener_whenExisting() {
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        UUID id = topic.addMessageListener(listener);

        boolean removed = topic.removeMessageListener(id);
        assertTrue(removed);
        topic.publish("1");

        // it should not receive any events
        assertTrueDelayed5sec(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, listener.objects.size());
            }
        });
    }

    @Test
    public void removeMessageListener_whenNonExisting() {
        boolean result = topic.removeMessageListener(UUID.randomUUID());

        assertFalse(result);
    }

    @Test
    public void removeMessageListener_whenAlreadyRemoved() {
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        UUID id = topic.addMessageListener(listener);
        topic.removeMessageListener(id);

        boolean result = topic.removeMessageListener(id);
        assertFalse(result);

        topic.publish("1");

        // it should not receive any events
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
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        topic.addMessageListener(listener);

        final long beforePublishTime = Clock.currentTimeMillis();
        topic.publish("foo");
        final long afterPublishTime = Clock.currentTimeMillis();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.messages.size());
                Message<String> message = listener.messages.get(0);

                assertEquals("foo", message.getMessageObject());
                Member localMember = local.getCluster().getLocalMember();
                assertEquals(localMember, message.getPublishingMember());

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
    public void statistics() {
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();

        topic.addMessageListener(listener);
        final ITopic<Object> anotherTopic = local.getReliableTopic("anotherTopic");

        final int messageCount = 10;
        final LocalTopicStats localTopicStats = topic.getLocalTopicStats();
        for (int k = 0; k < messageCount; k++) {
            topic.publish("foo");
            anotherTopic.publish("foo");
        }


        assertEquals(messageCount, localTopicStats.getPublishOperationCount());
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(messageCount, localTopicStats.getReceiveOperationCount());
            }
        });

        final ReliableTopicService reliableTopicService = getNode(local).nodeEngine.getService(ReliableTopicService.SERVICE_NAME);
        final Map<String, LocalTopicStats> stats = reliableTopicService.getStats();
        assertEquals(2, stats.size());
        assertEquals(messageCount, stats.get(topic.getName()).getPublishOperationCount());
        assertEquals(messageCount, stats.get(topic.getName()).getReceiveOperationCount());
        assertEquals(messageCount, stats.get(anotherTopic.getName()).getPublishOperationCount());
        assertEquals(0, stats.get(anotherTopic.getName()).getReceiveOperationCount());
    }

    @Test
    public void testDestroyTopicRemovesStatistics() {
        topic.publish("foobar");

        final ReliableTopicService reliableTopicService = getNode(local).nodeEngine.getService(ReliableTopicService.SERVICE_NAME);
        final Map<String, LocalTopicStats> stats = reliableTopicService.getStats();
        assertEquals(1, stats.size());
        assertEquals(1, stats.get(topic.getName()).getPublishOperationCount());

        topic.destroy();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(reliableTopicService.getStats().containsKey(topic.getName()));
            }
        });
    }
}
