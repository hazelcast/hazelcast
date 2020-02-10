/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientTopicTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
    }

    @Test
    public void testListener() throws InterruptedException {
        ITopic<String> topic = client.getTopic(randomString());
        final CountDownLatch latch = new CountDownLatch(10);
        topic.addMessageListener(new MessageListener<String>() {

            @Override
            public void onMessage(Message<String> message) {
                latch.countDown();
            }
        });
        for (int i = 0; i < 10; i++) {
            topic.publish("message " + i);
        }

        assertTrue(latch.await(20, TimeUnit.SECONDS));
    }

    @Test
    public void testRemoveListener() {
        ITopic topic = client.getTopic(randomString());

        MessageListener listener = new MessageListener() {
            public void onMessage(Message message) {
            }
        };
        UUID id = topic.addMessageListener(listener);

        assertTrue(topic.removeMessageListener(id));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalTopicStats() throws Exception {
        ITopic topic = client.getTopic(randomString());

        topic.getLocalTopicStats();
    }

    @Test
    public void testPublish() throws InterruptedException {
        ITopic<String> topic = client.getTopic(randomString());
        final AtomicInteger count = new AtomicInteger(0);

        topic.addMessageListener(new MessageListener<String>() {

            @Override
            public void onMessage(Message<String> message) {
                count.incrementAndGet();
            }
        });
        topic.publish("message");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, count.get());
            }
        });
    }


    @Test
    public void testPublishAsync() throws InterruptedException {
        ITopic<String> topic = client.getTopic(randomString());
        final AtomicInteger count = new AtomicInteger(0);

        topic.addMessageListener(new MessageListener<String>() {

            @Override
            public void onMessage(Message<String> message) {
                count.incrementAndGet();
            }
        });
        topic.publishAsync("message");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, count.get());
            }
        });
    }

    @Test
    public void testPublishAll() throws InterruptedException {
        ITopic<String> topic = client.getTopic(randomString());
        final AtomicInteger count = new AtomicInteger(0);

        topic.addMessageListener(new MessageListener<String>() {

            @Override
            public void onMessage(Message<String> message) {
                count.incrementAndGet();
            }
        });
        final List<String> messages = Arrays.asList("message 1", "message 2", "message 3");
        topic.publishAllAsync(messages);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, count.get());
            }
        });
    }

    @Test
    public void testPublishAllAsync() throws InterruptedException {
        ITopic<String> topic = client.getTopic(randomString());
        final AtomicInteger count = new AtomicInteger(0);

        topic.addMessageListener(new MessageListener<String>() {

            @Override
            public void onMessage(Message<String> message) {
                count.incrementAndGet();
            }
        });

        final List<String> messages = Arrays.asList("message 1", "message 2", "messgae 3");

        topic.publishAllAsync(messages);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, count.get());
            }
        });
    }

    @Test(expected = NullPointerException.class)
    public void testPublishAllException() {
        ITopic<Integer> topic = client.getTopic(randomString());
        Collection<Integer> messages = new ArrayList<>();
        messages.add(1);
        messages.add(null);
        messages.add(3);
        topic.publishAll(messages);
    }
}
