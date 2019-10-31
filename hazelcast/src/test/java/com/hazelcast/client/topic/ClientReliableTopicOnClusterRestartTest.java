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

package com.hazelcast.client.topic;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.proxy.ClientReliableTopicProxy;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.impl.reliable.DurableSubscriptionTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientReliableTopicOnClusterRestartTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    /**
     * Test for #9766 (https://github.com/hazelcast/hazelcast/issues/9766)
     */
    @Test
    public void serverRestartWhenReliableTopicListenerRegistered() {
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        String topicName = "topic";
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setFailOnMaxBackoff(false);
        HazelcastInstance hazelcastClient = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastInstance hazelcastClient2 = hazelcastFactory.newHazelcastClient(clientConfig);
        ITopic<Integer> topic = hazelcastClient.getReliableTopic(topicName);
        final ITopic<Integer> topic2 = hazelcastClient2.getReliableTopic(topicName);

        final CountDownLatch listenerLatch = new CountDownLatch(1);

        // Add listener using the first client
        topic.addMessageListener(new MessageListener<Integer>() {
            @Override
            public void onMessage(Message<Integer> message) {
                listenerLatch.countDown();
            }
        });

        // restart the server
        server.getLifecycleService().terminate();

        hazelcastFactory.newHazelcastInstance();

        // publish some data
        topic2.publish(5);

        assertOpenEventually(listenerLatch);
    }

    @Test
    public void shouldContinue_OnClusterRestart_afterInvocationTimeout() throws InterruptedException {
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setFailOnMaxBackoff(false);
        int invocationTimeoutSeconds = 2;
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), String.valueOf(invocationTimeoutSeconds));
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final CountDownLatch messageArrived = new CountDownLatch(1);
        String topicName = "topic";
        ITopic<String> topic = client.getReliableTopic(topicName);
        UUID registrationId = topic.addMessageListener(new DurableSubscriptionTest.DurableMessageListener<String>() {
            @Override
            public void onMessage(Message<String> message) {
                messageArrived.countDown();
            }

            @Override
            public boolean isLossTolerant() {
                return true;
            }
        });

        member.shutdown();
        // wait for the topic operation to timeout
        Thread.sleep(TimeUnit.SECONDS.toMillis(invocationTimeoutSeconds));

        member = hazelcastFactory.newHazelcastInstance();
        member.getReliableTopic(topicName).publish("message");
        assertOpenEventually(messageArrived);

        ClientReliableTopicProxy proxy = (ClientReliableTopicProxy) topic;
        assertFalse(proxy.isListenerCancelled(registrationId));
    }


    @Test
    public void shouldContinue_OnClusterRestart_whenDataLoss_LossTolerant_afterInvocationTimeout() throws InterruptedException {
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setFailOnMaxBackoff(false);
        int invocationTimeoutSeconds = 2;
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), String.valueOf(invocationTimeoutSeconds));
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final AtomicLong messageCount = new AtomicLong();
        final CountDownLatch messageArrived = new CountDownLatch(1);
        String topicName = "topic";

        member.getReliableTopic(topicName).publish("message");
        member.getReliableTopic(topicName).publish("message");

        final ITopic<String> topic = client.getReliableTopic(topicName);
        final UUID registrationId = topic.addMessageListener(new DurableSubscriptionTest.DurableMessageListener<String>() {
            @Override
            public void onMessage(Message<String> message) {
                messageCount.incrementAndGet();
                messageArrived.countDown();
            }

            @Override
            public boolean isLossTolerant() {
                return true;
            }
        });

        member.shutdown();
        // wait for the topic operation to timeout
        Thread.sleep(TimeUnit.SECONDS.toMillis(invocationTimeoutSeconds));

        member = hazelcastFactory.newHazelcastInstance();
        member.getReliableTopic(topicName).publish("message");

        assertOpenEventually(messageArrived);

        ClientReliableTopicProxy proxy = (ClientReliableTopicProxy) topic;
        assertFalse(proxy.isListenerCancelled(registrationId));
        assertEquals(1, messageCount.get());
    }

    @Test
    public void shouldFail_OnClusterRestart_whenDataLoss_notLossTolerant() {
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setFailOnMaxBackoff(false);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final AtomicLong messageCount = new AtomicLong();
        String topicName = "topic";

        member.getReliableTopic(topicName).publish("message");
        member.getReliableTopic(topicName).publish("message");

        final ITopic<String> topic = client.getReliableTopic(topicName);
        final UUID registrationId = topic.addMessageListener(new DurableSubscriptionTest.DurableMessageListener<String>() {
            @Override
            public void onMessage(Message<String> message) {
                messageCount.incrementAndGet();
            }

            @Override
            public boolean isLossTolerant() {
                return false;
            }
        });

        member.shutdown();

        hazelcastFactory.newHazelcastInstance();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                ClientReliableTopicProxy proxy = (ClientReliableTopicProxy) topic;
                assertTrue(proxy.isListenerCancelled(registrationId));
            }
        });
        assertEquals(0, messageCount.get());
    }
}
