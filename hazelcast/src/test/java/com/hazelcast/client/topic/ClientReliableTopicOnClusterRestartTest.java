/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.impl.reliable.DurableSubscriptionTest.DurableMessageListener;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(smallInstanceConfig());
        String topicName = "topic";
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance hazelcastClient = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastInstance hazelcastClient2 = hazelcastFactory.newHazelcastClient(clientConfig);
        ITopic<Integer> topic = hazelcastClient.getReliableTopic(topicName);
        ITopic<Integer> topic2 = hazelcastClient2.getReliableTopic(topicName);

        CountDownLatch listenerLatch = new CountDownLatch(1);

        // Add listener using the first client
        topic.addMessageListener(message -> listenerLatch.countDown());

        // restart the server
        server.getLifecycleService().terminate();

        hazelcastFactory.newHazelcastInstance(smallInstanceConfig());

        // publish some data
        topic2.publish(5);

        assertOpenEventually(listenerLatch);
    }

    @Test
    public void shouldContinue_OnClusterRestart_afterInvocationTimeout() throws InterruptedException {
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(smallInstanceConfig());
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        int invocationTimeoutSeconds = 2;
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), String.valueOf(invocationTimeoutSeconds));
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final CountDownLatch messageArrived = new CountDownLatch(1);
        String topicName = "topic";
        ITopic<String> topic = client.getReliableTopic(topicName);
        UUID registrationId = topic.addMessageListener(createListener(true, m -> messageArrived.countDown()));

        member.shutdown();
        // wait for the topic operation to timeout
        Thread.sleep(TimeUnit.SECONDS.toMillis(invocationTimeoutSeconds));

        member = hazelcastFactory.newHazelcastInstance(smallInstanceConfig());
        member.getReliableTopic(topicName).publish("message");
        assertOpenEventually(messageArrived);

        ClientReliableTopicProxy<?> proxy = (ClientReliableTopicProxy<?>) topic;
        assertFalse(proxy.isListenerCancelled(registrationId));
    }


    @Test
    public void shouldContinue_OnClusterRestart_whenDataLoss_LossTolerant_afterInvocationTimeout() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        int invocationTimeoutSeconds = 2;
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), String.valueOf(invocationTimeoutSeconds));

        final HazelcastInstance member = hazelcastFactory.newHazelcastInstance(smallInstanceConfig());
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        AtomicLong messageCount = new AtomicLong();
        CountDownLatch messageArrived = new CountDownLatch(1);
        String topicName = "topic";

        member.getReliableTopic(topicName).publish("message");
        member.getReliableTopic(topicName).publish("message");

        ClientReliableTopicProxy<?> topic = (ClientReliableTopicProxy<?>) client.getReliableTopic(topicName);
        UUID registrationId = topic.addMessageListener(createListener(true, m -> {
            messageCount.incrementAndGet();
            messageArrived.countDown();
        }));

        member.shutdown();

        final HazelcastInstance restartedMember = hazelcastFactory.newHazelcastInstance(smallInstanceConfig());

        // wait some time for subscription
        Thread.sleep(TimeUnit.SECONDS.toMillis(invocationTimeoutSeconds));

        assertTrueEventually(() -> {
            String item = "newItem " + UUID.randomUUID();
            restartedMember.getReliableTopic(topicName).publish(item);
            assertOpenEventually(messageArrived, 5);
        });

        assertFalse(topic.isListenerCancelled(registrationId));
        assertTrue(messageCount.get() >= 1);
    }

    @Test
    public void shouldFail_OnClusterRestart_whenDataLoss_notLossTolerant() {
        Config config = smallInstanceConfig();
        String topicName = "topic";
        config.getRingbufferConfig(topicName)
              .setCapacity(10);

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig()
                    .getConnectionRetryConfig()
                    .setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        AtomicLong messageCount = new AtomicLong();

        // initialises listener seq up to 10000
        for (int i = 0; i < 10_000; i++) {
            member.getReliableTopic(topicName).publish("message");
        }

        ITopic<String> topic = client.getReliableTopic(topicName);
        UUID registrationId = topic.addMessageListener(createListener(false, m -> messageCount.incrementAndGet()));

        member.shutdown();
        member = hazelcastFactory.newHazelcastInstance(config);

        HazelcastInstance finalMember = member;
        assertTrueEventually(() -> {
            // we require at least one new message to detect that the ringbuffer was recreated
            finalMember.getReliableTopic(topicName).publish("message");
            ClientReliableTopicProxy<?> proxy = (ClientReliableTopicProxy<?>) topic;
            assertTrue(proxy.isListenerCancelled(registrationId));
        });
        assertEquals(0, messageCount.get());
    }

    private <T> DurableMessageListener<T> createListener(boolean lossTolerant,
                                                         Consumer<Message<T>> messageListener) {
        return new DurableMessageListener<>() {
            @Override
            public void onMessage(Message<T> message) {
                messageListener.accept(message);
            }

            @Override
            public boolean isLossTolerant() {
                return lossTolerant;
            }
        };
    }
}
