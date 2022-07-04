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
import com.hazelcast.client.config.ClientReliableTopicConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.TopicOverloadPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class ClientReliableTopicBlockTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;

    @Before
    public void setup() {
        Config config = smallInstanceConfig();
        config.addRingBufferConfig(new RingbufferConfig("blockingReliableTopic*")
                .setCapacity(10)
                .setTimeToLiveSeconds(60));
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig()
                .addReliableTopicConfig(new ClientReliableTopicConfig("blockingReliableTopic*")
                        .setReadBatchSize(10)
                        .setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK));

        client = hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @After
    public void teardown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testBlockingAsync() {
        String randomName = "blockingReliableTopic" + generateRandomString(5);
        AtomicInteger count = new AtomicInteger(0);
        ITopic<Object> topic = client.getReliableTopic(randomName);
        topic.addMessageListener(message -> count.incrementAndGet());
        for (int i = 0; i < 10; i++) {
            topic.publish("message");
        }
        assertTrueEventually(() -> assertEquals(10, count.get()));
        final List<String> data = Arrays.asList("msg 1", "msg 2", "msg 3", "msg 4", "msg 5");
        assertCompletesEventually(topic.publishAllAsync(data).toCompletableFuture());
        assertTrueEventually(() -> assertEquals(15, count.get()));
    }
}
