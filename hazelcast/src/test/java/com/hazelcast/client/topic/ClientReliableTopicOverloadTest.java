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
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.proxy.ClientReliableTopicProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.topic.impl.reliable.TopicOverloadAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientReliableTopicOverloadTest extends TopicOverloadAbstractTest {

    private TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Before
    public void setupCluster() {
        Config config = new Config();
        config.addRingBufferConfig(new RingbufferConfig("when*")
                .setCapacity(100).setTimeToLiveSeconds(Integer.MAX_VALUE));
        hazelcastFactory.newHazelcastInstance(config);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addReliableTopicConfig(new ClientReliableTopicConfig("whenError_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.ERROR));
        clientConfig.addReliableTopicConfig(new ClientReliableTopicConfig("whenDiscardOldest_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST));
        clientConfig.addReliableTopicConfig(new ClientReliableTopicConfig("whenDiscardNewest_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_NEWEST));
        clientConfig.addReliableTopicConfig(new ClientReliableTopicConfig("whenBlock_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK));
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        serializationService = ((HazelcastClientProxy) client).getSerializationService();

        String topicName = getTestMethodName();
        topic = client.<String>getReliableTopic(topicName);

        ringbuffer = ((ClientReliableTopicProxy<String>) topic).getRingbuffer();
    }

    @After
    public void terminate() {
        hazelcastFactory.terminateAll();
    }


}
