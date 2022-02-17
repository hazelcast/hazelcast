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

package com.hazelcast.topic.impl.reliable;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.TopicOverloadPolicy;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static com.hazelcast.test.Accessors.getSerializationService;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TopicOverloadDistributedTest extends TopicOverloadAbstractTest {

    @Before
    public void setupCluster() {
        Config config = new Config();
        config.addRingBufferConfig(new RingbufferConfig("when*")
                .setCapacity(100).setTimeToLiveSeconds(Integer.MAX_VALUE));
        config.addReliableTopicConfig(new ReliableTopicConfig("whenError_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.ERROR));
        config.addReliableTopicConfig(new ReliableTopicConfig("whenDiscardOldest_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST));
        config.addReliableTopicConfig(new ReliableTopicConfig("whenDiscardNewest_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_NEWEST));
        config.addReliableTopicConfig(new ReliableTopicConfig("whenBlock_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK));

        HazelcastInstance[] hazelcastInstances = createHazelcastInstanceFactory(2).newInstances(config);
        HazelcastInstance hz = hazelcastInstances[0];
        warmUpPartitions(hazelcastInstances);
        serializationService = getSerializationService(hz);

        String topicName = getTestMethodName();
        topic = hz.<String>getReliableTopic(topicName);

        ringbuffer = ((ReliableTopicProxy<String>) topic).ringbuffer;
    }

}
