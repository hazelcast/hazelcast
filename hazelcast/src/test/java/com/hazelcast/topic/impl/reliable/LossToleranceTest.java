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

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.hazelcast.ringbuffer.impl.RingbufferService.TOPIC_RB_PREFIX;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LossToleranceTest extends HazelcastTestSupport {

    private static final String RELIABLE_TOPIC_NAME = "foo";
    private ReliableTopicProxy<String> topic;
    private Ringbuffer<ReliableTopicMessage> ringbuffer;
    private HazelcastInstance topicOwnerInstance;
    private HazelcastInstance topicBackupInstance;

    @Before
    public void setup() {
        Config config = smallInstanceConfig().addRingBufferConfig(
                new RingbufferConfig(RELIABLE_TOPIC_NAME)
                        .setCapacity(100)
                        .setTimeToLiveSeconds(0)
                        .setBackupCount(0)
                        .setAsyncBackupCount(0));
        final HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(config);

        warmUpPartitions(instances);
        for (HazelcastInstance instance : instances) {
            final Member owner = instance.getPartitionService().getPartition(TOPIC_RB_PREFIX + RELIABLE_TOPIC_NAME).getOwner();
            final Member localMember = instance.getCluster().getLocalMember();
            if (localMember.equals(owner)) {
                topicOwnerInstance = instance;
            } else {
                topicBackupInstance = instance;
            }
        }

        topic = (ReliableTopicProxy<String>) topicBackupInstance.<String>getReliableTopic(RELIABLE_TOPIC_NAME);
        ringbuffer = topic.ringbuffer;
    }

    @Test
    public void whenNotLossTolerant_thenTerminate() {
        ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        listener.initialSequence = 0;
        listener.isLossTolerant = false;

        topic.publish("foo");

        // we add so many items that the items the listener wants to listen to, doesn't exist anymore
        do {
            topic.publish("item");
        } while (ringbuffer.headSequence() <= listener.initialSequence);
        topic.addMessageListener(listener);

        assertTrueEventually(() -> assertTrue(topic.runnersMap.isEmpty()));
    }

    @Test
    public void whenLossTolerant_thenContinue() {
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        listener.initialSequence = 0;
        listener.isLossTolerant = true;

        // we add so many items that the items the listener wants to listen to, doesn't exist anymore
        do {
            topic.publish("item");
        } while (ringbuffer.headSequence() <= listener.initialSequence);

        topic.addMessageListener(listener);
        topic.publish("newItem");

        assertTrueEventually(() -> assertContains(listener.objects, "newItem"));
    }

    @Test
    public void whenLossTolerant_andOwnerCrashes_thenContinue() {
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        listener.isLossTolerant = true;
        topic.addMessageListener(listener);
        topic.publish("item1");
        topic.publish("item2");

        assertTrueEventually(() -> {
            assertContains(listener.objects, "item1");
            assertContains(listener.objects, "item2");
        });
        TestUtil.terminateInstance(topicOwnerInstance);

        assertTrueEventually(() -> {
            String item = "newItem " + UUID.randomUUID();
            topic.publish(item);
            assertTrueEventually(() -> assertContains(listener.objects, item), 5);
        });
    }
}
