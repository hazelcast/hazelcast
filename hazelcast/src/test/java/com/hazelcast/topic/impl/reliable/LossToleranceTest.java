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

package com.hazelcast.topic.impl.reliable;

import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LossToleranceTest extends HazelcastTestSupport {

    private String reliableTopicName;
    private ReliableTopicProxy<String> topic;
    private Ringbuffer<ReliableTopicMessage> ringbuffer;
    private HazelcastInstance topicOwnerInstance;

    @Before
    public void setup() {
        final HazelcastInstance[] instances = createHazelcastInstances(smallInstanceConfig(), 2);

        warmUpPartitions(instances);
        assertClusterSizeEventually(instances.length, instances);

        topicOwnerInstance = instances[0];
        reliableTopicName = randomNameOwnedBy(topicOwnerInstance, getClass().getName());

        // Update the configuration now we've generated a name
        instances[0].getConfig().addRingBufferConfig(
                new RingbufferConfig(reliableTopicName)
                        .setCapacity(100)
                        .setTimeToLiveSeconds(0)
                        .setBackupCount(0)
                        .setAsyncBackupCount(0));

        // Get the topic from the backup (i.e. non-owning) instance
        topic = (ReliableTopicProxy<String>) instances[1].<String>getReliableTopic(reliableTopicName);
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
