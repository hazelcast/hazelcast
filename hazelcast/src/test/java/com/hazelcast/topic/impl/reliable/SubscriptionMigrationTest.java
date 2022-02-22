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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.ReplicaMigrationEvent;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastParallelClassRunner.class)
public class SubscriptionMigrationTest extends HazelcastTestSupport {

    @Rule
    public OverridePropertyRule overridePropertyRule = OverridePropertyRule.set("hazelcast.partition.count", "2");

    // gh issue: https://github.com/hazelcast/hazelcast/issues/13602
    @Test
    public void testListenerReceivesMessagesAfterPartitionIsMigratedBack() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance instance1 = factory.newHazelcastInstance();

        final String rtNameOnPartition0 = generateReliableTopicNameForPartition(instance1, 0);
        final String rtNameOnPartition1 = generateReliableTopicNameForPartition(instance1, 1);

        ITopic<String> topic0 = instance1.getReliableTopic(rtNameOnPartition0);
        ITopic<String> topic1 = instance1.getReliableTopic(rtNameOnPartition1);

        final CountingMigrationListener migrationListener = new CountingMigrationListener();
        instance1.getPartitionService().addMigrationListener(migrationListener);

        final PayloadMessageListener<String> listener0 = new PayloadMessageListener<String>();
        final PayloadMessageListener<String> listener1 = new PayloadMessageListener<String>();

        topic0.addMessageListener(listener0);
        topic1.addMessageListener(listener1);

        topic0.publish("itemA");
        topic1.publish("item1");

        HazelcastInstance instance2 = factory.newHazelcastInstance();

        // 1 primary, 1 backup migration
        assertEqualsEventually(2, migrationListener.partitionMigrationCount);

        instance2.shutdown();

        assertEqualsEventually(3, migrationListener.partitionMigrationCount);

        topic0.publish("itemB");
        topic1.publish("item2");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(listener0.isReceived("itemA"));
                assertTrue(listener0.isReceived("itemB"));
                assertTrue(listener1.isReceived("item1"));
                assertTrue(listener1.isReceived("item2"));
            }
        });
    }

    public class PayloadMessageListener<V> implements MessageListener<V> {

        private Collection<V> receivedMessages = new HashSet<V>();

        @Override
        public void onMessage(Message<V> message) {
            receivedMessages.add(message.getMessageObject());
        }

        boolean isReceived(V message) {
            return receivedMessages.contains(message);
        }
    }

    public class CountingMigrationListener implements MigrationListener {

        AtomicInteger partitionMigrationCount = new AtomicInteger();

        @Override
        public void migrationStarted(MigrationState state) {
        }

        @Override
        public void migrationFinished(MigrationState state) {
        }

        @Override
        public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
            partitionMigrationCount.incrementAndGet();
        }

        @Override
        public void replicaMigrationFailed(ReplicaMigrationEvent event) {
        }
    }

    private String generateReliableTopicNameForPartition(HazelcastInstance instance, int partitionId) {
        return generateKeyForPartition(instance, RingbufferService.TOPIC_RB_PREFIX, partitionId);
    }

}
