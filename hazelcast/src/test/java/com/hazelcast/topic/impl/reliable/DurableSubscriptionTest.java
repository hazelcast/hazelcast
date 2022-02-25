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
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ReliableMessageListener;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DurableSubscriptionTest extends HazelcastTestSupport {

    @Test
    public void testDurableSubscription() {
        HazelcastInstance local = createHazelcastInstance();

        ITopic<String> topic = local.getReliableTopic("topic");
        final DurableMessageListener<String> listener = new DurableMessageListener<String>();

        UUID id = topic.addMessageListener(listener);
        topic.publish("item1");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertContains(listener.objects, "item1");
            }
        });

        topic.removeMessageListener(id);

        // TODO: this part is still racy because we don't know when the listener is terminated
        topic.publish("item2");
        topic.publish("item3");

        topic.addMessageListener(listener);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(asList("item1", "item2", "item3"), listener.objects);
                assertEquals(asList(0L, 1L, 2L), listener.sequences);
            }
        });
    }

    @Test
    public void beginFromStart() {
    }

    public static class DurableMessageListener<V> implements ReliableMessageListener<V> {

        public final List<V> objects = new CopyOnWriteArrayList<V>();
        public final List<Long> sequences = new CopyOnWriteArrayList<Long>();
        public volatile long sequence = -1;

        @Override
        public long retrieveInitialSequence() {
            if (sequence == -1) {
                return -1;
            } else {
                // we need to add one because we want to read the next item.
                return sequence + 1;
            }
        }

        @Override
        public void storeSequence(long sequence) {
            sequences.add(sequence);
            this.sequence = sequence;
        }

        @Override
        public boolean isTerminal(Throwable failure) {
            return true;
        }

        @Override
        public boolean isLossTolerant() {
            return false;
        }

        @Override
        public void onMessage(Message<V> message) {
            objects.add(message.getMessageObject());
        }
    }
}
