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
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ITopic;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.ringbuffer.impl.RingbufferService.TOPIC_RB_PREFIX;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReliableTopicDestroyTest extends HazelcastTestSupport {

    public static final String RELIABLE_TOPIC_NAME = "foo";
    private ITopic<String> topic;
    private HazelcastInstance member;

    @Before
    public void setup() {
        createInstances();
        this.topic = getDriver().getReliableTopic(RELIABLE_TOPIC_NAME);
    }

    @Test
    public void whenDestroyedThenListenersTerminate() {
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        topic.addMessageListener(listener);

        sleepSeconds(4);

        topic.destroy();

        System.out.println("Destroyed; now a bit of waiting");
        sleepSeconds(4);

        listener.clean();

        topic.publish("foo");

        // it should not receive any events.
        assertTrueDelayed5sec(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, listener.objects.size());
            }
        });
    }

    @Test
    public void whenDestroyedThenRingbufferRemoved() {
        topic.publish("foo");
        topic.destroy();
        final RingbufferService ringbufferService
                = getNodeEngineImpl(getMember()).getService(RingbufferService.SERVICE_NAME);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final String name = TOPIC_RB_PREFIX + RELIABLE_TOPIC_NAME;
                final Map<ObjectNamespace, RingbufferContainer> partitionContainers =
                        ringbufferService.getContainers().get(ringbufferService.getRingbufferPartitionId(name));
                assertNotNull(partitionContainers);
                assertFalse(partitionContainers.containsKey(RingbufferService.getRingbufferNamespace(name)));
            }
        });
    }

    protected void createInstances() {
        this.member = createHazelcastInstance();
    }

    protected HazelcastInstance getDriver() {
        return member;
    }

    protected HazelcastInstance getMember() {
        return member;
    }
}
