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

package com.hazelcast.topic;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.impl.TopicService;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TopicDestroyTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private ITopic<Object> topic;
    private String topicName;

    @Before
    public void setup() {
        instance = createHazelcastInstance();
        topicName = randomString();
        topic = instance.getTopic(topicName);
    }

    @Test
    public void testDestroyTopicRemovesListeners() {
        topic.addMessageListener(new EmptyListener());
        topic.destroy();

        assertRegistrationSize(0);
    }

    @Test
    public void testRemovingListenersRemovesRegistrations() {
        UUID registrationId = topic.addMessageListener(new EmptyListener());
        topic.removeMessageListener(registrationId);

        assertRegistrationSize(0);
    }

    void assertRegistrationSize(int size) {
        EventService eventService = getNode(instance).getNodeEngine().getEventService();
        Collection<EventRegistration> regs = eventService.getRegistrations(TopicService.SERVICE_NAME, topicName);

        assertEquals(size, regs.size());
    }

    static class EmptyListener implements MessageListener<Object> {
        @Override
        public void onMessage(Message message) {
        }
    }
}
