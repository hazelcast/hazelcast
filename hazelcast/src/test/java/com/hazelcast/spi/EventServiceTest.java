/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.impl.TopicEvent;
import com.hazelcast.topic.impl.TopicService;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class EventServiceTest extends HazelcastTestSupport {

    @Test(timeout = 90000)
    public void testEventService() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);

        HazelcastInstanceProxy h1 = (HazelcastInstanceProxy) factory.newHazelcastInstance();
        HazelcastInstanceProxy h2 = (HazelcastInstanceProxy) factory.newHazelcastInstance();
        HazelcastInstanceProxy h3 = (HazelcastInstanceProxy) factory.newHazelcastInstance();

        CountDownLatch l1 = new CountDownLatch(1000);
        CountDownLatch l2 = new CountDownLatch(1000);
        CountDownLatch l3 = new CountDownLatch(1000);

        ITopic t1 = h1.getTopic("foo");
        ITopic t2 = h2.getTopic("foo");
        ITopic t3 = h3.getTopic("foo");

        t1.addMessageListener(createMessageListener(l1));
        t2.addMessageListener(createMessageListener(l2));
        t3.addMessageListener(createMessageListener(l3));

        MemberImpl m1 = (MemberImpl) h1.getCluster().getLocalMember();
        MemberImpl m2 = (MemberImpl) h2.getCluster().getLocalMember();
        MemberImpl m3 = (MemberImpl) h3.getCluster().getLocalMember();

        Address a1 = m1.getAddress();
        Address a2 = m2.getAddress();
        Address a3 = m3.getAddress();

        Field original = HazelcastInstanceProxy.class.getDeclaredField("original");
        original.setAccessible(true);

        HazelcastInstanceImpl impl1 = (HazelcastInstanceImpl) original.get(h1);
        HazelcastInstanceImpl impl2 = (HazelcastInstanceImpl) original.get(h2);
        HazelcastInstanceImpl impl3 = (HazelcastInstanceImpl) original.get(h3);

        EventService es1 = impl1.node.nodeEngine.getEventService();
        EventService es2 = impl2.node.nodeEngine.getEventService();
        EventService es3 = impl3.node.nodeEngine.getEventService();

        SerializationService ss1 = impl1.node.nodeEngine.getSerializationService();
        SerializationService ss2 = impl2.node.nodeEngine.getSerializationService();
        SerializationService ss3 = impl3.node.nodeEngine.getSerializationService();

        int counter = 0;
        for (int i = 0; i < 3000; i++) {
            if (counter == 0) {
                TopicEvent event = builTopicEvent("Foo" + i, m1, ss1);
                Collection<EventRegistration> registrations = es1.getRegistrations(TopicService.SERVICE_NAME, "foo");
                EventRegistration registration = findEventRegistration(a3, registrations);
                es1.publishEvent(TopicService.SERVICE_NAME, registration, event, 0);
            } else if (counter == 1) {
                TopicEvent event = builTopicEvent("Foo" + i, m2, ss2);
                Collection<EventRegistration> registrations = es2.getRegistrations(TopicService.SERVICE_NAME, "foo");
                EventRegistration registration = findEventRegistration(a1, registrations);
                es2.publishEvent(TopicService.SERVICE_NAME, registration, event, 0);
            } else if (counter == 2) {
                TopicEvent event = builTopicEvent("Foo" + i, m3, ss3);
                Collection<EventRegistration> registrations = es3.getRegistrations(TopicService.SERVICE_NAME, "foo");
                EventRegistration registration = findEventRegistration(a2, registrations);
                es3.publishEvent(TopicService.SERVICE_NAME, registration, event, 0);
            }
            counter++;
            if (counter == 3) counter = 0;
        }

        l1.await(30, TimeUnit.SECONDS);
        l2.await(30, TimeUnit.SECONDS);
        l3.await(30, TimeUnit.SECONDS);
    }

    private TopicEvent builTopicEvent(String value, MemberImpl member, SerializationService ss) {
        return new TopicEvent("foo", ss.toData(value), member.getAddress());
    }

    private EventRegistration findEventRegistration(Address address, Collection<EventRegistration> registrations) {
        for (EventRegistration registration : registrations) {
            if (registration.getSubscriber().equals(address)) {
                return registration;
            }
        }
        return null;
    }

    private MessageListener createMessageListener(final CountDownLatch latch) {
        return new MessageListener() {
            @Override
            public void onMessage(Message message) {
                latch.countDown();
            }
        };
    }

}
