/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.eventservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EventServiceTest extends HazelcastTestSupport {

    private final String serviceName = "dummy-service";
    private final String topic = "dummy-topic";

    @Test
    public void test_registration_whileNewMemberJoining() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(newConfigWithDummyService());
        HazelcastInstance hz2 = factory.newHazelcastInstance(newConfigWithDummyService());

        Future<HazelcastInstance> future = spawn(new Callable<HazelcastInstance>() {
            @Override
            public HazelcastInstance call() throws Exception {
                return factory.newHazelcastInstance(newConfigWithDummyService());
            }
        });

        InternalEventService eventService = getEventService(hz2);
        Set<String> registrationIds = new HashSet<String>();
        Object listener = new Object();
        while (getClusterService(hz2).getSize() < 3) {
            EventRegistration registration = eventService.registerListener(serviceName, topic, listener);
            registrationIds.add(registration.getId());
        }

        HazelcastInstance hz3 = future.get();
        InternalEventService eventService3 = getEventService(hz3);
        Collection<EventRegistration> registrations = eventService3.getRegistrations(serviceName, topic);

        assertEquals(registrationIds.size(), registrations.size());
        for (EventRegistration registration : registrations) {
            assertThat(registrationIds, hasItem(registration.getId()));
        }
    }

    @Test
    public void test_deregistration_whileNewMemberJoining() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(newConfigWithDummyService());
        HazelcastInstance hz2 = factory.newHazelcastInstance(newConfigWithDummyService());

        InternalEventService eventService = getEventService(hz2);
        Set<String> registrationIds = new HashSet<String>();
        Object listener = new Object();
        for (int i = 0; i < 500; i++) {
            EventRegistration registration = eventService.registerListener(serviceName, topic, listener);
            registrationIds.add(registration.getId());
        }

        Future<HazelcastInstance> future = spawn(new Callable<HazelcastInstance>() {
            @Override
            public HazelcastInstance call() throws Exception {
                return factory.newHazelcastInstance(newConfigWithDummyService());
            }
        });

        for (String registrationId : registrationIds) {
            eventService.deregisterListener(serviceName, topic, registrationId);
        }

        assertThat(eventService.getRegistrations(serviceName, topic), Matchers.<EventRegistration>empty());

        HazelcastInstance hz3 = future.get();
        InternalEventService eventService3 = getEventService(hz3);
        assertThat(eventService3.getRegistrations(serviceName, topic), Matchers.<EventRegistration>empty());
    }

    private Config newConfigWithDummyService() {
        final Config config = new Config();
        ServiceConfig serviceConfig =
                new ServiceConfig().setEnabled(true).setName(serviceName).setImplementation(new Object());
        config.getServicesConfig().addServiceConfig(serviceConfig);
        return config;
    }

    private static InternalEventService getEventService(HazelcastInstance hz) {
        return getNodeEngineImpl(hz).getEventService();
    }
}
