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

package com.hazelcast.client.cp;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupAvailabilityListenerTest.CountingDownCPGroupAvailabilityListener;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCPGroupAvailabilityListenerTest extends HazelcastRaftTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return factory;
    }

    @Test
    public void whenRegisteredInConfig_thenReceiveEvents() {
        HazelcastInstance[] instances = newInstances(3);

        CountingDownCPGroupAvailabilityListener listener = new CountingDownCPGroupAvailabilityListener(1, 1);
        ClientConfig config = new ClientConfig();
        config.addListenerConfig(new ListenerConfig().setImplementation(listener));
        factory.newHazelcastClient(config);

        instances[2].getLifecycleService().terminate();
        assertOpenEventually(listener.availabilityLatch);
    }

    @Test
    public void whenGroupMemberTerminated_thenReceiveGroupAvailabilityEvents() throws Exception {
        CountingDownCPGroupAvailabilityListener listener = new CountingDownCPGroupAvailabilityListener(1, 1);

        HazelcastInstance[] instances = newInstances(3);
        HazelcastInstance client = factory.newHazelcastClient();
        client.getCPSubsystem().addGroupAvailabilityListener(listener);

        instances[0].getLifecycleService().terminate();
        assertOpenEventually(listener.availabilityLatch);
        assertFalse(listener.majorityLatch.await(1, TimeUnit.SECONDS));

        instances[2].getLifecycleService().terminate();
        assertOpenEventually(listener.majorityLatch);
    }

    @Test
    public void whenMemberTerminated_thenReceiveCpSubsystemAvailabilityEvents() throws Exception {
        CountingDownCPGroupAvailabilityListener listener = new CountingDownCPGroupAvailabilityListener(1, 1);

        HazelcastInstance[] instances = newInstances(5);
        HazelcastInstance client = factory.newHazelcastClient();
        client.getCPSubsystem().addGroupAvailabilityListener(listener);

        instances[0].getLifecycleService().terminate();
        assertOpenEventually(listener.availabilityLatch);
        assertEquals(1, listener.availabilityEventCount.get());

        instances[4].getLifecycleService().terminate();
        assertFalse(listener.majorityLatch.await(1, TimeUnit.SECONDS));
        assertEquals(2, listener.availabilityEventCount.get());

        instances[2].getLifecycleService().terminate();
        assertOpenEventually(listener.majorityLatch);
        assertEquals(1, listener.majorityEventCount.get());
    }

    @Test
    public void whenListenerDeregistered_thenNoEventsReceived() {
        CountingDownCPGroupAvailabilityListener listener = new CountingDownCPGroupAvailabilityListener(1, 1);

        HazelcastInstance[] instances = newInstances(3);
        HazelcastInstance client = factory.newHazelcastClient();
        UUID id = client.getCPSubsystem().addGroupAvailabilityListener(listener);

        instances[0].getLifecycleService().terminate();
        assertEqualsEventually(1, listener.availabilityEventCount);

        assertTrue(client.getCPSubsystem().removeGroupAvailabilityListener(id));

        instances[2].getLifecycleService().terminate();
        assertTrueAllTheTime(() -> assertEquals(0, listener.majorityEventCount.get()), 3);
    }

    @Test
    public void whenMultipleListenersRegistered_thenAllReceiveEvents() {
        HazelcastInstance[] instances = newInstances(3);
        HazelcastInstance client = factory.newHazelcastClient();

        CountingDownCPGroupAvailabilityListener listener1 = new CountingDownCPGroupAvailabilityListener(1, 1);
        client.getCPSubsystem().addGroupAvailabilityListener(listener1);
        CountingDownCPGroupAvailabilityListener listener2 = new CountingDownCPGroupAvailabilityListener(1, 1);
        client.getCPSubsystem().addGroupAvailabilityListener(listener2);

        instances[0].getLifecycleService().terminate();
        assertOpenEventually(listener1.availabilityLatch);
        assertOpenEventually(listener2.availabilityLatch);
        assertEquals(1, listener1.availabilityEventCount.get());
        assertEquals(1, listener2.availabilityEventCount.get());
    }
}
