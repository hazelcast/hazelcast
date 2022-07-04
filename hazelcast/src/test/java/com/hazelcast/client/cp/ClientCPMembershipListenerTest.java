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
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.CPMembershipListenerTest.CountingDownCPMembershipListener;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCPMembershipListenerTest extends HazelcastRaftTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return factory;
    }

    @Test
    public void whenRegisteredInConfig_thenReceiveEvents() {
        HazelcastInstance[] instances = newInstances(3);

        CountingDownCPMembershipListener listener = new CountingDownCPMembershipListener(0, 1);
        ClientConfig config = new ClientConfig();
        config.addListenerConfig(new ListenerConfig().setImplementation(listener));
        factory.newHazelcastClient(config);

        instances[2].shutdown();
        assertOpenEventually(listener.removedLatch);
    }

    @Test
    public void whenMemberShutdown_thenReceiveMemberRemovedEvent() {
        HazelcastInstance[] instances = newInstances(3);

        HazelcastInstance client = factory.newHazelcastClient();
        CountingDownCPMembershipListener listener = new CountingDownCPMembershipListener(0, 1);
        client.getCPSubsystem().addMembershipListener(listener);

        HazelcastInstance instance = instances[instances.length - 1];
        CPMember member = instance.getCPSubsystem().getLocalCPMember();
        instance.shutdown();

        assertOpenEventually(listener.removedLatch);
        assertEquals(member, listener.removedMembers.get(0));
    }

    @Test
    public void whenMemberTerminated_thenReceiveMemberRemovedEvent() {
        Config config = createConfig(3, 3);
        config.getCPSubsystemConfig().setSessionTimeToLiveSeconds(2).setSessionHeartbeatIntervalSeconds(1)
                .setMissingCPMemberAutoRemovalSeconds(3);

        HazelcastInstance[] instances = factory.newInstances(config, 3);
        waitUntilCPDiscoveryCompleted(instances);

        HazelcastInstance client = factory.newHazelcastClient();
        CountingDownCPMembershipListener listener = new CountingDownCPMembershipListener(0, 1);
        client.getCPSubsystem().addMembershipListener(listener);

        HazelcastInstance instance = instances[instances.length - 1];
        CPMember member = instance.getCPSubsystem().getLocalCPMember();
        instance.getLifecycleService().terminate();

        assertOpenEventually(listener.removedLatch);
        assertEquals(member, listener.removedMembers.get(0));
    }

    @Test
    public void whenMemberPromoted_thenReceiveMemberAddedEvent() {
        HazelcastInstance[] instances = newInstances(3, 3, 1);

        HazelcastInstance client = factory.newHazelcastClient();
        CountingDownCPMembershipListener listener = new CountingDownCPMembershipListener(1, 0);
        client.getCPSubsystem().addMembershipListener(listener);

        HazelcastInstance instance = instances[instances.length - 1];
        instance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember().toCompletableFuture().join();

        assertOpenEventually(listener.addedLatch);
        assertEquals(instance.getCPSubsystem().getLocalCPMember(), listener.addedMembers.get(0));
    }

    @Test
    public void whenListenerDeregistered_thenNoEventsReceived() {
        CountingDownCPMembershipListener listener = new CountingDownCPMembershipListener(1, 1);

        HazelcastInstance[] instances = newInstances(5);
        HazelcastInstance client = factory.newHazelcastClient();
        UUID id = client.getCPSubsystem().addMembershipListener(listener);

        instances[0].shutdown();
        assertTrueEventually(() -> assertEquals(1, listener.removedMembers.size()));

        assertTrue(client.getCPSubsystem().removeMembershipListener(id));

        instances[2].shutdown();
        assertTrueAllTheTime(() -> assertEquals(1, listener.removedMembers.size()), 3);
    }

    @Test
    public void whenMultipleListenersRegistered_thenAllReceiveEvents() {
        HazelcastInstance[] instances = newInstances(3);
        HazelcastInstance client = factory.newHazelcastClient();

        CountingDownCPMembershipListener listener1 = new CountingDownCPMembershipListener(0, 1);
        CountingDownCPMembershipListener listener2 = new CountingDownCPMembershipListener(0, 1);
        client.getCPSubsystem().addMembershipListener(listener1);
        client.getCPSubsystem().addMembershipListener(listener2);

        HazelcastInstance instance = instances[instances.length - 1];
        CPMember member = instance.getCPSubsystem().getLocalCPMember();
        instance.shutdown();

        assertOpenEventually(listener1.removedLatch);
        assertOpenEventually(listener2.removedLatch);
        assertEquals(member, listener1.removedMembers.get(0));
        assertEquals(member, listener2.removedMembers.get(0));
    }
}
