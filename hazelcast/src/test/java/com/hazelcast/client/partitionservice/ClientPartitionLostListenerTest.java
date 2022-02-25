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

package com.hazelcast.client.partitionservice;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.PartitionLostEventImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.partition.PartitionLostListenerStressTest.EventCollectingPartitionLostListener;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.internal.partition.InternalPartitionService.PARTITION_LOST_EVENT_TOPIC;
import static com.hazelcast.internal.partition.InternalPartitionService.SERVICE_NAME;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientPartitionLostListenerTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void test_partitionLostListener_registeredByConfig() {
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig().addListenerConfig(new ListenerConfig(mock(PartitionLostListener.class)));
        hazelcastFactory.newHazelcastClient(clientConfig);

        assertRegistrationsSizeEventually(instance, 4);
    }

    @Test
    public void test_partitionLostListener_registered() {
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        client.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));
        // Expected = 4 -> 1 added & 1 from {@link com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 4);
    }

    @Test
    public void test_partitionLostListener_removed() {
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        final UUID registrationId = client.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));
        // Expected = 4 -> 1 added & 1 from {@link com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 4);

        client.getPartitionService().removePartitionLostListener(registrationId);
        // Expected = 3 -> see {@link com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 3);
    }

    @Test
    public void test_partitionLostListener_registeredByConfig_invoked() {
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();
        final ClientConfig clientConfig = new ClientConfig().addListenerConfig(new ListenerConfig(listener));
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        warmUpPartitions(instance, client);

        // Expected = 4 -> 1 added & 1 from {@link com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 4);

        final InternalPartitionServiceImpl partitionService = getNode(instance).getNodeEngine().getService(SERVICE_NAME);
        final int partitionId = 5;
        Address address = instance.getCluster().getLocalMember().getAddress();
        partitionService.onPartitionLost(new PartitionLostEventImpl(partitionId, 0, address));

        assertPartitionLostEventEventually(listener, partitionId);
    }

    @Test
    public void test_partitionLostListener_invoked() {
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        warmUpPartitions(instance, client);

        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();

        client.getPartitionService().addPartitionLostListener(listener);
        // Expected = 4 -> 1 added & 1 from {@link com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 4);

        final InternalPartitionServiceImpl partitionService = getNode(instance).getNodeEngine().getService(SERVICE_NAME);
        final int partitionId = 5;
        Address address = instance.getCluster().getLocalMember().getAddress();
        partitionService.onPartitionLost(new PartitionLostEventImpl(partitionId, 0, address));

        assertPartitionLostEventEventually(listener, partitionId);
    }

    @Test
    public void test_partitionLostListener_invoked_fromOtherNode() {
        final HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        final HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        warmUpPartitions(instance1, instance2, client);

        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();
        client.getPartitionService().addPartitionLostListener(listener);
        // Expected = 2 -> 1 added & 1 from {@link com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers * instances
        assertRegistrationsSizeEventually(instance1, 7);
        assertRegistrationsSizeEventually(instance2, 7);

        final InternalPartitionServiceImpl partitionService = getNode(instance2).getNodeEngine().getService(SERVICE_NAME);
        final int partitionId = 5;
        Address address = instance1.getCluster().getLocalMember().getAddress();
        partitionService.onPartitionLost(new PartitionLostEventImpl(partitionId, 0, address));

        assertPartitionLostEventEventually(listener, partitionId);
    }

    private void assertRegistrationsSizeEventually(final HazelcastInstance instance, final int size) {
        assertTrueEventually(() -> {
            final EventService eventService = getNode(instance).getNodeEngine().getEventService();
            final Collection<EventRegistration> registrations = eventService
                    .getRegistrations(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC);
            assertEquals(size, registrations.size());
        });
    }

    private void assertPartitionLostEventEventually(final EventCollectingPartitionLostListener listener, final int partitionId) {
        assertTrueEventually(() -> {
            final List<PartitionLostEvent> events = listener.getEvents();
            assertFalse(events.isEmpty());
            assertEquals(partitionId, events.get(0).getPartitionId());

        });
    }

}
