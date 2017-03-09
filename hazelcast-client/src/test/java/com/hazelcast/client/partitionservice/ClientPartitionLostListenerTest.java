/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.partition.PartitionLostListenerStressTest.EventCollectingPartitionLostListener;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.impl.PortablePartitionLostEvent;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.spi.partition.IPartitionLostEvent;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.client.impl.ClientTestUtil.getHazelcastClientInstanceImpl;
import static com.hazelcast.internal.partition.InternalPartitionService.PARTITION_LOST_EVENT_TOPIC;
import static com.hazelcast.internal.partition.InternalPartitionService.SERVICE_NAME;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientPartitionLostListenerTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void test_partitionLostListener_registered() {
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        client.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));

        assertRegistrationsSizeEventually(instance, 1);
    }

    @Test
    public void test_partitionLostListener_removed() {
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        final String registrationId = client.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));
        assertRegistrationsSizeEventually(instance, 1);

        client.getPartitionService().removePartitionLostListener(registrationId);
        assertRegistrationsSizeEventually(instance, 0);
    }

    @Test
    public void test_partitionLostListener_invoked() {
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        warmUpPartitions(instance, client);

        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();

        client.getPartitionService().addPartitionLostListener(listener);

        assertRegistrationsSizeEventually(instance, 1);

        final InternalPartitionServiceImpl partitionService = getNode(instance).getNodeEngine().getService(SERVICE_NAME);
        final int partitionId = 5;
        partitionService.onPartitionLost(new IPartitionLostEvent(partitionId, 0, null));

        assertPartitionLostEventEventually(listener, partitionId);
    }

    @Test
    public void test_partitionLostListener_invoked_fromOtherNode() {
        final HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        warmUpPartitions(instance1, instance2, client);

        final HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        final Address clientOwnerAddress = clientInstanceImpl.getClientClusterService().getOwnerConnectionAddress();

        final HazelcastInstance other = getAddress(instance1).equals(clientOwnerAddress) ? instance2 : instance1;

        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();
        client.getPartitionService().addPartitionLostListener(listener);

        assertRegistrationsSizeEventually(instance1, 1);
        assertRegistrationsSizeEventually(instance2, 1);

        final InternalPartitionServiceImpl partitionService = getNode(other).getNodeEngine().getService(SERVICE_NAME);
        final int partitionId = 5;
        partitionService.onPartitionLost(new IPartitionLostEvent(partitionId, 0, null));

        assertPartitionLostEventEventually(listener, partitionId);
    }

    private void assertRegistrationsSizeEventually(final HazelcastInstance instance, final int size) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {

                final InternalEventService eventService = getNode(instance).getNodeEngine().getEventService();
                final Collection<EventRegistration> registrations = eventService
                        .getRegistrations(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC);
                assertEquals(size, registrations.size());

            }
        });
    }

    private void assertPartitionLostEventEventually(final EventCollectingPartitionLostListener listener, final int partitionId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {

                final List<PartitionLostEvent> events = listener.getEvents();
                assertFalse(events.isEmpty());
                assertEquals(partitionId, events.get(0).getPartitionId());

            }
        });
    }

    @Test
    public void test_portableMapPartitionLostEvent_serialization()
            throws IOException {
        final Address source = new Address();
        final PortablePartitionLostEvent event = new PortablePartitionLostEvent(1, 2, source);

        final PortableWriter writer = mock(PortableWriter.class);
        final ObjectDataOutput output = mock(ObjectDataOutput.class);
        when(writer.getRawDataOutput()).thenReturn(output);

        event.writePortable(writer);

        verify(writer).writeInt("p", 1);
        verify(writer).writeInt("l", 2);
        verify(output).writeObject(source);
    }

    @Test
    public void test_portableMapPartitionLostEvent_deserialization()
            throws IOException {
        final Address source = new Address();
        final PortablePartitionLostEvent event = new PortablePartitionLostEvent();

        final PortableReader reader = mock(PortableReader.class);
        final ObjectDataInput input = mock(ObjectDataInput.class);

        when(reader.getRawDataInput()).thenReturn(input);
        when(reader.readInt("p")).thenReturn(1);
        when(reader.readInt("l")).thenReturn(2);
        when(input.readObject()).thenReturn(source);

        event.readPortable(reader);
        assertEquals(1, event.getPartitionId());
        assertEquals(2, event.getLostBackupCount());
        assertEquals(source, event.getSource());
    }
}
