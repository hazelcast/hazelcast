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
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.MigrationStateImpl;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.PartitionMigrationListenerTest;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.partition.ReplicaMigrationEvent;
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
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.partition.IPartitionService.SERVICE_NAME;
import static com.hazelcast.internal.partition.InternalPartitionService.MIGRATION_EVENT_TOPIC;
import static com.hazelcast.partition.PartitionMigrationListenerTest.assertMigrationEventsConsistentWithResult;
import static com.hazelcast.partition.PartitionMigrationListenerTest.assertMigrationProcessCompleted;
import static com.hazelcast.partition.PartitionMigrationListenerTest.assertMigrationProcessEventsConsistent;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMigrationListenerTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testAddMigrationListener_whenRegisteredByConfig() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig().addListenerConfig(new ListenerConfig(mock(MigrationListener.class)));

        hazelcastFactory.newHazelcastClient(clientConfig);

        assertRegistrationsSizeEventually(instance, 1);
    }

    @Test
    public void testAddMigrationListener_whenRegisteredByPartitionService() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        client.getPartitionService().addMigrationListener(mock(MigrationListener.class));

        assertRegistrationsSizeEventually(instance, 1);
    }

    @Test
    public void testAddMigrationListener_whenListenerRegisteredTwice() {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        PartitionService partitionService = client.getPartitionService();
        MigrationListener listener = mock(MigrationListener.class);

        UUID id1 = partitionService.addMigrationListener(listener);
        UUID id2 = partitionService.addMigrationListener(listener);

        assertNotEquals(id1, id2);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveMigrationListener_whenNullListener() {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        PartitionService partitionService = client.getPartitionService();

        partitionService.removeMigrationListener(null);
    }

    @Test
    public void testRemoveMigrationListener_whenNonExistingRegistrationId() {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        PartitionService partitionService = client.getPartitionService();

        boolean result = partitionService.removeMigrationListener(UuidUtil.newUnsecureUUID());

        assertFalse(result);
    }

    @Test
    public void testRemoveMigrationListener_whenExistingRegistrationId() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        PartitionService clientPartitionService = client.getPartitionService();
        MigrationListener listener = mock(MigrationListener.class);

        UUID registrationId = clientPartitionService.addMigrationListener(listener);
        assertRegistrationsSizeEventually(instance, 1);

        boolean removed = clientPartitionService.removeMigrationListener(registrationId);
        assertRegistrationsSizeEventually(instance, 0);
        assertTrue(removed);

        HazelcastInstance hz2 = hazelcastFactory.newHazelcastInstance();
        warmUpPartitions(instance, hz2);

        verifyMigrationListenerNeverInvoked(listener);
    }

    @Test
    public void testMigrationListenerInvoked_whenRegisteredByConfig() {
        PartitionMigrationListenerTest.EventCollectingMigrationListener clientListener = eventCollectingMigrationListener();
        Function<MigrationListener, HazelcastInstance> clientSupplier = listener -> {
            ClientConfig clientConfig = new ClientConfig().addListenerConfig(new ListenerConfig(listener));
            return hazelcastFactory.newHazelcastClient(clientConfig);
        };

        testMigrationListenerInvoked(clientListener, clientSupplier, this::assertMigrationProcess);
    }

    @Test
    public void testMigrationListenerInvoked_whenRegisteredByPartitionService() {
        PartitionMigrationListenerTest.EventCollectingMigrationListener clientListener = eventCollectingMigrationListener();
        Function<MigrationListener, HazelcastInstance> clientSupplier = listener -> {
            HazelcastInstance client = hazelcastFactory.newHazelcastClient();
            client.getPartitionService().addMigrationListener(listener);
            return client;
        };

        testMigrationListenerInvoked(clientListener, clientSupplier, this::assertMigrationProcess);
    }

    @Test
    public void testAllMigrationListenerMethodsInvokedOnTheSameThread() {
        SingleThreadMigrationListener clientListener = new SingleThreadMigrationListener();
        Function<MigrationListener, HazelcastInstance> clientSupplier = listener -> {
            HazelcastInstance client = hazelcastFactory.newHazelcastClient();
            client.getPartitionService().addMigrationListener(listener);
            return client;
        };

        testMigrationListenerInvoked(clientListener, clientSupplier,
                SingleThreadMigrationListener::assertAllMethodsInvokedOnTheSameThread);
    }

    private <T extends MigrationListener> void testMigrationListenerInvoked(
            T clientListener,
            Function<MigrationListener, HazelcastInstance> clientFactory,
            Consumer<T> assertFunction) {

        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = clientFactory.apply(clientListener);
        warmUpPartitions(instance1, client);

        // Change to NO_MIGRATION to prevent repartitioning
        // before 2nd member started and ready.
        instance1.getCluster().changeClusterState(ClusterState.NO_MIGRATION);

        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();

        changeClusterStateEventually(instance2, ClusterState.ACTIVE);
        waitAllForSafeState(instance2, instance1, client);

        assertRegistrationsSizeEventually(instance1, 1);
        assertFunction.accept(clientListener);
    }

    private void verifyMigrationListenerNeverInvoked(MigrationListener listener) {
        verify(listener, never()).migrationStarted(any(MigrationStateImpl.class));
        verify(listener, never()).migrationFinished(any(MigrationStateImpl.class));
        verify(listener, never()).replicaMigrationCompleted(any(ReplicaMigrationEvent.class));
        verify(listener, never()).replicaMigrationFailed(any(ReplicaMigrationEvent.class));
    }

    private void assertMigrationProcess(PartitionMigrationListenerTest.EventCollectingMigrationListener listener) {
        PartitionMigrationListenerTest.MigrationEventsPack eventsPack = listener.ensureAndGetSingleEventPack();
        assertMigrationProcessCompleted(eventsPack);
        assertMigrationProcessEventsConsistent(eventsPack);
        assertMigrationEventsConsistentWithResult(eventsPack);
    }

    private PartitionMigrationListenerTest.EventCollectingMigrationListener eventCollectingMigrationListener() {
        return new PartitionMigrationListenerTest.EventCollectingMigrationListener();
    }

    private void assertRegistrationsSizeEventually(HazelcastInstance instance, int size) {
        assertTrueEventually(() -> {
            EventService eventService = getNode(instance).getNodeEngine().getEventService();
            Collection<EventRegistration> registrations = eventService
                    .getRegistrations(SERVICE_NAME, MIGRATION_EVENT_TOPIC);
            assertEquals(size, registrations.size());
        });
    }

    static class SingleThreadMigrationListener implements MigrationListener {

        private String threadName;
        private volatile boolean finished = false;
        private volatile boolean invokedOnSingleThread = true;

        @Override
        public void migrationStarted(MigrationState state) {
            threadName = Thread.currentThread().getName();
        }

        @Override
        public void migrationFinished(MigrationState state) {
            if (isInvokedFromAnotherThread()) {
                invokedOnSingleThread = false;
            }
            finished = true;
        }

        @Override
        public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
            if (isInvokedFromAnotherThread()) {
                invokedOnSingleThread = false;
            }
        }

        @Override
        public void replicaMigrationFailed(ReplicaMigrationEvent event) {
            if (isInvokedFromAnotherThread()) {
                invokedOnSingleThread = false;
            }
        }

        public void assertAllMethodsInvokedOnTheSameThread() {
            assertTrueEventually(() -> {
                assertTrue(finished);
                assertTrue(invokedOnSingleThread);
            });
        }

        private boolean isInvokedFromAnotherThread() {
            return !Thread.currentThread().getName().equals(threadName);
        }
    }
}
