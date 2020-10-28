/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.internal.partition.MigrationStateImpl;
import com.hazelcast.partition.PartitionMigrationListenerTest;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.partition.ReplicaMigrationEvent;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

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
import static org.hamcrest.MatcherAssert.assertThat;
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
    public void testRemoveMigrationListener() {
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

        verify(listener, never()).migrationStarted(any(MigrationStateImpl.class));
        verify(listener, never()).replicaMigrationCompleted(any(ReplicaMigrationEvent.class));
    }

    @Test
    public void testMigrationListenerCalledOnlyOnceWhenMigrationHappens() {
        Config config = new Config();
        // even partition count to make migration count deterministic
        int partitionCount = 10;
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(partitionCount));

        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        warmUpPartitions(instance1);

        // Change to NO_MIGRATION to prevent repartitioning
        // before 2nd member started and ready.
        instance1.getCluster().changeClusterState(ClusterState.NO_MIGRATION);

        PartitionMigrationListenerTest.CountingMigrationListener migrationListener = new PartitionMigrationListenerTest.CountingMigrationListener(partitionCount);
        client.getPartitionService().addMigrationListener(migrationListener);

        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance(config);

        changeClusterStateEventually(instance2, ClusterState.ACTIVE);

        waitAllForSafeState(instance2, instance1);

        assertTrueEventually(() -> {
            assertEquals(1, migrationListener.migrationStarted.get());
            assertEquals(1, migrationListener.migrationCompleted.get());

            int completed = getTotal(migrationListener.replicaMigrationCompleted);
            int failed = getTotal(migrationListener.replicaMigrationFailed);

            assertEquals(partitionCount, completed);
            assertEquals(0, failed);
        });

        for (AtomicInteger integer : migrationListener.replicaMigrationCompleted) {
            assertThat(integer.get(), Matchers.lessThanOrEqualTo(1));
        }
    }

    @Test
    public void testMigrationStats_whenMigrationProcessCompletes() {
        HazelcastInstance hz1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        warmUpPartitions(hz1);

        // Change to NO_MIGRATION to prevent repartitioning
        // before 2nd member started and ready.
        hz1.getCluster().changeClusterState(ClusterState.NO_MIGRATION);

        PartitionMigrationListenerTest.EventCollectingMigrationListener listener = new PartitionMigrationListenerTest.EventCollectingMigrationListener();
        client.getPartitionService().addMigrationListener(listener);

        HazelcastInstance hz2 = hazelcastFactory.newHazelcastInstance();
        // Back to ACTIVE
        changeClusterStateEventually(hz2, ClusterState.ACTIVE);

        PartitionMigrationListenerTest.MigrationEventsPack eventsPack = listener.ensureAndGetSingleEventPack();
        assertMigrationProcessCompleted(eventsPack);
        assertMigrationProcessEventsConsistent(eventsPack);
        assertMigrationEventsConsistentWithResult(eventsPack);
    }

    private void assertRegistrationsSizeEventually(HazelcastInstance instance, int size) {
        assertTrueEventually(() -> {
            EventService eventService = getNode(instance).getNodeEngine().getEventService();
            Collection<EventRegistration> registrations = eventService
                    .getRegistrations(SERVICE_NAME, MIGRATION_EVENT_TOPIC);
            assertEquals(size, registrations.size());
        });
    }

    private int getTotal(AtomicInteger[] integers) {
        int total = 0;
        for (AtomicInteger count : integers) {
            total += count.get();
        }
        return total;
    }
}
