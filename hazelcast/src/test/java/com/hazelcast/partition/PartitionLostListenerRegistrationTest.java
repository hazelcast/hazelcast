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

package com.hazelcast.partition;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.internal.partition.InternalPartitionService.PARTITION_LOST_EVENT_TOPIC;
import static com.hazelcast.internal.partition.InternalPartitionService.SERVICE_NAME;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionLostListenerRegistrationTest extends HazelcastTestSupport {

    @Test(expected = NullPointerException.class)
    public void test_addPartitionLostListener_whenNullListener() {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();

        partitionService.addPartitionLostListener(null);
    }

    @Test
    public void test_addPartitionLostListener_whenListenerRegisteredProgrammatically() {
        final HazelcastInstance instance = createHazelcastInstance();

        final UUID id = instance.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));
        assertNotNull(id);

        // Expected = 4 -> 1 added + 1 from {@link com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 4);
    }

    @Test
    public void test_partitionLostListener_whenListenerRegisteredViaConfiguration() {
        Config config = new Config();
        config.addListenerConfig(new ListenerConfig(mock(PartitionLostListener.class)));

        HazelcastInstance instance = createHazelcastInstance(config);
        // Expected = 4 -> 1 added + 1 from {@link com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 4);
    }

    @Test
    public void test_addPartitionLostListener_whenListenerRegisteredTwice() {
        HazelcastInstance instance = createHazelcastInstance();
        PartitionService partitionService = instance.getPartitionService();

        PartitionLostListener listener = mock(PartitionLostListener.class);

        UUID id1 = partitionService.addPartitionLostListener(listener);
        UUID id2 = partitionService.addPartitionLostListener(listener);

        assertNotEquals(id1, id2);
        // Expected = 5 -> 2 added + 1 from {@link com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 5);
    }

    @Test
    public void test_removePartitionLostListener_whenRegisteredListenerRemovedSuccessfully() {
        HazelcastInstance instance = createHazelcastInstance();
        PartitionService partitionService = instance.getPartitionService();

        PartitionLostListener listener = mock(PartitionLostListener.class);

        UUID id1 = partitionService.addPartitionLostListener(listener);
        boolean result = partitionService.removePartitionLostListener(id1);

        assertTrue(result);
        // Expected = 1 -> see {@link com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 3);
    }

    @Test
    public void test_removePartitionLostListener_whenNonExistingRegistrationIdRemoved() {
        HazelcastInstance instance = createHazelcastInstance();
        PartitionService partitionService = instance.getPartitionService();

        boolean result = partitionService.removePartitionLostListener(UuidUtil.newUnsecureUUID());
        assertFalse(result);
    }

    @Test(expected = NullPointerException.class)
    public void test_removePartitionLostListener_whenNullRegistrationIdRemoved() {
        HazelcastInstance instance = createHazelcastInstance();
        PartitionService partitionService = instance.getPartitionService();

        partitionService.removePartitionLostListener(null);
    }

    private static void assertRegistrationsSizeEventually(final HazelcastInstance instance, final int expectedSize) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrueEventually(new AssertTask() {
                    @Override
                    public void run() {
                        EventService eventService = getNode(instance).getNodeEngine().getEventService();
                        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME,
                                PARTITION_LOST_EVENT_TOPIC);
                        assertEquals(expectedSize, registrations.size());
                    }
                });
            }
        });
    }
}
