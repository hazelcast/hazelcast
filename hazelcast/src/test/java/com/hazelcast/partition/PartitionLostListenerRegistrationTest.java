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

package com.hazelcast.partition;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitionService;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.partition.InternalPartitionService.PARTITION_LOST_EVENT_TOPIC;
import static com.hazelcast.internal.partition.InternalPartitionService.SERVICE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartitionLostListenerRegistrationTest
        extends HazelcastTestSupport {

    @Test(expected = NullPointerException.class)
    public void test_addPartitionLostListener_whenNullListener() {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();

        partitionService.addPartitionLostListener(null);
    }

    @Test
    public void test_addPartitionLostListener_whenListenerRegisteredProgramatically() {
        final HazelcastInstance instance = createHazelcastInstance();

        final String id = instance.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));
        assertNotNull(id);

        assertRegistrationsSizeEventually(instance, 1);
    }

    @Test
    public void test_partitionLostListener_whenListenerRegisteredViaConfiguration() {
        final Config config = new Config();
        config.addListenerConfig(new ListenerConfig(mock(PartitionLostListener.class)));

        final HazelcastInstance instance = createHazelcastInstance(config);
        assertRegistrationsSizeEventually(instance, 1);
    }

    private void assertRegistrationsSizeEventually(final HazelcastInstance instance, final int expectedSize) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrueEventually(new AssertTask() {
                    @Override
                    public void run()
                            throws Exception {
                        final InternalEventService eventService = getNode(instance).getNodeEngine().getEventService();
                        assertEquals(expectedSize, eventService.getRegistrations(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC).size());
                    }
                });
            }
        });
    }

    @Test
    public void test_addPartitionLostListener_whenListenerRegisteredTwice() {
        final HazelcastInstance instance = createHazelcastInstance();
        final PartitionService partitionService = instance.getPartitionService();

        final PartitionLostListener listener = mock(PartitionLostListener.class);

        final String id1 = partitionService.addPartitionLostListener(listener);
        final String id2 = partitionService.addPartitionLostListener(listener);

        assertNotEquals(id1, id2);
        assertRegistrationsSizeEventually(instance, 2);
    }

    @Test
    public void test_removeMigrationListener_whenRegisteredListenerRemovedSuccessfully() {
        final HazelcastInstance instance = createHazelcastInstance();
        final PartitionService partitionService = instance.getPartitionService();

        final PartitionLostListener listener = mock(PartitionLostListener.class);

        final String id1 = partitionService.addPartitionLostListener(listener);
        final boolean result = partitionService.removePartitionLostListener(id1);

        assertTrue(result);
        assertRegistrationsSizeEventually(instance, 0);
    }


    @Test
    public void test_removeMigrationListener_whenNonExistingRegistrationIdRemoved() {
        final HazelcastInstance instance = createHazelcastInstance();
        final PartitionService partitionService = instance.getPartitionService();

        final boolean result = partitionService.removePartitionLostListener("notexist");
        assertFalse(result);
    }

    @Test(expected = NullPointerException.class)
    public void test_removeMigrationListener_whenNullRegistrationIdRemoved() {
        final HazelcastInstance instance = createHazelcastInstance();
        final PartitionService partitionService = instance.getPartitionService();

        partitionService.removePartitionLostListener(null);
    }

}
