/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.core.PartitionService;
import com.hazelcast.internal.partition.impl.MigrationCommitTest.DelayMigrationStart;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartitionMigrationListenerTest extends HazelcastTestSupport {

    @Test
    public void testMigrationListenerCalledOnlyOnceWhenMigrationHappens() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        Config config = new Config();
        // even partition count to make migration count deterministic
        final int partitionCount = 10;
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(partitionCount));

        // hold the migrations until all nodes join so that there will be no retries / failed migrations etc.
        CountDownLatch migrationStartLatch = new CountDownLatch(1);
        config.addListenerConfig(new ListenerConfig(new DelayMigrationStart(migrationStartLatch)));

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        warmUpPartitions(instance1);

        final CountingMigrationListener migrationListener = new CountingMigrationListener(partitionCount);
        instance1.getPartitionService().addMigrationListener(migrationListener);

        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        migrationStartLatch.countDown();

        waitAllForSafeState(instance2, instance1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int startedTotal = getTotal(migrationListener.migrationStarted);
                int completedTotal = getTotal(migrationListener.migrationCompleted);

                assertEquals(partitionCount / 2, startedTotal);
                assertEquals(startedTotal, completedTotal);
            }
        });

        assertAllLessThanOrEqual(migrationListener.migrationStarted, 1);
        assertAllLessThanOrEqual(migrationListener.migrationCompleted, 1);
    }

    private int getTotal(AtomicInteger[] integers) {
        int total = 0;
        for (AtomicInteger count : integers) {
            total += count.get();
        }
        return total;
    }

    @Test(expected = NullPointerException.class)
    public void testAddMigrationListener_whenNullListener() {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();

        partitionService.addMigrationListener(null);
    }

    @Test
    public void testAddMigrationListener_whenListenerRegisteredTwice() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        PartitionService partitionService = hz1.getPartitionService();

        MigrationListener listener = mock(MigrationListener.class);

        String id1 = partitionService.addMigrationListener(listener);
        String id2 = partitionService.addMigrationListener(listener);

        // first we check if the registration id's are different
        assertNotEquals(id1, id2);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveMigrationListener_whenNullListener() {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();

        partitionService.removeMigrationListener(null);
    }

    @Test
    public void testRemoveMigrationListener_whenNonExistingRegistrationId() {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();

        boolean result = partitionService.removeMigrationListener("notExist");

        assertFalse(result);
    }

    @Test
    public void testRemoveMigrationListener() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        PartitionService partitionService = hz1.getPartitionService();

        MigrationListener listener = mock(MigrationListener.class);

        String id = partitionService.addMigrationListener(listener);
        boolean removed = partitionService.removeMigrationListener(id);

        assertTrue(removed);

        // now we add a member
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        warmUpPartitions(hz1, hz2);

        // and verify that the listener isn't called.
        verify(listener, never()).migrationStarted(any(MigrationEvent.class));
    }


    @SuppressWarnings("SameParameterValue")
    private void assertAllLessThanOrEqual(AtomicInteger[] integers, int expected) {
        for (AtomicInteger integer : integers) {
            assertTrue(integer.get() <= expected);
        }
    }

    private static class CountingMigrationListener implements MigrationListener {

        AtomicInteger[] migrationStarted;
        AtomicInteger[] migrationCompleted;
        AtomicInteger[] migrationFailed;

        CountingMigrationListener(int partitionCount) {
            migrationStarted = new AtomicInteger[partitionCount];
            migrationCompleted = new AtomicInteger[partitionCount];
            migrationFailed = new AtomicInteger[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                migrationStarted[i] = new AtomicInteger();
                migrationCompleted[i] = new AtomicInteger();
                migrationFailed[i] = new AtomicInteger();
            }
        }

        @Override
        public void migrationStarted(MigrationEvent migrationEvent) {
            migrationStarted[migrationEvent.getPartitionId()].incrementAndGet();
        }

        @Override
        public void migrationCompleted(MigrationEvent migrationEvent) {
            migrationCompleted[migrationEvent.getPartitionId()].incrementAndGet();
        }

        @Override
        public void migrationFailed(MigrationEvent migrationEvent) {
            migrationFailed[migrationEvent.getPartitionId()].incrementAndGet();
        }
    }
}
