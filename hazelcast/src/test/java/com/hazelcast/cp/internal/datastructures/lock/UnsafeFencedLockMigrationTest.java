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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsafeFencedLockMigrationTest extends HazelcastRaftTestSupport {

    @Test
    public void whenLockIsMigrated_thenSessionInformationShouldMigrate() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "2");

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);

        // Create two locks on each partition,
        // so one of them is guaranteed to be migrated
        // after new instance is started.
        FencedLock lock1 = hz1.getCPSubsystem().getLock(generateName(hz1, 0));
        FencedLock lock2 = hz1.getCPSubsystem().getLock(generateName(hz1, 1));

        lock1.lock();
        lock2.lock();

        // Start new instance to trigger migration
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        waitAllForSafeState(hz1, hz2);

        // Unlock after lock is migrated
        lock1.unlock();
        lock2.unlock();
    }

    @Test
    public void whenLockIsMigrated_thenWaitingOpsShouldBeNotified() throws Exception {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "2");

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);

        // Create two locks on each partition,
        // so one of them is guaranteed to be migrated
        // after new instance is started.
        FencedLock lock1 = hz1.getCPSubsystem().getLock(generateName(hz1, 0));
        FencedLock lock2 = hz1.getCPSubsystem().getLock(generateName(hz1, 1));

        lock1.lock();
        lock2.lock();

        Future waitingLock1 = spawn(lock1::lock);
        Future waitingLock2 = spawn(lock2::lock);

        // Ensure waiting op is registered
        LockService lockService = getNodeEngineImpl(hz1).getService(LockService.SERVICE_NAME);
        assertTrueEventually(() -> {
            LockRegistry registry1 = lockService.getRegistryOrNull(lock1.getGroupId());
            assertThat(registry1.getLiveOperations(), hasSize(1));

            LockRegistry registry2 = lockService.getRegistryOrNull(lock2.getGroupId());
            assertThat(registry2.getLiveOperations(), hasSize(1));
        });

        // Start new instance to trigger migration
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        waitAllForSafeState(hz1, hz2);

        // Unlock after lock is migrated
        lock1.unlock();
        lock2.unlock();

        // Waiting op is triggered after unlock
        waitingLock1.get();
        waitingLock2.get();
    }

    @Test
    public void whenLockIsBlocked_thenBackupIsReplicated() {
        Config config = new Config();

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        FencedLock lock = hz2.getCPSubsystem().getLock("lock@" + generateKeyOwnedBy(hz1));
        CountDownLatch latch = new CountDownLatch(1);

        spawn(() -> {
            lock.lock();
            latch.countDown();

            // wait until other thread blocks
            LockService lockService = getNodeEngineImpl(hz1).getService(LockService.SERVICE_NAME);
            assertTrueEventually(() -> {
                LockRegistry registry = lockService.getRegistryOrNull(lock.getGroupId());
                assertThat(registry.getLiveOperations(), hasSize(1));
            });
            lock.unlock();
        });

        assertOpenEventually(latch);
        lock.lock();

        waitAllForSafeState(hz1, hz2);
        hz1.getLifecycleService().terminate();

        lock.unlock();
    }

    private String generateName(HazelcastInstance instance, int partitionId) {
        return "lock@" + generateKeyForPartition(instance, partitionId);
    }

}
