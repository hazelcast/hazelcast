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

package com.hazelcast.internal.partition;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.partition.AntiEntropyCorrectnessTest.setBackupPacketDropFilter;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionServiceSafetyCheckTest extends PartitionCorrectnessTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(PartitionCorrectnessTestSupport.class);

    private static final float BLOCK_RATIO = 0.95f;

    @Before
    public void setupParams() {
        backupCount = 3;
        nodeCount = 4;
    }

    @Override
    protected Config getConfig() {
        // Partition count is overwritten back to PartitionCorrectnessTestSupport.partitionCount
        // in PartitionCorrectnessTestSupport.getConfig(boolean, boolean).
        return smallInstanceConfig();
    }

    @Test
    public void clusterShouldBeSafe_withoutPartitionInitialization() throws InterruptedException {
        Config config = getConfig(false, false);
        startNodes(config, nodeCount);

        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();

        assertSafe(instances);
    }

    @Test
    public void clusterShouldBeSafe_withOnlyLiteMembers() throws InterruptedException {
        Config config = getConfig(false, false);
        config.setLiteMember(true);

        startNodes(config, nodeCount);

        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();

        assertSafe(instances);
    }

    @Test
    public void clusterShouldBeSafe_withSingleDataMember() throws InterruptedException {
        Config config0 = getConfig(true, false);

        HazelcastInstance hz = factory.newHazelcastInstance(config0);
        fillData(hz);

        Config config = getConfig(true, false);
        config.setLiteMember(true);
        startNodes(config, nodeCount - 1);

        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();

        assertSafeEventually(instances);
    }

    @Test
    public void clusterShouldBeEventuallySafe_withPartitionInitialization() throws InterruptedException {
        clusterShouldBeEventuallySafe_withPartitionInitialization(false);
    }

    @Test
    public void clusterShouldBeEventuallySafe_withPartitionInitializationAndAntiEntropy() throws InterruptedException {
        clusterShouldBeEventuallySafe_withPartitionInitialization(true);
    }

    private void clusterShouldBeEventuallySafe_withPartitionInitialization(boolean withAntiEntropy) throws InterruptedException {
        Config config = getConfig(true, withAntiEntropy);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        startNodes(config, nodeCount - 1);

        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        warmUpPartitions(instances);

        if (withAntiEntropy) {
            for (HazelcastInstance instance : instances) {
                setBackupPacketDropFilter(instance, BLOCK_RATIO);
            }
        }

        fillData(hz);
        assertSafeEventually(instances);
    }

    @Test
    public void clusterShouldBeEventuallySafe_duringMigration() throws InterruptedException {
        Config config = getConfig(true, false);
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        fillData(hz);

        startNodes(config, nodeCount - 1);
        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();

        assertSafeEventually(instances);
    }

    @Test
    public void clusterShouldNotBeSafe_whenBackupsBlocked_withoutAntiEntropy() throws InterruptedException {
        Config config = getConfig(true, false);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        startNodes(config, nodeCount - 1);

        final Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        warmUpPartitions(instances);

        for (HazelcastInstance instance : instances) {
            setBackupPacketDropFilter(instance, BLOCK_RATIO);
        }

        fillData(hz);

        assertTrueFiveSeconds(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(isAllInSafeState(instances));

                for (HazelcastInstance instance : instances) {
                    PartitionService ps = instance.getPartitionService();
                    assertFalse(ps.isClusterSafe());
                }
            }
        });
    }

    @Category(SlowTest.class)
    @Test(timeout = 120000)
    public void clusterSafe_completes_whenInvokedFromCommonPool() throws InterruptedException {
        Config config = getConfig(true, true);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        startNodes(config, nodeCount);

        final Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();

        // ensure we start enough threads to block all threads of FJP#commonPool
        int threadCount = ForkJoinPool.commonPool().getParallelism() + 2;
        spawn(() -> {
            // trigger partition assignment while many threads are querying cluster safety
            sleepSeconds(5);
            warmUpPartitions(instances);
            fillData(hz);
        });
        ExecutorService executorService = ForkJoinPool.commonPool();
        // no assertions are actually executed. We only care that isClusterSafe
        // does not hang indefinitely
        AtomicInteger loopCount = new AtomicInteger();
        assertTrueAllTheTime(() -> {
            LOGGER.info(">> loop start: " + loopCount.incrementAndGet());
            long start = System.currentTimeMillis();
            CountDownLatch startQueryClusterSafe = new CountDownLatch(1);
            CountDownLatch completed = new CountDownLatch(threadCount);
            for (int i = 0; i < threadCount; i++) {
                executorService.submit(() -> {
                    try {
                        startQueryClusterSafe.await();
                        for (HazelcastInstance instance : instances) {
                            PartitionService ps = instance.getPartitionService();
                            // just invoke isClusterSafe, no need to assert result
                            // we only care about the call eventually completing
                                    ps.isClusterSafe();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        completed.countDown();
                    }
                });
            }
            startQueryClusterSafe.countDown();
            completed.await();
            long end = System.currentTimeMillis();
            LOGGER.info(">> loop finish: " + TimeUnit.MILLISECONDS.toSeconds(end - start) + "s " + loopCount);
        }, 30);
    }

    @Test
    public void clusterShouldBeSafe_whenBackupsBlocked_withForceToBeSafe() throws InterruptedException {
        Config config = getConfig(true, true);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        startNodes(config, nodeCount - 1);

        final Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        warmUpPartitions(instances);

        for (HazelcastInstance instance : instances) {
            setBackupPacketDropFilter(instance, BLOCK_RATIO);
        }

        fillData(hz);

        for (HazelcastInstance instance : instances) {
            assertTrue(instance.getPartitionService().forceLocalMemberToBeSafe(1, TimeUnit.MINUTES));
        }
    }

    @Test
    public void partitionAssignmentsShouldBeCorrect_whenClusterIsSafe() {
        Config config = getConfig(false, false);

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        warmUpPartitions(hz1);

        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);
        waitAllForSafeState(hz1, hz2, hz3);

        hz2.getLifecycleService().terminate();
        assertClusterSizeEventually(2, hz1, hz3);
        waitAllForSafeState(hz1, hz3);
        assertPartitionAssignments(factory);

        hz1.getLifecycleService().terminate();
        assertClusterSizeEventually(1, hz3);
        waitAllForSafeState(hz3);
        assertPartitionAssignments(factory);
    }

    private void assertSafe(Collection<HazelcastInstance> instances) {
        assertAllInSafeState(instances);
        for (HazelcastInstance instance : instances) {
            isClusterInSafeState(instance);
        }
    }

    private void assertSafeEventually(Collection<HazelcastInstance> instances) {
        waitAllForSafeState(instances);
        for (HazelcastInstance instance : instances) {
            isClusterInSafeState(instance);
        }
    }
}
