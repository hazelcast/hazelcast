/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.RebalanceMode;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.partition.TestPartitionUtils.assertAllPartitionsBelongTo;
import static com.hazelcast.internal.partition.TestPartitionUtils.assertNoPartitionsBelongTo;
import static com.hazelcast.internal.partition.TestPartitionUtils.assertSomePartitionsBelongTo;
import static com.hazelcast.internal.partition.TestPartitionUtils.dumpPartitionTable;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getPartitionService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionRebalanceTest extends HazelcastTestSupport {

    // maximum time to wait for assertions after partition rebalancing
    // if tests fail on busy build machines, this may have to be increased
    private static final int PARTITION_REBALANCE_TIMEOUT = 5;

    private HazelcastInstance instance1;
    private HazelcastInstance instance2;
    TestHazelcastInstanceFactory factory;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory();
    }

    @Test
    public void triggerRebalance_whenManual() {
        instance1 = factory.newHazelcastInstance(getConfig());
        instance2 = factory.newHazelcastInstance(getConfig());
        assertNoPartitionsBelongTo(instance1);
        assertNoPartitionsBelongTo(instance2);

        instance1.getPartitionService().triggerRebalance();
        assertTrueEventually(() -> {
            assertSomePartitionsBelongTo(instance1);
            assertSomePartitionsBelongTo(instance2);
        }, PARTITION_REBALANCE_TIMEOUT);
    }

    @Test
    public void triggerRebalance_whenManual_fromNonMaster() {
        instance1 = factory.newHazelcastInstance(getConfig());
        instance2 = factory.newHazelcastInstance(getConfig());
        assertNoPartitionsBelongTo(instance1);
        assertNoPartitionsBelongTo(instance2);

        instance2.getPartitionService().triggerRebalance();
        assertTrueEventually(() -> {
            assertSomePartitionsBelongTo(instance1);
            assertSomePartitionsBelongTo(instance2);
        }, PARTITION_REBALANCE_TIMEOUT);
    }

    @Test
    public void triggerRebalance_whenManualAndNewMemberJoinsCluster() {
        instance1 = factory.newHazelcastInstance(getConfig());
        getPartitionService(instance1).firstArrangement();

        instance2 = factory.newHazelcastInstance(getConfig());
        assertTrueAllTheTime(() -> assertNoPartitionsBelongTo(instance2), PARTITION_REBALANCE_TIMEOUT);

        instance1.getPartitionService().triggerRebalance();
        assertTrueEventually(() -> assertSomePartitionsBelongTo(instance2), PARTITION_REBALANCE_TIMEOUT);
    }

    @Test
    public void rebalance_whenAutoWithDelay() {
        // this test will take > 10 seconds :(
        instance1 = factory.newHazelcastInstance(getConfig(10));
        getPartitionService(instance1).firstArrangement();

        AtomicBoolean done = new AtomicBoolean(true);

        instance2 = factory.newHazelcastInstance(getConfig());
        // assert partition rebalancing occurs automatically without delay on member join
        assertTrueEventually(() -> assertSomePartitionsBelongTo(instance2), PARTITION_REBALANCE_TIMEOUT);
        Address shutdownMemberAddress = getAddress(instance2);
        instance2.getLifecycleService().terminate();

        // some partitions still belong to the gone member
        try {
            assertTrueAllTheTime(() -> assertSomePartitionsBelongTo(instance1, shutdownMemberAddress), PARTITION_REBALANCE_TIMEOUT);
            sleepAtLeastSeconds(PARTITION_REBALANCE_TIMEOUT);

            // after configured delay, partition rebalancing occurs automatically
            assertTrueEventually(() -> assertAllPartitionsBelongTo(instance1), PARTITION_REBALANCE_TIMEOUT);
        } finally {
            done.set(false);
        }
    }

    @Test
    public void rebalanceMapWithoutBackups_whenAutoWithDelay()
            throws ExecutionException, InterruptedException {
        instance1 = factory.newHazelcastInstance(getConfig(5));
        IMap<Integer, Integer> map = instance1.getMap("nobackup");
        for (int i = 0; i < 1000; i++) {
            map.set(i, i);
        }
        assertEquals(1000, map.size());

        instance2 = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(instance1, instance2);
        instance2.getLifecycleService().terminate();

        // map.size will complete after partition rebalancing
        // since operation timeout is greater than auto partition
        // rebalancing delay
        Future<Integer> sizeFuture = spawn(() -> map.size());

        // since there are no backups, some data are lost
        assertTrue(sizeFuture.get() < 1000);
    }

    @Test
    public void rebalanceMapWithBackups_whenAutoWithDelay()
            throws ExecutionException, InterruptedException {
        instance1 = factory.newHazelcastInstance(getConfig(5));
        String mapName = randomMapName();
        IMap<Integer, Integer> map = instance1.getMap(mapName);
        for (int i = 0; i < 1000; i++) {
            map.set(i, i);
        }
        assertEquals(1000, map.size());

        instance2 = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(instance1, instance2);
        instance2.getLifecycleService().terminate();

        // map.size will complete after partition rebalancing
        // since operation timeout is greater than auto partition
        // rebalancing delay
        Future<Integer> sizeFuture = spawn(() -> map.size());

        // map is expected to be intact, since backups partitions
        // will be promoted on rebalance
        assertEquals(1000, (int) sizeFuture.get());
    }

    @Test
    public void rebalanceMapWithBackups_whenManualAndMemberRejoins()
            throws ExecutionException, InterruptedException {
        instance1 = factory.newHazelcastInstance(getConfig());
        String mapName = randomMapName();
        IMap<Integer, Integer> map = instance1.getMap(mapName);
        for (int i = 0; i < 1000; i++) {
            map.set(i, i);
        }
        assertEquals(1000, map.size());

        instance2 = factory.newHazelcastInstance(getConfig());
        Address instance2Address = getAddress(instance2);
        instance2.getPartitionService().triggerRebalance();
        waitAllForSafeState(instance1, instance2);
        instance2.getLifecycleService().terminate();

        // map.size will complete after partition rebalancing
        // since operation timeout is greater than auto partition
        // rebalancing delay
        Future<Integer> sizeFuture = spawn(() -> map.size());

        instance2 = factory.newHazelcastInstance(instance2Address, getConfig());
        instance2.getPartitionService().triggerRebalance();

        // map is expected to be intact, since backup partitions
        // will be promoted, then rebalancing will redistribute
        // replica ownership to both members
        // this is because the joining member is considered a new member
        // (has same address but different UUID than entries in partition
        // table -> these replicas are pruned and backups are promoted)
        // related: MemberMap#getMember(Address address, UUID uuid)
        assertEquals(1000, (int) sizeFuture.get());
    }

    /** Configuration with manual rebalance mode */
    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig()
                .setProperty(ClusterProperty.PARTITIONING_REBALANCE_MODE.getName(), RebalanceMode.MANUAL.name());
        return config;
    }

    /** Configuration with auto rebalance mode and given rebalance delay */
    protected Config getConfig(int rebalanceDelaySeconds) {
        Config config = smallInstanceConfig()
                .setProperty(ClusterProperty.PARTITION_REBALANCE_DELAY_SECONDS.getName(), "" + rebalanceDelaySeconds);
        config.getMapConfig("nobackup").setBackupCount(0);
        return config;
    }

    public static void spawnPartitionTableDumper(HazelcastInstance instance, AtomicBoolean done) {
        spawn(() -> {
            while (!done.get()) {
                sleepMillis(700);
                System.out.println(">\n" + dumpPartitionTable(getPartitionService(instance).createPartitionTableView()));
            }
        });
    }
}
