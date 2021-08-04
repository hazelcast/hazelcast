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

package com.hazelcast.internal.partition;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.internal.partition.TestPartitionUtils.assertAllPartitionsBelongTo;
import static com.hazelcast.internal.partition.TestPartitionUtils.assertSomePartitionsBelongTo;
import static com.hazelcast.test.Accessors.getPartitionService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(ParallelJVMTest.class)
public class PartitionRebalanceTest extends HazelcastTestSupport {

    // maximum time to wait for assertions after partition rebalancing
    // if tests fail on busy build machines, this may have to be increased
    private static final int PARTITION_REBALANCE_TIMEOUT = 5;

    @Rule
    public final Timeout timeout = Timeout.seconds(20);

    private HazelcastInstance instance1;
    private HazelcastInstance instance2;
    TestHazelcastInstanceFactory factory;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory();
    }

    @Test(expected = IllegalArgumentException.class)
    public void configureWithNegativeDelay() {
        instance1 = factory.newHazelcastInstance(getConfig(-3));
    }

    @Test
    public void rebalanceOccursAfterDelay_whenDelayed() {
        // this test will take > 10 seconds :(
        instance1 = factory.newHazelcastInstance(getConfig(PARTITION_REBALANCE_TIMEOUT));
        getPartitionService(instance1).firstArrangement();

        instance2 = factory.newHazelcastInstance(getConfig());
        // assert partition rebalancing occurs automatically without delay on member join
        assertTrueEventually(() -> assertSomePartitionsBelongTo(instance2), PARTITION_REBALANCE_TIMEOUT / 2);
        instance2.getLifecycleService().terminate();

        sleepAtLeastSeconds(PARTITION_REBALANCE_TIMEOUT);
        // after configured delay, partition rebalancing occurs automatically
        assertTrueEventually(() -> assertAllPartitionsBelongTo(instance1), PARTITION_REBALANCE_TIMEOUT);
    }

    @Test
    public void rebalanceMapWithoutBackups_whenDelayed()
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
    public void rebalanceMapWithBackups_whenDelayed()
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
    public void rebalanceImmediatelyOnJoin_afterDelayedRebalancing() {
        // start instances with high rebalance delay
        instance1 = factory.newHazelcastInstance(getConfig(1000));
        getPartitionService(instance1).firstArrangement();

        instance2 = factory.newHazelcastInstance(getConfig(1000));
        assertTrueEventually(() -> assertSomePartitionsBelongTo(instance2), PARTITION_REBALANCE_TIMEOUT);
        // terminate second member
        instance2.getLifecycleService().terminate();
        sleepSeconds(1);

        // when member rejoins, partition rebalancing happens immediately, before configured delay
        instance2 = factory.newHazelcastInstance(getConfig(1000));
        assertTrueEventually(() -> assertSomePartitionsBelongTo(instance2), PARTITION_REBALANCE_TIMEOUT);
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        return config;
    }

    /** Configuration with given rebalance delay */
    protected Config getConfig(int rebalanceDelaySeconds) {
        Config config = smallInstanceConfig()
                .setProperty(ClusterProperty.PARTITION_REBALANCE_AFTER_MEMBER_LEFT_DELAY_SECONDS.getName(), "" + rebalanceDelaySeconds);
        config.getMapConfig("nobackup").setBackupCount(0);
        return config;
    }
}
