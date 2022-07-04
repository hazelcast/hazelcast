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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.Accessors.getAddress;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * https://github.com/hazelcast/hazelcast/issues/5444
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({SlowTest.class})
@Ignore(value = "https://github.com/hazelcast/hazelcast/issues/9828")
public class SlowMigrationCorrectnessTest extends AbstractMigrationCorrectnessTest {

    @Parameters(name = "backups:{0},nodes:{1},fragmented:{2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {1, 2, true},
                {1, InternalPartition.MAX_REPLICA_COUNT, true},
                {2, 3, true},
                {2, InternalPartition.MAX_REPLICA_COUNT, true},
                {3, 4, true},
                {3, InternalPartition.MAX_REPLICA_COUNT, true},
                {3, 4, false},
                {3, InternalPartition.MAX_REPLICA_COUNT, false},
        });
    }

    @Test(timeout = 6000 * 10 * 10)
    public void testPartitionData_whenSameNodesRestarted_afterPartitionsSafe() throws InterruptedException {
        partitionCount = 14;

        Config config = getConfig(true, false);
        config.setProperty(ClusterProperty.PARTITION_MIGRATION_INTERVAL.getName(), "1");

        HazelcastInstance[] instances = factory.newInstances(config, nodeCount);
        warmUpPartitions(instances);

        fillData(instances[instances.length - 1]);
        assertSizeAndDataEventually();

        Address[] restartingAddresses = new Address[backupCount];
        for (int i = 0; i < backupCount; i++) {
            restartingAddresses[i] = getAddress(instances[i]);
        }

        for (int i = 0; i < 5; i++) {
            restartNodes(config, restartingAddresses);
        }
        assertSizeAndDataEventually();
    }

    private void restartNodes(final Config config, Address[] addresses) throws InterruptedException {
        if (addresses.length == 1) {
            HazelcastInstance hz = factory.getInstance(addresses[0]);
            assertNotNull("No instance known for address: " + addresses[0], hz);
            TestUtil.terminateInstance(hz);
            sleepMillis(RandomPicker.getInt(1, 3000));
            factory.newHazelcastInstance(addresses[0], config);
        } else {
            final CountDownLatch latch = new CountDownLatch(addresses.length);
            for (final Address address : addresses) {
                final HazelcastInstance hz = factory.getInstance(address);
                assertNotNull("No instance known for address: " + address, hz);

                new Thread() {
                    public void run() {
                        TestUtil.terminateInstance(hz);
                        sleepMillis(RandomPicker.getInt(1, 3000));
                        factory.newHazelcastInstance(address, config);
                        latch.countDown();
                    }
                }.start();
            }
            assertTrue(latch.await(2, TimeUnit.MINUTES));
        }
    }
}
